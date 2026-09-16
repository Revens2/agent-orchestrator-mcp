"""API privée runner (`/runner/v1/*`), exposée uniquement via nginx sur l'IP NetBird.

Authentification : `Authorization: Bearer <jeton runner>` comparé en temps constant
à une empreinte SHA-256 (`ORCH_RUNNER_TOKENS=runner_id:sha256hex[,…]`), jamais le
jeton en clair côté VPS. Défense en profondeur : l'IP d'origine posée par nginx
(`X-Real-IP`) doit appartenir à `ORCH_RUNNER_CIDRS` (overlay NetBird).
Le runner_id est dérivé du jeton : un client ne peut pas se déclarer autre machine.

Recovery uniquement : `ORCH_RUNNER_SOURCE_BINDINGS` peut temporairement remapper
UNE identité déjà authentifiée selon sa source, sous la forme
`base_runner_id@ip=target_runner_id`. Le bearer token valide reste obligatoire ;
une IP seule n'authentifie jamais un runner. Ce mécanisme sert à séparer deux
machines ayant accidentellement reçu le même token, puis doit être retiré après
rotation vers des jetons distincts.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import logging
import os
from typing import Any

import anyio
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

import orch_protocol as P
from orch_mcp.store import BrokerError, Store

log = logging.getLogger("orch.runner_api")

HTTP_STATUS = {
    "unauthorized": 401,
    "forbidden_source": 403,
    "unknown_runner": 409,
    "superseded": 409,
    "stale_fencing": 409,
    "state_conflict": 409,
    "invalid_transition": 409,
    "unknown_job": 404,
}


def parse_tokens(raw: str) -> dict[str, str]:
    """`runner_id:sha256hex,…` -> {sha256hex: runner_id}."""
    out: dict[str, str] = {}
    for item in (raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        runner_id, _, digest = item.partition(":")
        digest = digest.strip().lower()
        if not P.valid_id(runner_id) or len(digest) != 64:
            raise RuntimeError("ORCH_RUNNER_TOKENS invalide (runner_id:sha256hex)")
        out[digest] = runner_id
    return out


def parse_source_bindings(raw: str) -> dict[tuple[str, str], str]:
    """Parse les aliases de recovery `base_runner_id@ip=target_runner_id`.

    La clé contient l'identité issue DU TOKEN puis l'IP source normalisée. Un
    binding ne peut donc jamais transformer une simple IP en authentification.
    """
    out: dict[tuple[str, str], str] = {}
    for item in (raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        lhs, sep, target = item.partition("=")
        base, at, addr_raw = lhs.partition("@")
        base, target, addr_raw = base.strip(), target.strip(), addr_raw.strip()
        if not sep or not at or not P.valid_id(base) or not P.valid_id(target):
            raise RuntimeError(
                "ORCH_RUNNER_SOURCE_BINDINGS invalide (base_runner_id@ip=target_runner_id)"
            )
        try:
            addr = ipaddress.ip_address(addr_raw)
        except ValueError as exc:
            raise RuntimeError(
                "ORCH_RUNNER_SOURCE_BINDINGS invalide (IP source invalide)"
            ) from exc
        key = (base, str(addr))
        if key in out and out[key] != target:
            raise RuntimeError("ORCH_RUNNER_SOURCE_BINDINGS conflit pour la même source")
        out[key] = target
    return out


class RunnerAuth:
    def __init__(
        self,
        token_digests: dict[str, str],
        cidrs: list[str],
        source_bindings: dict[tuple[str, str], str] | None = None,
    ) -> None:
        self.digests = token_digests
        self.networks = [ipaddress.ip_network(c.strip()) for c in cidrs if c.strip()]
        # Le serveur historique ne passait que deux arguments. Lire l'env ici
        # garde cette compatibilité tout en faisant échouer le démarrage si une
        # configuration de recovery est invalide.
        self.source_bindings = (
            parse_source_bindings(os.environ.get("ORCH_RUNNER_SOURCE_BINDINGS", ""))
            if source_bindings is None
            else dict(source_bindings)
        )

    def identify(self, request: Request) -> str:
        real_ip = request.headers.get("x-real-ip", "")
        try:
            addr = ipaddress.ip_address(real_ip)
        except ValueError:
            raise BrokerError("forbidden_source", "origine inconnue")
        if not any(addr in net for net in self.networks):
            raise BrokerError("forbidden_source", "origine hors overlay autorisé")
        header = request.headers.get("authorization", "")
        scheme, _, token = header.partition(" ")
        if scheme.lower() != "bearer" or len(token) < 32:
            raise BrokerError("unauthorized", "jeton runner requis")
        digest = hashlib.sha256(token.strip().encode()).hexdigest()
        base_runner_id: str | None = None
        for known, runner_id in self.digests.items():
            if hmac.compare_digest(known, digest):
                base_runner_id = runner_id
                break
        if base_runner_id is None:
            raise BrokerError("unauthorized", "jeton runner invalide")
        # Recovery strict : le token décide d'abord de l'identité de base ; la
        # source ne peut qu'appliquer un alias explicitement configuré POUR
        # cette identité authentifiée.
        return self.source_bindings.get((base_runner_id, str(addr)), base_runner_id)


def _error(exc: BrokerError) -> JSONResponse:
    return JSONResponse({"error": exc.code, "message": exc.message}, status_code=HTTP_STATUS.get(exc.code, 400))


async def _body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if len(raw) > 256_000:
        raise BrokerError("too_large", "corps trop volumineux")
    try:
        data = await request.json()
    except Exception:  # noqa: BLE001 - JSON client invalide
        raise BrokerError("invalid_json", "JSON invalide")
    if not isinstance(data, dict):
        raise BrokerError("invalid_json", "objet JSON attendu")
    if data.get("protocol_version") != P.PROTOCOL_VERSION:
        raise BrokerError("protocol_mismatch", f"protocol_version {P.PROTOCOL_VERSION} requis")
    return data


def build_routes(store: Store, auth: RunnerAuth) -> list[Route]:
    def handler(fn):
        async def endpoint(request: Request) -> JSONResponse:
            try:
                runner_id = auth.identify(request)
                data = await _body(request)
                if data.get("runner_id") not in (None, runner_id):
                    raise BrokerError("unauthorized", "runner_id ne correspond pas au jeton")
                result = await fn(runner_id, data, request)
                return JSONResponse(result)
            except BrokerError as exc:
                if exc.code in ("unauthorized", "forbidden_source"):
                    log.warning("runner_auth_refused code=%s path=%s", exc.code, request.url.path)
                return _error(exc)

        return endpoint

    async def hello(runner_id: str, data: dict, _: Request) -> dict:
        epoch = await anyio.to_thread.run_sync(
            store.hello, runner_id, data.get("info") or {}, list(data.get("held") or [])
        )
        return {"runner_id": runner_id, "epoch": epoch, "lease_s": P.LEASE_S, "heartbeat_s": P.HEARTBEAT_S}

    async def heartbeat(runner_id: str, data: dict, _: Request) -> dict:
        return await anyio.to_thread.run_sync(
            store.heartbeat, runner_id, int(data.get("epoch", -1)), list(data.get("held") or [])
        )

    async def job_state(runner_id: str, data: dict, _: Request) -> dict:
        return await anyio.to_thread.run_sync(
            store.runner_job_state, runner_id, int(data.get("epoch", -1)),
            str(data.get("job_id")), int(data.get("fencing", -1)),
        )

    async def claim(runner_id: str, data: dict, request: Request) -> dict:
        epoch = int(data.get("epoch", -1))
        slots = int(data.get("free_slots", 0))
        wait_s = max(0.0, min(float(data.get("wait_s", P.CLAIM_POLL_S)), P.CLAIM_POLL_S))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_s
        while True:
            jobs = await anyio.to_thread.run_sync(store.claim, runner_id, epoch, slots)
            if jobs or loop.time() >= deadline or await request.is_disconnected():
                return {"jobs": jobs}
            await asyncio.sleep(0.5)

    async def event(runner_id: str, data: dict, _: Request) -> dict:
        tele = {k: data[k] for k in ("pid", "proc_alive", "proc_started_at", "child_procs", "tool") if k in data}
        return await anyio.to_thread.run_sync(
            lambda: store.event(
                runner_id,
                int(data.get("epoch", -1)),
                str(data.get("job_id")),
                int(data.get("fencing", -1)),
                str(data.get("event_id")),
                activity=data.get("activity"),
                output=data.get("output"),
                runtime_session_id=data.get("runtime_session_id"),
                telemetry=tele or None,
            )
        )

    async def transition(runner_id: str, data: dict, _: Request) -> dict:
        exit_code = data.get("exit_code")
        tele = {k: data[k] for k in ("pid", "proc_alive", "proc_started_at", "child_procs", "tool") if k in data}
        return await anyio.to_thread.run_sync(
            lambda: store.transition(
                runner_id,
                int(data.get("epoch", -1)),
                str(data.get("job_id")),
                int(data.get("fencing", -1)),
                str(data.get("from")),
                str(data.get("to")),
                exit_code=None if exit_code is None else int(exit_code),
                result_summary=data.get("result_summary"),
                error=data.get("error"),
                runtime_session_id=data.get("runtime_session_id"),
                telemetry=tele or None,
            )
        )

    async def question(runner_id: str, data: dict, _: Request) -> dict:
        q, created = await anyio.to_thread.run_sync(
            lambda: store.record_runner_question(
                runner_id,
                int(data.get("epoch", -1)),
                str(data.get("job_id")),
                int(data.get("fencing", -1)),
                str(data.get("runtime", "")),
                str(data.get("title", "")),
                str(data.get("question", "")),
                list(data.get("options") or []) if isinstance(data.get("options"), list) else None,
            )
        )
        return {"question": q, "created": created}

    base = "/runner/v1"
    return [
        Route(f"{base}/hello", handler(hello), methods=["POST"]),
        Route(f"{base}/heartbeat", handler(heartbeat), methods=["POST"]),
        Route(f"{base}/job-state", handler(job_state), methods=["POST"]),
        Route(f"{base}/claim", handler(claim), methods=["POST"]),
        Route(f"{base}/event", handler(event), methods=["POST"]),
        Route(f"{base}/question", handler(question), methods=["POST"]),
        Route(f"{base}/transition", handler(transition), methods=["POST"]),
    ]