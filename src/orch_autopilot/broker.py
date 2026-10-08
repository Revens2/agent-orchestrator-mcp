"""Client du broker orch : uniquement ses outils MCP existants, en JSON-RPC sur l'upstream local.

Jamais d'accès à `orch.db`. Allowlist d'outils locale (l'autopilot ne passe pas par la
gateway OAuth, il applique donc sa propre politique). Le runtime est verrouillé sur OpenCode.
"""

from __future__ import annotations

import itertools
import json
from typing import Any, Protocol

import httpx

from orch_autopilot import PRODUCT_RUNTIMES

ALLOWED_TOOLS = frozenset({"agent_runner_list", "agent_job_start", "agent_job_get", "agent_job_output", "agent_job_cancel"})
HEADERS = {
    "accept": "application/json, text/event-stream",
    "content-type": "application/json",
    "mcp-protocol-version": "2025-06-18",
}
MAX_OUTPUT_FETCH = 200_000


class BrokerUnavailable(Exception):
    """Échec de transport : l'issue de l'appel est INCERTAINE (la requête a pu être appliquée)."""


class RuntimeForbidden(Exception):
    pass


class Broker(Protocol):
    def runners(self) -> list[dict]: ...
    def start_job(self, payload: dict) -> dict: ...
    def get_job(self, job_id: str) -> dict: ...
    def read_output(self, job_id: str) -> str: ...
    def cancel_job(self, job_id: str) -> dict: ...


def check_start_payload(payload: dict) -> None:
    """Défense en profondeur : aucun job autopilot hors OpenCode, quel que soit l'appelant."""
    if payload.get("runtime") not in PRODUCT_RUNTIMES:
        raise RuntimeForbidden(f"runtime interdit pour l'autopilot : {payload.get('runtime')!r}")
    if payload.get("mode") not in ("read_only", "workspace_write"):
        raise RuntimeForbidden(f"mode interdit : {payload.get('mode')!r}")
    if not payload.get("idempotency_key"):
        raise RuntimeForbidden("idempotency_key obligatoire")


class MCPBroker:
    def __init__(self, url: str = "http://127.0.0.1:8802/mcp", client: httpx.Client | None = None, timeout_s: float = 30.0) -> None:
        self.url = url
        self._client = client or httpx.Client(timeout=timeout_s)
        self._ids = itertools.count(1)
        self._initialized = False

    def _rpc(self, method: str, params: dict) -> dict:
        body = {"jsonrpc": "2.0", "id": next(self._ids), "method": method, "params": params}
        try:
            r = self._client.post(self.url, json=body, headers=HEADERS)
        except httpx.HTTPError as exc:
            raise BrokerUnavailable(f"transport: {type(exc).__name__}") from exc
        if r.status_code >= 500 or r.status_code == 429:
            raise BrokerUnavailable(f"http {r.status_code}")
        if r.status_code != 200:
            raise BrokerUnavailable(f"http {r.status_code}")
        text = r.text
        if text.startswith("event:") or "data:" in text[:200]:
            text = "\n".join(line[5:].strip() for line in text.splitlines() if line.startswith("data:"))
        try:
            msg = json.loads(text)
        except ValueError as exc:
            raise BrokerUnavailable("réponse non JSON") from exc
        if "error" in msg:
            raise BrokerUnavailable(f"jsonrpc error {msg['error'].get('code')}")
        return msg.get("result", {})

    def _call(self, name: str, args: dict) -> Any:
        if name not in ALLOWED_TOOLS:
            raise RuntimeForbidden(f"outil broker non autorisé pour l'autopilot : {name}")
        if not self._initialized:
            self._rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                     "clientInfo": {"name": "orch-autopilot", "version": "1"}})
            self._initialized = True
        res = self._rpc("tools/call", {"name": name, "arguments": args})
        if res.get("isError"):
            text = (res.get("content") or [{}])[0].get("text", "")
            return {"error": "tool_error", "message": text[:500]}
        data = res.get("structuredContent")
        if data is None:
            data = json.loads((res.get("content") or [{"text": "{}"}])[0]["text"])
        if isinstance(data, dict) and set(data) == {"result"}:
            data = data["result"]
        return data

    def runners(self) -> list[dict]:
        return self._call("agent_runner_list", {}).get("runners", [])

    def start_job(self, payload: dict) -> dict:
        check_start_payload(payload)
        return self._call("agent_job_start", {**payload, "fire_and_forget": True})

    def get_job(self, job_id: str) -> dict:
        return self._call("agent_job_get", {"job_id": job_id, "tail_chars": 2000})

    def read_output(self, job_id: str) -> str:
        parts, cursor, size = [], 0, 0
        while cursor is not None and size < MAX_OUTPUT_FETCH:
            page = self._call("agent_job_output", {"job_id": job_id, "cursor": cursor, "limit": 20000})
            if "error" in page:
                break
            parts.append(page.get("text", ""))
            size += len(parts[-1])
            cursor = page.get("next_cursor")
        return "".join(parts)[:MAX_OUTPUT_FETCH]

    def cancel_job(self, job_id: str) -> dict:
        return self._call("agent_job_cancel", {"job_id": job_id})
