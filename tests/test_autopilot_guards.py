"""Gardes du superviseur : disque plein, codes d'approbation HMAC, relais du jeton cockpit par la passerelle."""

from __future__ import annotations

import hashlib

import pytest

import orch_autopilot as A
from orch_autopilot.db import AutopilotDB
from orch_autopilot.policy import Config
from orch_autopilot.supervisor import AutopilotError, Supervisor
from tests.autopilot_fakes import Clock, FakeBroker, MemoryNotifier

MARK = "AUTOPILOT_STEP_OK"
GB = 1024 ** 3


def spec(key="r1"):
    return {"title": "t", "goal": "g", "steps": [
        {"key": key, "kind": "read", "workspace_id": "e2e", "prompt": "p", "expect": {"markers": [MARK]}}]}


def make(tmp_path, disk):
    clock, broker, notifier = Clock(), FakeBroker(), MemoryNotifier()
    sup = Supervisor(AutopilotDB(tmp_path / "g.db", clock=clock), broker, Config(), notifier=notifier, clock=clock,
                     disk_usage=lambda: disk["v"])
    return sup, broker, notifier


def test_low_disk_refuses_new_missions_and_new_jobs_then_recovers(tmp_path):
    disk = {"v": (193 * GB, 178 * GB, 15 * GB)}  # état VPS observé : 92 %, 15 Go libres => OK
    sup, broker, notifier = make(tmp_path, disk)
    oid = sup.submit(spec())["objective_id"]
    disk["v"] = (193 * GB, 192 * GB, 1 * GB)       # < 2 Go libres
    sup.tick()
    assert broker.start_calls == []
    assert sup.objective(oid)["steps"][0]["last_error"] == "disk_low"
    assert any(subject == "autopilot-disk-low" for subject, _ in notifier.messages)
    with pytest.raises(AutopilotError) as exc:
        sup.submit(spec("r2"))
    assert exc.value.code == "disk_low"
    disk["v"] = (193 * GB, 150 * GB, 43 * GB)
    sup.tick()
    assert len(broker.start_calls) == 1 and sup.objective(oid)["steps"][0]["state"] == A.RUNNING


def test_disk_pct_threshold_and_probe_failure_fail_closed(tmp_path):
    disk = {"v": (100 * GB, 96 * GB, 4 * GB)}  # 96 % > 95 %
    sup, _, _ = make(tmp_path, disk)
    assert sup.disk_status()["low"] is True

    def boom():
        raise OSError("io")

    sup._disk_usage = boom
    with pytest.raises(AutopilotError):
        sup.submit(spec())


def test_approval_code_hash_is_keyed(tmp_path):
    clock, notifier = Clock(), MemoryNotifier()
    sup = Supervisor(AutopilotDB(tmp_path / "c.db", clock=clock), FakeBroker(), Config(), notifier=notifier, clock=clock,
                     consent_key=b"k" * 32, disk_usage=lambda: (100 * GB, 10 * GB, 90 * GB))
    oid = sup.submit({"title": "t", "goal": "g", "steps": [{"key": "d1", "kind": "decision", "prompt": "q"}]})["objective_id"]
    sup.tick()
    req = sup.request_approval(f"{oid}:d1", "reject")
    code = notifier.last_code()
    stored = sup.db.one("SELECT code_hash FROM consents")["code_hash"]
    # la base seule ne permet pas de vérifier un code candidat (hash non salé = brute force hors ligne)
    assert stored.startswith("h1:") and stored != hashlib.sha256(f"{req['action_hash']}:{code}".encode()).hexdigest()
    # un autre superviseur (autre clé) ne valide pas ce code
    other = Supervisor(sup.db, FakeBroker(), Config(), notifier=notifier, clock=clock, consent_key=b"z" * 32,
                       disk_usage=lambda: (100 * GB, 10 * GB, 90 * GB))
    assert other.approve(req["action_hash"], code)["approved"] is False
    assert sup.approve(req["action_hash"], code)["approved"] is True


@pytest.mark.anyio
async def test_gateway_presents_upstream_token_and_never_relays_client_authorization():
    pytest.importorskip("fcntl")  # passerelle : Linux uniquement (verrou de fichier OAuth)
    import httpx

    from orch_gateway.upstream import ProxyMCP

    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {}})

    proxy = ProxyMCP("http://127.0.0.1:8812", jeton_upstream="u" * 40)
    proxy._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    body = b'{"jsonrpc":"2.0","id":1,"method":"ping","params":{}}'
    scope = {"type": "http", "method": "POST", "path": "/mcp", "query_string": b"", "headers": [
        (b"content-type", b"application/json"), (b"accept", b"application/json, text/event-stream"),
        (b"mcp-protocol-version", b"2025-06-18"), (b"authorization", b"Bearer client-oauth-token-should-not-pass")]}
    sent = []

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(msg):
        sent.append(msg)

    await proxy(scope, receive, send)
    assert seen.get("auth") == "Bearer " + "u" * 40
