"""Intégration : superviseur autopilot contre le VRAI broker (Store + MCP in-process), cockpit MCP réel."""

import json

import pytest
from starlette.testclient import TestClient

import orch_autopilot as A
from orch_autopilot import cockpit
from orch_autopilot.broker import MCPBroker
from orch_autopilot.db import AutopilotDB
from orch_autopilot.policy import Config
from orch_autopilot.service import build_app as build_autopilot_app
from orch_autopilot.supervisor import Supervisor
from orch_mcp.runner_api import RunnerAuth
from orch_mcp.server import build_app as build_broker_app
from orch_mcp.store import BrokerError, Store
from tests.autopilot_fakes import AGENT_SID, ARTIFACT, RUNNER_ID, MemoryNotifier, RunnerSigner
from tests.test_mcp_tools import rpc

MARK = "AUTOPILOT_STEP_OK"
SIGNER = RunnerSigner()
COCKPIT_TOKEN = "c" * 40
WS = [{"id": "e2e", "modes": ["read_only", "workspace_write"], "description": "sandbox"}]


def hello(store, available=True, held=()):
    return store.hello(RUNNER_ID, {"runtimes": [{"id": "opencode", "available": available}], "workspaces": WS},
                       list(held))


@pytest.fixture
def stack(tmp_path):
    store = Store(tmp_path / "orch.db")
    epoch = hello(store)
    app = build_broker_app(store, RunnerAuth({}, ["10.200.0.0/16"]))
    with TestClient(app, base_url="http://127.0.0.1:8802") as http:
        broker = MCPBroker("http://127.0.0.1:8802/mcp", client=http)
        notifier = MemoryNotifier()
        sup = Supervisor(AutopilotDB(tmp_path / "autopilot.db"), broker, Config(runner_id=RUNNER_ID, runner_pubkey=SIGNER.pubkey, expected_agent_sid=AGENT_SID, allowed_kinds=frozenset({"read", "local_write", "decision"}), local_write_needs_approval=False), notifier=notifier)
        yield store, epoch, sup, broker, notifier


def spec(kind="read"):
    return {"title": "smoke", "goal": "valider la chaîne", "steps": [
        {"key": "s1", "kind": kind, "workspace_id": "e2e", "prompt": "réponds",
         "expect": {"markers": [MARK], **({"artifacts": [ARTIFACT]} if kind == "local_write" else {})}}]}


def runner_complete(store, epoch, output, state="completed", summary="fini", **over):
    [c] = store.claim(RUNNER_ID, epoch, 1)
    jid, f = c["job_id"], c["fencing"]
    store.transition(RUNNER_ID, epoch, jid, f, "claimed", "starting")
    store.transition(RUNNER_ID, epoch, jid, f, "starting", "running")
    store.event(RUNNER_ID, epoch, jid, f, "evt-00000001", activity="working", output=output)
    if state == "completed":
        job = {"job_id": jid, "payload": {"runner_id": RUNNER_ID, "workspace_id": c["workspace_id"], "mode": c["mode"]},
               "created_at": store.clock()}
        env = SIGNER.envelope(SIGNER.payload(job, **over))
        store.transition(RUNNER_ID, epoch, jid, f, "running", "completed", exit_code=0, result_summary=f"{env}\n{summary}")
    return c


def step(sup, oid):
    return sup.objective(oid)["steps"][0]


def test_real_broker_read_step_done_and_payload_is_opencode(stack):
    store, epoch, sup, broker, _ = stack
    oid = sup.submit(spec())["objective_id"]
    sup.tick()
    s = step(sup, oid)
    assert s["state"] == A.RUNNING
    job = store.get_job(s["job_id"])
    assert job["runtime"] == "opencode" and job["mode"] == "read_only" and job["runner_id"] == RUNNER_ID
    # rejeu des mêmes octets (réponse perdue) : le broker renvoie le même job
    payload = json.loads(sup.db.one("SELECT payload FROM outbox")["payload"])
    again = broker.start_job(payload)
    assert again["job_id"] == s["job_id"] and again["created"] is False
    runner_complete(store, epoch, f"rapport {MARK}\n")
    sup.tick()
    assert step(sup, oid)["state"] == A.DONE
    assert sup.objective(oid)["state"] == A.OBJ_DONE


def test_real_broker_lost_write_late_result_rejected(stack):
    store, epoch, sup, _, _ = stack
    oid = sup.submit(spec("local_write"))["objective_id"]
    sup.tick()
    c = runner_complete(store, epoch, "travail partiel\n", state="running")
    hello(store, held=[])  # runner redémarré sans le job : le broker le marque lost
    sup.tick()
    assert store.get_job(c["job_id"])["state"] == "lost"
    assert step(sup, oid)["state"] == A.NEEDS_AUDIT
    with pytest.raises(BrokerError):  # résultat tardif refusé par le broker
        store.transition(RUNNER_ID, epoch, c["job_id"], c["fencing"], "running", "completed", exit_code=0)
    sup.tick()
    assert step(sup, oid)["state"] == A.NEEDS_AUDIT
    assert len(store.list_jobs(limit=10)) == 1


def test_real_broker_opencode_unavailable_blocked_worker(stack):
    store, _, sup, _, _ = stack
    hello(store, available=False)
    oid = sup.submit(spec())["objective_id"]
    sup.tick()
    assert step(sup, oid)["state"] == A.BLOCKED_WORKER
    assert store.list_jobs(limit=10) == []


# ------------------------------------------------------------------ cockpit MCP
@pytest.fixture
def cockpit_client(stack):
    _, _, sup, _, notifier = stack
    with TestClient(build_autopilot_app(sup, run_loop=False, token=COCKPIT_TOKEN), base_url="http://127.0.0.1:8807",
                    headers={"authorization": f"Bearer {COCKPIT_TOKEN}"}) as c:
        rpc(c, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}})
        yield c, sup, notifier


def call(c, name, args):
    res = rpc(c, "tools/call", {"name": name, "arguments": args})["result"]
    assert not res.get("isError"), res
    data = res.get("structuredContent") or json.loads(res["content"][0]["text"])
    return data.get("result", data) if set(data) == {"result"} else data


def test_cockpit_surface_matches_policy_and_has_no_privileged_inputs(cockpit_client):
    c, _, _ = cockpit_client
    tools = rpc(c, "tools/list")["result"]["tools"]
    assert {t["name"] for t in tools} == cockpit.TOOLS_READ | cockpit.TOOLS_WRITE
    for t in tools:
        props = set(t["inputSchema"].get("properties", {}))
        assert not props & {"runtime", "runner_id", "mode", "path", "command", "shell"}, t["name"]


def test_cockpit_new_chat_resume_by_id_and_approval_code_never_returned(cockpit_client, stack):
    c, sup, notifier = cockpit_client
    store, epoch, *_ = stack
    sub = call(c, "autopilot_submit", {"spec": spec("local_write"), "client_key": "chatgpt-conv-1"})
    oid = sub["objective_id"]
    assert call(c, "autopilot_submit", {"spec": spec("local_write"), "client_key": "chatgpt-conv-1"})["created"] is False
    sup.tick()
    runner_complete(store, epoch, "pas de marqueur\n")
    sup.tick()  # completed sans contrat de sortie : audit
    h = call(c, "autopilot_handoff", {"objective_id": oid})  # « nouveau chat »
    assert h["blockers"][0]["state"] == A.NEEDS_AUDIT and "autopilot_approve" in h["next_action"]
    req = call(c, "autopilot_approve", {"step_id": f"{oid}:s1", "resolution": "mark_failed"})
    assert notifier.last_code() not in json.dumps(req)
    assert call(c, "autopilot_approve", {"action_hash": req["action_hash"], "code": "AAAAAAAA"})["error"] == "bad_code"
    ok = call(c, "autopilot_approve", {"action_hash": req["action_hash"], "code": notifier.last_code()})
    assert ok["approved"] is True
    sup.tick()
    assert call(c, "autopilot_status", {"objective_id": oid})["state"] == A.OBJ_FAILED


def test_cockpit_kill_switch_on_only(cockpit_client):
    c, sup, _ = cockpit_client
    assert call(c, "autopilot_pause", {"objective_id": "*"}) == {"kill_switch": True}
    assert call(c, "autopilot_resume", {"objective_id": "*"})["error"] == "forbidden"
    assert sup.kill_switch_on()


def test_gateway_autopilot_policy_matches_cockpit():
    pytest.importorskip("fcntl", reason="passerelle Linux (fcntl) : testée sur le VPS")
    from orch_gateway.politique import politique_depuis_nom

    pol = politique_depuis_nom("autopilot")
    assert pol.lecture == cockpit.TOOLS_READ and pol.ecriture == cockpit.TOOLS_WRITE
    assert pol.autoriser_call("agent_job_start", {"orch:lecture", "orch:ecriture"}) is not None
    with pytest.raises(RuntimeError):
        politique_depuis_nom("autre")


def test_service_loop_ticks_and_health(stack):
    import time

    _, _, sup, _, _ = stack
    with TestClient(build_autopilot_app(sup, run_loop=True), base_url="http://127.0.0.1:8807") as c:
        deadline = time.time() + 10
        while sup.last_tick_at is None and time.time() < deadline:
            time.sleep(0.05)
        body = c.get("/health").json()
        assert body["status"] == "ok" and body["kill_switch"] is False


# ------------------------------------------------------------------ authentification cockpit (loopback)
def test_cockpit_refuses_unauthenticated_local_callers(stack):
    _, _, sup, _, _ = stack
    app = build_autopilot_app(sup, run_loop=False, token=COCKPIT_TOKEN)
    init = {"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}}
    hdr = {"accept": "application/json, text/event-stream", "content-type": "application/json"}
    with TestClient(app, base_url="http://127.0.0.1:8807") as c:
        assert c.post("/mcp", json=init, headers=hdr).status_code == 401
        assert c.post("/mcp", json=init, headers={**hdr, "authorization": "Bearer " + "x" * 40}).status_code == 401
        assert c.post("/mcp", json=init, headers={**hdr, "authorization": f"Bearer {COCKPIT_TOKEN}"}).status_code == 200
        health = c.get("/health").json()
        assert health["kill_switch"] in (True, False) and "disk" in health


def test_service_refuses_to_start_without_cockpit_token(monkeypatch, tmp_path):
    from orch_autopilot import service

    monkeypatch.setenv("AUTOPILOT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("AUTOPILOT_PORT", "18999")
    monkeypatch.delenv("AUTOPILOT_COCKPIT_TOKEN_FILE", raising=False)
    monkeypatch.setattr(service.uvicorn, "run", lambda *a, **k: pytest.fail("ne doit pas démarrer"))
    assert service.main(["run"]) == 2
    key = tmp_path / "consent.key"
    assert key.exists() and len(key.read_bytes()) == 32
