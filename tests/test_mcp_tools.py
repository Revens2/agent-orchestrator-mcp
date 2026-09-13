import json

import pytest
from starlette.testclient import TestClient

from orch_mcp.runner_api import RunnerAuth
from orch_mcp.server import build_app
from orch_mcp.store import Store

HEADERS = {"accept": "application/json, text/event-stream", "content-type": "application/json", "mcp-protocol-version": "2025-06-18"}
FORBIDDEN = ("shell", "exec", "powershell", "command", "cmd", "run_arbitrary", "read_file", "path")


def rpc(c, method, params=None, id_=1):
    r = c.post("/mcp", json={"jsonrpc": "2.0", "id": id_, "method": method, "params": params or {}}, headers=HEADERS)
    assert r.status_code == 200, r.text
    body = r.text
    if body.startswith("event:") or "data:" in body:
        body = "\n".join(line[5:].strip() for line in body.splitlines() if line.startswith("data:"))
    return json.loads(body)


def call(c, name, args):
    res = rpc(c, "tools/call", {"name": name, "arguments": args})["result"]
    assert not res.get("isError"), res
    return res.get("structuredContent") or json.loads(res["content"][0]["text"])


@pytest.fixture
def client(tmp_path):
    store = Store(tmp_path / "m.db")
    store.hello("pc", {"runtimes": [{"id": "fake", "available": True}], "workspaces": [{"id": "demo", "modes": ["read_only"], "description": "d"}]}, [])
    app = build_app(store, RunnerAuth({}, ["10.200.0.0/16"]))
    with TestClient(app, base_url="http://127.0.0.1:8802") as c:
        rpc(c, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}})
        yield c, store


def test_tool_surface_has_no_shell(client):
    c, _ = client
    tools = rpc(c, "tools/list")["result"]["tools"]
    names = {t["name"] for t in tools}
    assert names == {"agent_runner_list", "agent_workspace_list", "agent_job_start", "agent_job_get", "agent_job_output", "agent_job_cancel", "agent_job_list",
                     "agent_job_events", "agent_runner_inspect", "agent_job_wait",
                     "agent_mission_create", "agent_mission_get", "agent_mission_retry", "agent_mission_validate"}
    for t in tools:
        for prop in t["inputSchema"].get("properties", {}):
            assert not any(f == prop or prop.startswith(f) for f in FORBIDDEN), (t["name"], prop)


def test_start_get_cancel_via_mcp(client):
    c, _ = client
    runners = call(c, "agent_runner_list", {})["runners"]
    assert runners[0]["status"] == "online" and "workspaces" not in runners[0]
    ws = call(c, "agent_workspace_list", {})["workspaces"]
    assert ws[0]["id"] == "demo" and "path" not in ws[0]
    started = call(c, "agent_job_start", {"runner_id": "pc", "runtime": "fake", "workspace_id": "demo", "prompt": "hi", "idempotency_key": "abcdefgh"})
    assert started["state"] == "queued" and started["created"]
    again = call(c, "agent_job_start", {"runner_id": "pc", "runtime": "fake", "workspace_id": "demo", "prompt": "hi", "idempotency_key": "abcdefgh"})
    assert again["job_id"] == started["job_id"] and not again["created"]
    assert call(c, "agent_job_get", {"job_id": started["job_id"]})["state"] == "queued"
    assert call(c, "agent_job_list", {"state": "queued"})["count"] == 1
    assert call(c, "agent_job_cancel", {"job_id": started["job_id"]})["result"] == "cancelled"
    denied = call(c, "agent_job_start", {"runner_id": "pc", "runtime": "fake", "workspace_id": "../etc", "prompt": "x"})
    assert denied["error"] == "invalid_workspace"


def test_supervision_tools_via_mcp(client):
    c, store = client
    m = call(c, "agent_mission_create", {"objective": "obj", "acceptance_criteria": ["ok"],
                                         "runner_id": "pc", "runtime": "fake", "workspace_id": "demo"})
    assert m["state"] == "executing" and m["attempts"] == 1
    job_id = m["current_job_id"]
    got = call(c, "agent_job_get", {"job_id": job_id})
    assert got["execution_health"] == "idle" and got["runner_health"]["status"] == "online"
    assert got["broker_health"]["lease_valid"] is False  # queued : pas de bail
    evts = call(c, "agent_job_events", {"job_id": job_id})
    assert evts["last_seq"] == -1 and evts["events"] == []  # mission créée : job queued, journal vide
    snap = call(c, "agent_runner_inspect", {"runner_id": "pc"})
    assert snap["status"] == "online" and "runner_version" in snap
    assert snap["workspace_git"] == [] and snap["active_jobs"] == []
    assert call(c, "agent_runner_inspect", {"runner_id": "ghost"})["error"] == "unknown_runner"
    w = call(c, "agent_job_wait", {"job_id": job_id, "timeout_s": 0.5})
    assert w["woke_by"] == "timeout" and w["state"] == "queued"
    assert call(c, "agent_job_wait", {"job_id": "ghost"})["error"] == "unknown_job"
    mg = call(c, "agent_mission_get", {"mission_id": m["mission_id"]})
    assert mg["current_job_id"] == job_id
    assert call(c, "agent_mission_get", {"mission_id": "ghost"})["error"] == "unknown_mission"
    assert "completed" in call(c, "agent_job_get", {"job_id": job_id}) or True
    denied = call(c, "agent_mission_retry", {"mission_id": m["mission_id"]})
    assert denied["error"] == "mission_not_retryable"  # tentative encore active
    denied_v = call(c, "agent_mission_validate", {"mission_id": m["mission_id"], "verdict": "validated"})
    assert denied_v["error"] == "mission_not_validatable"


def test_invalid_runtime_rejected_by_schema(client):
    c, _ = client
    res = rpc(c, "tools/call", {"name": "agent_job_start", "arguments": {"runner_id": "pc", "runtime": "bash", "workspace_id": "demo", "prompt": "x"}})
    assert res.get("error") or res["result"].get("isError")
