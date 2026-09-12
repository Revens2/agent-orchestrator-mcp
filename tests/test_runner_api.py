import hashlib

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

import orch_protocol as P
from orch_mcp.runner_api import RunnerAuth, build_routes, parse_tokens
from orch_mcp.store import Store

TOKEN = "t" * 48
OTHER = "o" * 48


@pytest.fixture
def client(tmp_path):
    store = Store(tmp_path / "api.db")
    digests = parse_tokens(f"pc:{hashlib.sha256(TOKEN.encode()).hexdigest()},pc2:{hashlib.sha256(OTHER.encode()).hexdigest()}")
    app = Starlette(routes=build_routes(store, RunnerAuth(digests, ["10.200.0.0/16"])))
    return TestClient(app), store


def post(c, path, body, token=TOKEN, ip="10.200.208.99"):
    headers = {"x-real-ip": ip}
    if token:
        headers["authorization"] = f"Bearer {token}"
    return c.post(f"/runner/v1/{path}", json={"protocol_version": P.PROTOCOL_VERSION, **body}, headers=headers)


INFO = {"runtimes": [{"id": "fake", "available": True}], "workspaces": [{"id": "demo", "modes": ["read_only"]}], "max_parallel": 1}


def test_auth_required(client):
    c, _ = client
    assert post(c, "hello", {"info": INFO}, token=None).status_code == 401
    assert post(c, "hello", {"info": INFO}, token="x" * 48).status_code == 401


def test_source_ip_enforced(client):
    c, _ = client
    assert post(c, "hello", {"info": INFO}, ip="1.2.3.4").status_code == 403
    assert post(c, "hello", {"info": INFO}, ip="").status_code == 403


def test_runner_cannot_impersonate(client):
    c, _ = client
    r = post(c, "hello", {"info": INFO, "runner_id": "pc"}, token=OTHER)
    assert r.status_code == 401


def test_protocol_version_required(client):
    c, _ = client
    r = c.post("/runner/v1/hello", json={"info": INFO}, headers={"authorization": f"Bearer {TOKEN}", "x-real-ip": "10.200.1.1"})
    assert r.status_code == 400 and r.json()["error"] == "protocol_mismatch"


def test_flow_over_http(client):
    c, store = client
    epoch = post(c, "hello", {"info": INFO}).json()["epoch"]
    job, _ = store.create_job("pc", "fake", "demo", "hi", "read_only")
    jobs = post(c, "claim", {"epoch": epoch, "free_slots": 1, "wait_s": 0}).json()["jobs"]
    assert jobs[0]["job_id"] == job["job_id"]
    fencing = jobs[0]["fencing"]
    for src, dst in (("claimed", "starting"), ("starting", "running")):
        assert post(c, "transition", {"epoch": epoch, "job_id": job["job_id"], "fencing": fencing, "from": src, "to": dst}).status_code == 200
    assert post(c, "event", {"epoch": epoch, "job_id": job["job_id"], "fencing": fencing, "event_id": "e-00000001", "output": "o"}).status_code == 200
    # un autre runner authentifié ne peut pas toucher ce job
    e2 = post(c, "hello", {"info": INFO}, token=OTHER).json()["epoch"]
    r = post(c, "transition", {"epoch": e2, "job_id": job["job_id"], "fencing": fencing, "from": "running", "to": "completed", "exit_code": 0}, token=OTHER)
    assert r.status_code == 404
    r = post(c, "transition", {"epoch": epoch, "job_id": job["job_id"], "fencing": fencing, "from": "running", "to": "completed", "exit_code": 0})
    assert r.status_code == 200 and r.json()["state"] == "completed"


def test_stale_epoch_409(client):
    c, _ = client
    e1 = post(c, "hello", {"info": INFO}).json()["epoch"]
    post(c, "hello", {"info": INFO})
    r = post(c, "heartbeat", {"epoch": e1, "held": []})
    assert r.status_code == 409 and r.json()["error"] == "superseded"


def test_body_limit(client):
    c, _ = client
    r = post(c, "hello", {"info": INFO, "pad": "x" * 300_000})
    assert r.status_code == 400
