import hashlib

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

import orch_protocol as P
from orch_mcp.runner_api import RunnerAuth, build_routes, parse_source_bindings, parse_tokens
from orch_mcp.store import Store

TOKEN = "t" * 48
OTHER = "o" * 48
FIXED_IP = "10.200.160.243"
PORTABLE_IP = "10.200.208.99"

INFO = {
    "runtimes": [{"id": "fake", "available": True}],
    "workspaces": [{"id": "demo", "modes": ["read_only"]}],
    "max_parallel": 1,
}


def _sha(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def post(c, path, body, *, token=TOKEN, ip=PORTABLE_IP):
    headers = {"x-real-ip": ip}
    if token is not None:
        headers["authorization"] = f"Bearer {token}"
    return c.post(
        f"/runner/v1/{path}",
        json={"protocol_version": P.PROTOCOL_VERSION, **body},
        headers=headers,
    )


@pytest.fixture
def bound_client(tmp_path):
    store = Store(tmp_path / "api.db")
    digests = parse_tokens(
        f"main-windows-pc:{_sha(TOKEN)},other-runner:{_sha(OTHER)}"
    )
    bindings = parse_source_bindings(
        f"main-windows-pc@{FIXED_IP}=pc-fixe"
    )
    app = Starlette(
        routes=build_routes(
            store,
            RunnerAuth(digests, ["10.200.0.0/16"], bindings),
        )
    )
    return TestClient(app), store


def test_parse_source_binding_normalizes_ip():
    assert parse_source_bindings(
        "main-windows-pc@10.200.160.243=pc-fixe"
    ) == {("main-windows-pc", "10.200.160.243"): "pc-fixe"}


@pytest.mark.parametrize(
    "raw",
    [
        "main-windows-pc",
        "main-windows-pc@not-an-ip=pc-fixe",
        "bad id@10.200.160.243=pc-fixe",
        "main-windows-pc@10.200.160.243=bad id",
        "main-windows-pc@10.200.160.243=",
        "@10.200.160.243=pc-fixe",
        (
            "main-windows-pc@10.200.160.243=pc-fixe,"
            "main-windows-pc@10.200.160.243=other-runner"
        ),
    ],
)
def test_parse_source_binding_rejects_invalid(raw):
    with pytest.raises(RuntimeError):
        parse_source_bindings(raw)


def test_same_valid_token_gets_distinct_identity_by_source(bound_client):
    c, _ = bound_client
    portable = post(c, "hello", {"info": INFO}, ip=PORTABLE_IP)
    fixed = post(c, "hello", {"info": INFO}, ip=FIXED_IP)
    assert portable.status_code == 200
    assert fixed.status_code == 200
    assert portable.json()["runner_id"] == "main-windows-pc"
    assert fixed.json()["runner_id"] == "pc-fixe"


def test_invalid_token_cannot_use_source_binding(bound_client):
    c, _ = bound_client
    r = post(c, "hello", {"info": INFO}, token="x" * 48, ip=FIXED_IP)
    assert r.status_code == 401
    assert r.json()["error"] == "unauthorized"


def test_missing_token_cannot_use_source_binding(bound_client):
    c, _ = bound_client
    r = post(c, "hello", {"info": INFO}, token=None, ip=FIXED_IP)
    assert r.status_code == 401


def test_other_token_is_not_remapped(bound_client):
    c, _ = bound_client
    r = post(c, "hello", {"info": INFO}, token=OTHER, ip=FIXED_IP)
    assert r.status_code == 200
    assert r.json()["runner_id"] == "other-runner"


def test_source_outside_allowed_cidr_is_still_forbidden(bound_client):
    c, _ = bound_client
    r = post(c, "hello", {"info": INFO}, token=TOKEN, ip="192.0.2.123")
    assert r.status_code == 403
    assert r.json()["error"] == "forbidden_source"


def test_pc_fixe_job_isolated_from_portable_source(bound_client):
    c, store = bound_client
    portable_epoch = post(c, "hello", {"info": INFO}, ip=PORTABLE_IP).json()["epoch"]
    fixed_epoch = post(c, "hello", {"info": INFO}, ip=FIXED_IP).json()["epoch"]

    job, _ = store.create_job("pc-fixe", "fake", "demo", "sentinel", "read_only")

    portable_jobs = post(
        c,
        "claim",
        {"epoch": portable_epoch, "free_slots": 1, "wait_s": 0},
        ip=PORTABLE_IP,
    ).json()["jobs"]
    assert portable_jobs == []

    fixed_jobs = post(
        c,
        "claim",
        {"epoch": fixed_epoch, "free_slots": 1, "wait_s": 0},
        ip=FIXED_IP,
    ).json()["jobs"]
    assert [j["job_id"] for j in fixed_jobs] == [job["job_id"]]
