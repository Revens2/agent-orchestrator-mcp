"""Traçabilité additive d'origine des jobs (métadonnées réellement observables, rétrocompatible).

- create_job/create_mission acceptent origin_actor/mode/label + conversation_id (tous optionnels) ;
- absents => NULL (limite documentée : sans ID transmis, pas de conversation retrouvable) ;
- actor connu => transition initiale `mcp:<actor>`, sinon `mcp` ;
- exposés en lecture (get_job, list_jobs) ; idempotency inchangée.
"""

from orch_mcp.store import Store

INFO = {
    "version": "test",
    "max_parallel": 2,
    "runtimes": [{"id": "fake", "available": True}],
    "workspaces": [{"id": "demo", "modes": ["read_only", "workspace_write"]}],
}


def _env(tmp_path):
    s = Store(tmp_path / "o.db")
    s.hello("pc", INFO, [])
    return s


def test_origin_defaults_to_null_and_actor_mcp(tmp_path):
    s = _env(tmp_path)
    job, created = s.create_job("pc", "fake", "demo", "hello", "read_only")
    assert created
    assert job["origin_actor"] is None
    assert job["origin_mode"] is None
    assert job["origin_label"] is None
    assert job["conversation_id"] is None
    rows = s._db.execute("SELECT actor FROM transitions WHERE job_id=? ORDER BY rowid", (job["job_id"],)).fetchall()
    assert rows[0]["actor"] == "mcp"


def test_origin_stored_bounded_and_exposed(tmp_path):
    s = _env(tmp_path)
    job, _ = s.create_job(
        "pc", "fake", "demo", "hello", "read_only",
        origin_actor="chatgpt-connector-abc", origin_mode="oauth",
        origin_label="conv titre quelconque", conversation_id="conv-123",
    )
    assert job["origin_actor"] == "chatgpt-connector-abc"
    assert job["origin_mode"] == "oauth"
    assert job["origin_label"] == "conv titre quelconque"
    assert job["conversation_id"] == "conv-123"
    rows = s._db.execute("SELECT actor FROM transitions WHERE job_id=? ORDER BY rowid", (job["job_id"],)).fetchall()
    assert rows[0]["actor"] == "mcp:chatgpt-connector-abc"
    listed = s.list_jobs(limit=5)
    assert listed[0]["origin_actor"] == "chatgpt-connector-abc"
    assert listed[0]["conversation_id"] == "conv-123"
    # migration rejouable : colonnes présentes après _migrate sur base existante
    s._migrate()
    job2 = s.get_job(job["job_id"])
    assert job2["origin_actor"] == "chatgpt-connector-abc"


def test_mission_propagates_origin(tmp_path):
    s = _env(tmp_path)
    m = s.create_mission(
        "obj", ["ok"], 2, "pc", "fake", "demo", "read_only", None, None, None,
        origin_actor="orch-cli-statique", origin_mode="cli",
        origin_label="label-opt", conversation_id=None,
    )
    job = s.get_job(m["current_job_id"])
    assert job["origin_actor"] == "orch-cli-statique"
    assert job["origin_mode"] == "cli"
    assert job["origin_label"] == "label-opt"
    assert job["conversation_id"] is None


def test_request_origin_contextvar_roundtrip():
    from orch_mcp import tools as T

    assert T._current_request_origin() == (None, None)
    tok = T.set_request_origin("client-xyz", "oauth")
    try:
        assert T._current_request_origin() == ("client-xyz", "oauth")
    finally:
        T._request_origin.reset(tok)
    assert T._current_request_origin() == (None, None)
    # mode inconnu neutralisé, jamais bloquant
    tok = T.set_request_origin("c", "inconnu")
    try:
        assert T._current_request_origin() == ("c", None)
    finally:
        T._request_origin.reset(tok)


def test_origin_via_mcp_headers_and_labels(tmp_path):
    """Gateway relayée (x-orch-mcp-acteur/mode) + labels optionnels => stockés ;
    boucle locale sans en-têtes => NULL (limite, rien d'inventé)."""
    import json

    from starlette.testclient import TestClient

    from orch_mcp.runner_api import RunnerAuth
    from orch_mcp.server import build_app

    store = Store(tmp_path / "m.db")
    store.hello("pc", INFO, [])
    app = build_app(store, RunnerAuth({}, ["10.200.0.0/16"]))
    base = {"accept": "application/json, text/event-stream", "content-type": "application/json",
            "mcp-protocol-version": "2025-06-18"}

    def _rpc(c, method, params, headers):
        r = c.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, headers=headers)
        assert r.status_code == 200, r.text
        body = r.text
        if "data:" in body:
            body = "\n".join(line[5:].strip() for line in body.splitlines() if line.startswith("data:"))
        return json.loads(body)

    def _call(c, name, args, headers):
        res = _rpc(c, "tools/call", {"name": name, "arguments": args}, headers)["result"]
        assert not res.get("isError"), res
        return res.get("structuredContent") or json.loads(res["content"][0]["text"])

    with TestClient(app, base_url="http://127.0.0.1:8802") as c:
        _rpc(c, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                               "clientInfo": {"name": "t", "version": "0"}}, base)
        gw = dict(base, **{"x-orch-mcp-acteur": "chatgpt-oauth-client", "x-orch-mcp-mode": "oauth"})
        started = _call(c, "agent_job_start", {"runner_id": "pc", "runtime": "fake", "workspace_id": "demo",
                                               "prompt": "hi gw", "source_label": "conv-titre",
                                               "conversation_id": "chat-abc"}, gw)
        got = _call(c, "agent_job_get", {"job_id": started["job_id"]}, gw)
        assert got["origin_actor"] == "chatgpt-oauth-client"
        assert got["origin_mode"] == "oauth"
        assert got["origin_label"] == "conv titre quelconque" or got["origin_label"] == "conv-titre"
        assert got["conversation_id"] == "chat-abc"
        # boucle locale directe : origine NULL
        direct = _call(c, "agent_job_start", {"runner_id": "pc", "runtime": "fake",
                                              "workspace_id": "demo", "prompt": "hi direct"}, base)
        got2 = _call(c, "agent_job_get", {"job_id": direct["job_id"]}, base)
        assert got2["origin_actor"] is None
        assert got2["conversation_id"] is None
