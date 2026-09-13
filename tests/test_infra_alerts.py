"""Alertes infra unifiées : persistance, déduplication, lecture MCP read-only."""

import json

import pytest
from starlette.testclient import TestClient

import orch_protocol as P
from orch_mcp.runner_api import RunnerAuth
from orch_mcp.server import build_app
from orch_mcp.store import BrokerError, Store

HEADERS = {"accept": "application/json, text/event-stream", "content-type": "application/json", "mcp-protocol-version": "2025-06-18"}


class Clock:
    def __init__(self):
        self.t = 1_700_000_000.0

    def __call__(self):
        return self.t


@pytest.fixture
def store(tmp_path):
    return Store(tmp_path / "a.db", clock=Clock())


def test_record_and_get(store):
    alert, created = store.record_alert("etude", "orch-mcp", "critical", "broker hors ligne", "exit 1")
    assert created and alert["occurrences"] == 1 and alert["state"] == "active"
    assert alert["source"] == "etude" and alert["severity"] == "critical"
    assert alert["first_at"] == alert["last_at"] and alert["detail"] == "exit 1"
    got = store.get_alert(alert["alert_id"])
    assert got and got["detail"] == "exit 1"
    assert store.get_alert("ghost") is None


def test_dedup_same_fingerprint_bumps_occurrences(store):
    a1, c1 = store.record_alert("nexus", "nexus-api", "warning", "latence élevée", "p99=2s")
    a2, c2 = store.record_alert("nexus", "nexus-api", "warning", "latence élevée", "p99=3s")
    assert c1 and not c2 and a1["alert_id"] == a2["alert_id"]
    assert a2["occurrences"] == 2 and a2["detail"] == "p99=3s"  # détail rafraîchi


def test_dedup_escalates_severity(store):
    a1, _ = store.record_alert("etude", "hermes", "warning", "sidecar instable")
    a2, created = store.record_alert("etude", "hermes", "critical", "sidecar instable")
    assert not created and a2["occurrences"] == 2 and a2["severity"] == "critical"


def test_dedup_window_expires_and_resolved_reopens(store):
    clock = store.clock
    a1, _ = store.record_alert("etude", "svc", "info", "ping")
    clock.t += P.ALERT_DEDUP_WINDOW_S + 1
    a2, created = store.record_alert("etude", "svc", "info", "ping")
    assert created and a2["alert_id"] != a1["alert_id"]
    store.set_alert_state(a2["alert_id"], "resolved")
    a3, created3 = store.record_alert("etude", "svc", "info", "ping")
    assert created3 and a3["alert_id"] != a2["alert_id"]  # résolue => nouvelle ligne


def test_distinct_titles_are_distinct_alerts(store):
    a1, _ = store.record_alert("etude", "svc", "info", "erreur A")
    a2, _ = store.record_alert("etude", "svc", "info", "erreur B")
    assert a1["fingerprint"] != a2["fingerprint"]


def test_validation(store):
    with pytest.raises(BrokerError):
        store.record_alert("mars", "svc", "info", "t")
    with pytest.raises(BrokerError):
        store.record_alert("etude", "svc", "fatal", "t")
    with pytest.raises(BrokerError):
        store.record_alert("etude", "svc", "info", "   ")
    with pytest.raises(BrokerError):
        store.set_alert_state("ghost", "acked")
    with pytest.raises(BrokerError):
        store.set_alert_state(store.record_alert("etude", "svc", "info", "t")[0]["alert_id"], "active")


def test_secrets_redacted_and_detail_bounded(store):
    secret = "sk-ant-" + "b" * 40
    alert, _ = store.record_alert("etude", "svc", "critical", "fuite ?", f"key {secret} " + "x" * 10_000)
    assert secret not in alert["detail"] and len(alert["detail"]) <= P.MAX_ALERT_DETAIL_CHARS
    assert alert["detail"].endswith("[tronqué]")


def test_list_filters(store):
    store.record_alert("etude", "a", "info", "i1")
    store.record_alert("etude", "a", "critical", "c1")
    store.record_alert("nexus", "b", "warning", "w1")
    assert store.list_alerts(source="nexus")["count"] == 1
    assert store.list_alerts(severity="critical")["count"] == 1
    assert store.list_alerts(state="active")["count"] == 3
    assert store.list_alerts(source="etude", severity="info")["count"] == 1
    assert store.list_alerts(source="etude", limit=1)["count"] == 1
    page = store.list_alerts(source="etude")
    assert "detail" not in page["alerts"][0]  # liste compacte, sans détail
    with pytest.raises(BrokerError):
        store.list_alerts(source="mars")


def test_ack_resolve_and_purge(store):
    clock = store.clock
    alert, _ = store.record_alert("nexus", "b", "warning", "vieux")
    assert store.set_alert_state(alert["alert_id"], "acked")["state"] == "acked"
    assert store.set_alert_state(alert["alert_id"], "resolved")["state"] == "resolved"
    clock.t += P.ALERT_RETENTION_S + 1
    stats = store.purge()
    assert stats["alerts"] == 1 and store.get_alert(alert["alert_id"]) is None


def rpc(c, method, params=None):
    r = c.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}, headers=HEADERS)
    assert r.status_code == 200, r.text
    body = r.text
    if "data:" in body:
        body = "\n".join(line[5:].strip() for line in body.splitlines() if line.startswith("data:"))
    return json.loads(body)


def call(c, name, args):
    res = rpc(c, "tools/call", {"name": name, "arguments": args})["result"]
    assert not res.get("isError"), res
    return res.get("structuredContent") or json.loads(res["content"][0]["text"])


def test_alert_tools_via_mcp(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "m.db", clock=clock)
    store.hello("pc", {"runtimes": [{"id": "fake", "available": True}], "workspaces": [{"id": "demo", "modes": ["read_only"], "description": "d"}]}, [])
    app = build_app(store, RunnerAuth({}, ["10.200.0.0/16"]))
    with TestClient(app, base_url="http://127.0.0.1:8802") as c:
        rpc(c, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}})
        names = {t["name"] for t in rpc(c, "tools/list")["result"]["tools"]}
        assert "infra_alert_list" in names and "infra_alert_get" in names
        store.record_alert("etude", "orch-mcp", "critical", "canari", "boom")
        store.record_alert("nexus", "nexus-api", "warning", "latence", "p99")
        assert call(c, "infra_alert_list", {})["count"] == 2
        assert call(c, "infra_alert_list", {"source": "nexus"})["count"] == 1
        one = call(c, "infra_alert_list", {"severity": "critical"})
        assert one["count"] == 1
        full = call(c, "infra_alert_get", {"alert_id": one["alerts"][0]["alert_id"]})
        assert full["detail"] == "boom" and full["source"] == "etude"
        assert call(c, "infra_alert_get", {"alert_id": "ghost"})["error"] == "unknown_alert"
        bad = rpc(c, "tools/call", {"name": "infra_alert_list", "arguments": {"source": "mars"}})
        assert bad.get("error") or bad["result"].get("isError")  # rejet schéma Literal
