
import pytest

pytest.importorskip("fcntl", reason="passerelle Linux (fcntl) : testee sur le VPS")

from starlette.testclient import TestClient

from orch_gateway.politique import OUTILS_ADMIN, OUTILS_ECRITURE, OUTILS_LECTURE, PolitiqueOutils
from orch_mcp.tools import TOOLS_READ, TOOLS_WRITE


def test_policy_matches_upstream_tools():
    assert OUTILS_LECTURE == TOOLS_READ
    assert OUTILS_ECRITURE == TOOLS_WRITE
    assert not OUTILS_ADMIN


def test_policy_scopes():
    pol = PolitiqueOutils()
    assert pol.autoriser_call("agent_job_start", {"orch:lecture"}) is not None
    assert pol.autoriser_call("agent_job_start", {"orch:lecture", "orch:ecriture"}) is None
    assert pol.autoriser_call("agent_job_get", {"orch:lecture"}) is None
    assert pol.autoriser_call("shell", {"orch:lecture", "orch:ecriture"}) is not None
    assert "agent_job_start" not in pol.visibles({"orch:lecture"})


def test_gateway_requires_auth(monkeypatch, tmp_path):
    monkeypatch.setenv("ORCH_GW_ISSUER", "https://mymcps.duckdns.org/oauth/orch")
    monkeypatch.setenv("ORCH_GW_UPSTREAM", "http://127.0.0.1:8802")
    monkeypatch.setenv("ORCH_GW_OAUTH_DIR", str(tmp_path))
    monkeypatch.delenv("ORCH_GW_TOKEN", raising=False)
    from orch_gateway.app import construire_application

    app = construire_application(repertoire_oauth=tmp_path)
    c = TestClient(app, base_url="http://127.0.0.1:8801")
    assert c.get("/health").json()["service"] == "orch-gateway"
    r = c.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert r.status_code == 401
    assert "resource_metadata" in r.headers.get("www-authenticate", "")
    assert c.get("/.well-known/oauth-authorization-server").status_code == 200
    assert c.get("/anything").status_code == 404
    # un Bearer arbitraire (jeton statique désactivé) est refusé
    assert c.post("/mcp", json={}, headers={"authorization": "Bearer " + "x" * 40}).status_code == 401
