"""Option `socks_proxy` facultative du runner (NetBird netstack loopback).

Conventions :
- transverse PC + S9 (aucun réseau émis, aucun secret, monkeypatch httpx.Client
  pour capturer les kwargs au lieu de connecter) ;
- fail-closed : seul le loopback 127.0.0.1/::1 en socks5/socks5h, sans userinfo ;
- "" = désactivé (défaut, comportement inchangé, trust_env=False conservé).
"""

import httpx
import pytest

from orch_runner.policy import Config, PolicyError, validate_socks_proxy
from orch_runner.runner import BrokerClient


def _minimal_toml(tmp_path, extra=""):
    (tmp_path / "ws").mkdir()
    toml = tmp_path / "runner.toml"
    toml.write_bytes(
        ("runner_id = \"s9plus-arm64\"\n"
         "broker_url = \"http://10.200.114.203:8803\"\n"
         "max_parallel = 1\n" + extra +
         "[workspaces.s9plus]\n"
         f"path = \"{(tmp_path / 'ws').as_posix()}\"\n"
         "modes = [\"read_only\"]\n").encode()
    )
    return toml


# ------------------------------------------------------- validation (transverse)
def test_socks_proxy_vide_desactive():
    assert validate_socks_proxy("") == ""


def test_socks_proxy_loopback_ok_et_normalise():
    assert validate_socks_proxy("socks5h://127.0.0.1:1080") == "socks5h://127.0.0.1:1080"
    assert validate_socks_proxy("socks5://127.0.0.1") == "socks5://127.0.0.1:1080"
    assert validate_socks_proxy("socks5h://[::1]:1080") == "socks5h://::1:1080"


@pytest.mark.parametrize("bad", [
    "socks5h://10.200.114.203:1080",  # overlay via proxy distant : interdit
    "socks5h://0.0.0.0:1080",  # exposition LAN : interdite
    "socks5h://localhost:1080",  # nom DNS : interdit (IP loopback explicite exigée)
    "http://127.0.0.1:1080",  # schéma non-SOCKS : interdit
    "socks5h://user:pass@127.0.0.1:1080",  # userinfo : interdite
    "socks5h://127.0.0.1:99999",  # port invalide
    "socks5h://127.0.0.1:1080/path",  # chemin : refusé
    "socks5h://127.0.0.1:1080?q=1",  # query : refusée
])
def test_socks_proxy_refus_fail_closed(bad):
    with pytest.raises(PolicyError):
        validate_socks_proxy(bad)


def test_config_defaut_sans_proxy(tmp_path):
    cfg = Config.load(_minimal_toml(tmp_path))
    assert cfg.socks_proxy == ""


def test_config_charge_proxy_loopback(tmp_path):
    cfg = Config.load(_minimal_toml(tmp_path, "socks_proxy = \"socks5h://127.0.0.1:1080\"\n"))
    assert cfg.socks_proxy == "socks5h://127.0.0.1:1080"


def test_config_rejette_proxy_distant(tmp_path):
    with pytest.raises(PolicyError):
        Config.load(_minimal_toml(tmp_path, "socks_proxy = \"socks5h://10.0.0.1:1080\"\n"))


# ------------------------------------------------------- câblage BrokerClient
def test_broker_client_sans_proxy_trust_env_false(monkeypatch):
    captured = {}

    class FakeClient:
        def __init__(self, **kw):
            captured.update(kw)

    monkeypatch.setattr(httpx, "Client", FakeClient)
    BrokerClient("http://10.200.114.203:8803", "jeton-factice-32-caracteres-minimum-ok")
    assert captured["proxy"] is None
    assert captured["trust_env"] is False


def test_broker_client_proxy_transmis_trust_env_false(monkeypatch):
    captured = {}

    class FakeClient:
        def __init__(self, **kw):
            captured.update(kw)

    monkeypatch.setattr(httpx, "Client", FakeClient)
    BrokerClient("http://10.200.114.203:8803", "jeton-factice-32-caracteres-minimum-ok",
                 proxy="socks5h://127.0.0.1:1080")
    assert captured["proxy"] == "socks5h://127.0.0.1:1080"
    assert captured["trust_env"] is False
