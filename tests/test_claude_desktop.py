"""Runtime claude-desktop : second compte Claude via profil CLI isolé (jamais de GUI MSIX).

Contrat :
- même binaire/flags que claude-code, seule l'auth diffère (CLAUDE_CONFIG_DIR distinct) ;
- probe = --version PUIS `claude auth status` (seul `loggedIn` lu, jamais de secret) ;
- profil non connecté => available=false + reason auth_required avec action de login claire ;
- build injecte CLAUDE_CONFIG_DIR dans le seul environnement enfant.
"""

from pathlib import Path

import orch_protocol as P
from orch_runner import adapters as A
from orch_runner.adapters import ADAPTERS


def test_runtime_registered():
    assert "claude-desktop" in P.RUNTIMES
    assert "claude-desktop" in ADAPTERS
    assert ADAPTERS["claude-desktop"] is A.ClaudeDesktop


def test_desktop_same_modes_and_flags_as_claude_code():
    tmp = Path("C:/tmp")
    for mode in ("read_only", "workspace_write"):
        cc = ADAPTERS["claude-code"]("C:\\x\\claude.exe", {}, "unattended").build("p", mode, "C:\\ws", tmp)
        dt = ADAPTERS["claude-desktop"]("C:\\x\\claude.exe", {}, "unattended").build("p", mode, "C:\\ws", tmp)
        # mêmes flags (même argv), seul l'env diffère (profil isolé)
        assert dt.argv == cc.argv
        assert dt.env_extra.get("CLAUDE_CONFIG_DIR") == str(Path.home() / A.DESKTOP_DEFAULT_DIRNAME)
        assert "CLAUDE_CONFIG_DIR" not in cc.env_extra


def test_desktop_config_dir_override():
    tmp = Path("C:/tmp")
    launch = ADAPTERS["claude-desktop"]("C:\\x\\claude.exe", {"config_dir": "C:\\profils\\desktop"}, "unattended").build(
        "p", "read_only", "C:\\ws", tmp
    )
    assert launch.env_extra["CLAUDE_CONFIG_DIR"] == "C:\\profils\\desktop"
    assert ADAPTERS["claude-code"]("C:\\x\\claude.exe", {"config_dir": "C:\\profils\\x"}, "unattended").build(
        "p", "read_only", "C:\\ws", tmp
    ).env_extra["CLAUDE_CONFIG_DIR"] == "C:\\profils\\x"
    # claude-code ambient : aucun env (comportement historique intact)
    assert ADAPTERS["claude-code"]("C:\\x\\claude.exe", {}, "unattended").build(
        "p", "read_only", "C:\\ws", tmp
    ).env_extra == {}


def test_desktop_probe_requires_auth(tmp_path, monkeypatch):
    iso = tmp_path / "iso-desktop"
    iso.mkdir()
    monkeypatch.setattr(A, "_claude_auth_logged_in", lambda exe, cfg: (False, None))
    info = ADAPTERS["claude-desktop"]("C:\\x\\claude.exe", {"config_dir": str(iso)}, "unattended").probe()
    # exe absent => raison exe, pas auth (pas de faux auth_required)
    assert info["available"] is False
    assert "reason" in info


def test_desktop_probe_unauthenticated_profile_has_clear_login_action(monkeypatch):
    monkeypatch.setattr(A.Adapter, "probe", lambda self: {"id": self.id, "available": True, "modes": ["read_only"], "version": "x"})
    monkeypatch.setattr(A, "_claude_auth_logged_in", lambda exe, cfg: (False, None))
    info = ADAPTERS["claude-desktop"]("C:\\x\\claude.exe", {"config_dir": "C:\\profils\\desktop"}, "unattended").probe()
    assert info["available"] is False
    assert "auth_required" in info["reason"]
    assert "claude auth login" in info["reason"]
    assert "CLAUDE_CONFIG_DIR" in info["reason"]
    # aucun secret dans le motif (pas d'email ni de valeur de credential ;
    # le mot "token" n'apparaît que dans la consigne "aucun token copié")
    assert "@" not in info["reason"]
    for leak in ("sk-", "sk-ant-", "bearer ", "cookie"):
        assert leak not in info["reason"].lower()


def test_desktop_probe_authenticated_is_available(monkeypatch):
    monkeypatch.setattr(A.Adapter, "probe", lambda self: {"id": self.id, "available": True, "modes": ["read_only"], "version": "x"})
    monkeypatch.setattr(A, "_claude_auth_logged_in", lambda exe, cfg: (True, None))
    info = ADAPTERS["claude-desktop"]("C:\\x\\claude.exe", {}, "unattended").probe()
    assert info["available"] is True
    assert info.get("profile") == "isolated-desktop"


def test_desktop_probe_unverifiable_is_unavailable_with_reason(monkeypatch):
    monkeypatch.setattr(A.Adapter, "probe", lambda self: {"id": self.id, "available": True, "modes": ["read_only"], "version": "x"})
    monkeypatch.setattr(A, "_claude_auth_logged_in", lambda exe, cfg: (None, "boom"))
    info = ADAPTERS["claude-desktop"]("C:\\x\\claude.exe", {}, "unattended").probe()
    assert info["available"] is False
    assert "auth_unverifiable" in info["reason"]


def test_probe_command_lists_all_runtimes_without_network(tmp_path):
    """`python -m orch_runner probe` ne doit jamais mourir (régression : lock
    manquant sur le Runner factice) et annonce claude-desktop avec son vrai état."""
    import json

    from orch_runner.__main__ import main
    from orch_runner.policy import Config

    (tmp_path / "ws").mkdir()
    (tmp_path / "runner.toml").write_text(
        'runner_id = "pc"\nbroker_url = "http://10.0.0.1:1"\nmax_parallel = 1\n'
        '[runtimes.fake]\nenabled = true\n'
        f"exe = '{__import__('sys').executable}'\n"
        '[runtimes.claude-desktop]\nenabled = true\n'
        "exe = 'C:\\x\\claude.exe'\n"
        f"[workspaces.e2e]\npath = '{tmp_path / 'ws'}'\nmodes = [\"read_only\"]\n",
    )
    cfg = Config.load(tmp_path / "runner.toml")
    assert "claude-desktop" in cfg.runtimes
    assert main(["probe", "--config", str(tmp_path / "runner.toml")]) == 0


def test_auth_helper_parses_only_logged_in(monkeypatch):
    import json
    import subprocess as sp

    class _Out:
        stdout = json.dumps({"loggedIn": True, "email": "a@b.c", "orgId": "x"})
        stderr = ""

    monkeypatch.setattr(sp, "run", lambda *a, **k: _Out())
    ok, err = A._claude_auth_logged_in("C:\\x\\claude.exe", "C:\\iso")
    assert ok is True and err is None

    class _Out2:
        stdout = json.dumps({"loggedIn": False})
        stderr = ""

    monkeypatch.setattr(sp, "run", lambda *a, **k: _Out2())
    ok, _ = A._claude_auth_logged_in("C:\\x\\claude.exe", "C:\\iso")
    assert ok is False
