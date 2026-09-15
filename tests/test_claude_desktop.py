"""Runtime `claude-desktop` : contrat (protocole/MCP), adapter, bridge, garde read_only.

Le Desktop partage le profil/l'historique de l'utilisateur : aucun confinement
au workspace n'est démontrable → `read_only` uniquement, refusé partout ailleurs
(broker `mode_denied`, adapter `ValueError`). Aucun test n'écrit dans le clone
partagé ni ne touche au compte Desktop (vérification en lecture seule).
"""

import json
import sys
import typing
from pathlib import Path

import pytest

import orch_protocol as P
from orch_mcp import tools as mcp_tools
from orch_mcp.store import BrokerError, Store
from orch_runner import adapters as A
from orch_runner import claude_desktop_bridge as B

CWD = r"C:\ws\e2e"
PY = sys.executable


# ------------------------------------------------------------------ contrat
def test_protocol_registers_runtime_read_only():
    assert "claude-desktop" in P.RUNTIMES
    assert P.RUNTIME_MODES["claude-desktop"] == ("read_only",)


def test_mcp_runtime_literal_includes_desktop():
    assert "claude-desktop" in typing.get_args(mcp_tools.RuntimeT)


def test_adapter_registered_read_only_only():
    assert A.ADAPTERS["claude-desktop"] is A.ClaudeDesktop
    ad = A.ClaudeDesktop(PY, None)
    assert ad.modes == ("read_only",)
    with pytest.raises(ValueError):
        ad.build("prompt", "workspace_write", CWD, Path("C:/tmp"))


def test_adapter_build_never_interpolates_prompt(tmp_path):
    hostile = '"; powershell -c calc `$x` %PATH% > NUL ☃'
    ad = A.ClaudeDesktop(PY, {"profile": "Caroline"})
    launch = ad.build("Dis bonjour. " + hostile, "read_only", CWD, tmp_path)
    assert launch.stdin_text is None
    assert hostile not in " ".join(launch.argv)
    prompt_file = tmp_path / "prompt.txt"
    assert prompt_file.read_text(encoding="utf-8").endswith(hostile)
    assert "--prompt-file" in launch.argv and "--out-dir" in launch.argv
    assert "Caroline" in launch.argv


def test_adapter_probe_rejects_non_exe_or_missing_bridge(monkeypatch):
    ad = A.ClaudeDesktop(r"C:\x\run.bat", None)
    assert ad.probe()["available"] is False
    ad2 = A.ClaudeDesktop(PY, None)
    monkeypatch.setattr(ad2, "_bridge", r"C:\x\nope_bridge.py")
    info = ad2.probe()
    assert info["available"] is False and "bridge" in info["reason"]


def test_adapter_result_flow(tmp_path):
    ad = A.ClaudeDesktop(PY, None)
    text, activity = ad.on_line(json.dumps({"type": "activity", "text": "saisie…"}))
    assert text is None and activity
    text, _ = ad.on_line(json.dumps({"type": "result", "text": "voici la réponse"}))
    assert "voici la réponse" in text
    out = ad.finish(0, tmp_path)
    assert out.ok and out.summary == "voici la réponse"
    # last_message.txt prime sur le flux (convention codex).
    (tmp_path / "last_message.txt").write_text("texte final", encoding="utf-8")
    out = ad.finish(0, tmp_path)
    assert out.ok and out.summary == "texte final"
    # Réponse vide => échec (jamais de completed sans texte).
    ad3 = A.ClaudeDesktop(PY, None)
    out = ad3.finish(0, tmp_path.parent / "vide-inexistant-xyz")
    assert not out.ok


# ---------------------------------------------------------------- broker ---
@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "d.db")
    s.hello(
        "pc",
        {
            "version": "test",
            "max_parallel": 2,
            "runtimes": [{"id": "claude-desktop", "available": True}],
            "workspaces": [{"id": "demo", "modes": ["read_only", "workspace_write"]}],
        },
        [],
    )
    return s


def test_broker_accepts_read_only(store):
    job, created = store.create_job("pc", "claude-desktop", "demo", "Bonjour.", "read_only")
    assert created and job["state"] == "queued"


def test_broker_refuses_workspace_write_for_desktop(store):
    with pytest.raises(BrokerError) as exc:
        store.create_job("pc", "claude-desktop", "demo", "Bonjour.", "workspace_write")
    assert exc.value.code == "mode_denied"


def test_broker_refuses_workspace_write_for_mission(store):
    with pytest.raises(BrokerError) as exc:
        store.create_mission("objectif", ["ok"], 1, "pc", "claude-desktop", "demo", "workspace_write")
    assert exc.value.code == "mode_denied"


# ---------------------------------------------------------------- bridge ---
def test_profile_match_case_insensitive():
    assert B.profile_in_texts(["Caroline · Pro", "autre"], "caroline")
    assert B.profile_in_texts(["quelqu'un d'autre"], "Caroline") is False
    assert B.profile_in_texts(["Caroline"], "") is False


def test_find_input_prefers_named_edit():
    els = [
        {"ct": "ControlType.Button", "name": "Send", "aid": "b1", "text": "Send"},
        {"ct": "ControlType.Edit", "name": "", "aid": "e1", "text": ""},
        {"ct": "ControlType.Edit", "name": "", "aid": "chat-input", "text": ""},
    ]
    edit, send = B.find_input_and_send(els)
    assert (edit or {})["aid"] == "chat-input"
    assert (send or {})["aid"] == "b1"


def test_find_input_falls_back_to_last_edit():
    els = [
        {"ct": "ControlType.Edit", "name": "", "aid": "", "text": ""},
        {"ct": "ControlType.Edit", "name": "", "aid": "", "text": ""},
    ]
    edit, send = B.find_input_and_send(els)
    assert edit is els[-1] and send is None


def test_find_send_ignores_substring_noise_and_prefers_after_edit():
    """Régression E2E 2026-09-15 : `go` matchait "Google Drive…" avant "Envoyer"."""
    els = [
        {"ct": "ControlType.Button", "name": "Inactif Google Drive API key pour rclone", "aid": "", "text": ""},
        {"ct": "ControlType.Edit", "name": "Prompt", "aid": "", "text": ""},
        {"ct": "ControlType.Button", "name": "Envoyer", "aid": "", "text": "Envoyer"},
    ]
    edit, send = B.find_input_and_send(els)
    assert (edit or {})["name"] == "Prompt"
    assert (send or {})["name"] == "Envoyer"


def test_composer_holds_detects_unsent_prompt():
    anchor = B.prompt_anchor("Reply with exactly: NEUTRE-5814 (and nothing else).")
    els = [{"ct": "ControlType.Edit", "name": "Prompt", "aid": "", "text": "Reply with exactly: NEUTRE-5814 (and nothing else)."}]
    assert B.composer_holds(els, anchor)
    els[0]["text"] = ""
    assert not B.composer_holds(els, anchor)


def test_conversation_diff_and_stability():
    assert B.conversation_diff("bonjour", "bonjour\nvoici la réponse") == "voici la réponse"
    assert B.is_stable(["a", "b", "b", "b"], stable_rounds=3)
    assert not B.is_stable(["a", "b"], stable_rounds=3)
    assert not B.is_stable(["", "", ""], stable_rounds=3)


def test_anchored_wait_never_settles_before_echo():
    """Régression E2E 2026-09-15 : la stabilité précoce ne capturait que l'écho."""
    anchor = B.prompt_anchor("Reply with exactly: PING-OK-7291 (and nothing else).")
    assert B.response_tail("ancien échange sans notre message", anchor) is None
    snap = "Vous avez dit : Reply with exactly: PING-OK-7291 (and nothing else)."
    tail = B.response_tail(snap, anchor)
    assert tail is not None and not B.response_finished(tail)
    done = "Vous avez dit : Reply with exactly: PING-OK-7291 (and nothing else). Claude a répondu : PING-OK-7291. Claude a terminé la réponse."
    tail = B.response_tail(done, anchor)
    assert tail is not None and B.response_finished(tail)
    assert "PING-OK-7291" in tail


def test_bridge_self_test_runs():
    """Ne pilote rien : verrou + powershell + UIA + fixtures (skip hors Windows)."""
    checks = B.do_self_test()
    assert isinstance(checks, dict) and checks
    if sys.platform == "win32":
        assert checks.get("heuristics_fixture") == "OK"
        assert checks.get("diff_fixture") == "OK"
        assert checks.get("profile_fixture") == "OK"


def test_bridge_error_codes_stable():
    codes = {
        "profile_mismatch": B.E_PROFILE,
        "ui_busy": B.E_BUSY,
        "claude_desktop_not_running": B.E_NOT_RUNNING,
        "claude_desktop_no_window": B.E_NO_WINDOW,
        "input_not_found": B.E_INPUT,
        "response_timeout": B.E_TIMEOUT,
        "send_failed": B.E_SEND,
    }
    for code, const in codes.items():
        assert const == code, (code, const)
