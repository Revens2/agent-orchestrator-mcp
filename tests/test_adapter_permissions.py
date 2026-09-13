"""Contrat de permissions des adapters : un retrait silencieux de l'auto-approbation doit casser ces tests."""

from pathlib import Path

import pytest

import orch_protocol as P
from orch_runner.adapters import ADAPTERS
from orch_runner.policy import Config, PolicyError

CWD = r"C:\ws\e2e"


def argv(runtime, mode, policy=P.UNATTENDED, extra=None):
    return ADAPTERS[runtime]("C:\\x\\rt.exe", extra, policy).build("Create E2E_PERMISSION_TEST.txt containing exactly OK.", mode, CWD, Path("C:/tmp")).argv


def after(args, flag):
    return args[args.index(flag) + 1]


def test_claude_unattended():
    ww = argv("claude-code", "workspace_write")
    assert after(ww, "--permission-mode") == "bypassPermissions"
    assert "--dangerously-skip-permissions" not in ww  # un seul mécanisme
    ro = argv("claude-code", "read_only")
    assert after(ro, "--permission-mode") == "plan" and after(ro, "--permission-prompts") == "none"
    assert "bypassPermissions" not in ro


def test_claude_guarded():
    ww = argv("claude-code", "workspace_write", P.GUARDED)
    assert after(ww, "--permission-mode") == "acceptEdits" and after(ww, "--permission-prompts") == "none"


def test_codex_explicit_sandbox_and_approval():
    for policy, expected in ((P.UNATTENDED, "danger-full-access"), (P.GUARDED, "workspace-write")):
        ww = argv("codex", "workspace_write", policy)
        assert after(ww, "-s") == expected and 'approval_policy="never"' in ww and after(ww, "-C") == CWD
        ro = argv("codex", "read_only", policy)
        assert after(ro, "-s") == "read-only" and 'approval_policy="never"' in ro


def test_agy():
    ww = argv("agy", "workspace_write")
    assert after(ww, "--mode") == "accept-edits" and "--dangerously-skip-permissions" in ww
    ro = argv("agy", "read_only")
    assert after(ro, "--mode") == "plan" and "--sandbox" in ro and "--dangerously-skip-permissions" not in ro
    g = argv("agy", "workspace_write", P.GUARDED)
    assert "--sandbox" in g and "--dangerously-skip-permissions" not in g


def test_opencode():
    ww = argv("opencode", "workspace_write", extra={"model": "opencode/m"})
    assert after(ww, "--agent") == "build" and "--auto" in ww and after(ww, "-m") == "opencode/m"
    assert ww[-2:] == ["--", "Create E2E_PERMISSION_TEST.txt containing exactly OK."]
    ro = argv("opencode", "read_only")
    assert after(ro, "--agent") == "plan" and "--auto" not in ro and "-m" not in ro
    guarded = ADAPTERS["opencode"]("x", None, P.GUARDED)
    assert guarded.modes == ("read_only",)
    with pytest.raises(ValueError):
        argv("opencode", "workspace_write", P.GUARDED)


def test_unknown_policy_refused():
    with pytest.raises(ValueError):
        ADAPTERS["codex"]("x", None, "yolo")


def test_config_policy(tmp_path):
    base = 'runner_id = "r1"\nbroker_url = "http://10.0.0.1:1"\n'
    (tmp_path / "a.toml").write_text(base)
    assert Config.load(tmp_path / "a.toml").permission_policy == P.UNATTENDED
    (tmp_path / "b.toml").write_text(base + 'default_permission_policy = "guarded"\n')
    assert Config.load(tmp_path / "b.toml").permission_policy == P.GUARDED
    (tmp_path / "c.toml").write_text(base + 'default_permission_policy = "yolo"\n')
    with pytest.raises(PolicyError):
        Config.load(tmp_path / "c.toml")
