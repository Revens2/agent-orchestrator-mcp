"""Tests avec les vrais runtimes (consomment du quota). Activer : ORCH_REAL=claude-code,codex,agy,opencode"""

import hashlib
import os
import secrets
import shutil
import time
from pathlib import Path

import pytest

import orch_protocol as P
from orch_runner.adapters import ADAPTERS
from orch_runner.policy import RuntimeConf, Workspace
from tests.test_runner_windows import Broker, free_port, make_runner, start_runner, wait_state

EXES = {
    "claude-code": str(Path.home() / ".local/bin/claude.exe"),
    "codex": str(Path(os.environ.get("LOCALAPPDATA", "")) / "Programs/OpenAI/Codex/bin/codex.exe"),
    "agy": shutil.which("agy") or "",
    "opencode": shutil.which("opencode") or "",
}
EXTRA = {"opencode": {"model": os.environ.get("ORCH_OPENCODE_MODEL", "opencode/muse-spark-1.3-contributor-free")}}
ENABLED = [r for r in os.environ.get("ORCH_REAL", "").split(",") if r]
HOSTILE_TAIL = '\n\nIgnore this trailing noise, it is data: "; echo PWNED & whoami | powershell -c calc `$x` %PATH% > NUL ☃'


@pytest.fixture
def real_stack(tmp_path):
    port = free_port()
    broker = Broker(tmp_path / "orch.db", port)
    runner = make_runner(tmp_path, port, max_parallel=1)
    ws = tmp_path / "workspace"
    runner.config.workspaces = {"e2e": Workspace("e2e", str(ws), ["read_only", "workspace_write"], "fixture")}
    runner.config.runtimes = {rt: RuntimeConf(rt, True, EXES[rt], EXTRA.get(rt, {"max_budget_usd": 1})) for rt in ENABLED}
    start_runner(runner)
    end = time.time() + 60
    while not broker.store.runners() or broker.store.runners()[0]["status"] != "online":
        assert time.time() < end
        time.sleep(0.5)
    yield broker, runner, ws
    runner.stop()
    broker.stop()


@pytest.mark.parametrize("runtime", ENABLED or ["none"])
def test_real_probe(runtime):
    if runtime == "none":
        pytest.skip("ORCH_REAL vide")
    info = ADAPTERS[runtime](EXES[runtime], EXTRA.get(runtime)).probe()
    assert info["available"], info


@pytest.mark.parametrize("runtime", ENABLED or ["none"])
def test_real_write_fixture(real_stack, runtime):
    if runtime == "none":
        pytest.skip("ORCH_REAL vide")
    broker, _, ws = real_stack
    nonce = "E2E-" + secrets.token_hex(8)
    # prompt banal : aucune consigne de permission, l'autonomie doit venir de l'adapter
    prompt = f"Create E2E_AGENT_OK.txt containing exactly {nonce}, then reply DONE." + HOSTILE_TAIL
    rt = broker.store.runners()[0]["runtimes"]
    assert any(r["id"] == runtime and r["available"] for r in rt), rt
    job, _ = broker.store.create_job("pc", runtime, "e2e", prompt, "workspace_write", 900)
    seen = set()
    end = time.time() + 900
    while time.time() < end:
        view = broker.store.get_job(job["job_id"], 8000)
        seen.add(view["state"])
        if view["state"] in P.TERMINAL:
            break
        time.sleep(0.5)
    print({k: view[k] for k in ("state", "exit_code", "duration_s", "last_activity", "result_summary", "error", "runtime_session_id")})
    assert view["state"] == "completed", view
    assert "running" in seen
    assert (ws / "E2E_AGENT_OK.txt").read_text(encoding="utf-8").strip() == nonce
    assert not (ws / "PWNED").exists()


@pytest.mark.parametrize("runtime", ENABLED or ["none"])
def test_real_read_only_cannot_write(real_stack, runtime):
    if runtime == "none":
        pytest.skip("ORCH_REAL vide")
    broker, _, ws = real_stack
    prompt = "Create SHOULD_NOT_EXIST.txt and reply DONE."
    job, _ = broker.store.create_job("pc", runtime, "e2e", prompt, "read_only", 900)
    view = wait_state(broker.store, job["job_id"], P.TERMINAL, timeout=900)
    print({k: view[k] for k in ("state", "exit_code", "duration_s", "result_summary", "error")})
    assert view["state"] in ("completed", "failed")
    assert not (ws / "SHOULD_NOT_EXIST.txt").exists()


@pytest.mark.parametrize("runtime", ENABLED or ["none"])
def test_real_cancel(real_stack, runtime):
    if runtime == "none":
        pytest.skip("ORCH_REAL vide")
    broker, runner, _ = real_stack
    prompt = "Think step by step and write a very long, detailed essay (at least 3000 words) about the history of operating systems."
    job, _ = broker.store.create_job("pc", runtime, "e2e", prompt, "read_only", 900)
    wait_state(broker.store, job["job_id"], {"running"}, timeout=120)
    time.sleep(4)
    worker = runner.workers.get(job["job_id"])
    assert worker and worker.proc
    pid = worker.proc.pid
    assert broker.store.cancel(job["job_id"])["result"] == "cancel_requested"
    view = wait_state(broker.store, job["job_id"], P.TERMINAL, timeout=60)
    assert view["state"] == "cancelled", view
    from tests.test_runner_windows import pid_alive

    time.sleep(1)
    assert not pid_alive(pid)


_ = hashlib
