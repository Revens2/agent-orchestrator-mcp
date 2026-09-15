"""E2E live `claude-desktop` via broker + runner locaux (opt-in).

Exécution : ORCH_DESKTOP_E2E=1 python -m pytest tests/test_claude_desktop_live.py -q
Exige Claude Desktop ouvert sur le profil attendu (ORCH_DESKTOP_PROFILE, défaut
"Caroline"). Prompt neutre, trace assumée dans l'historique Desktop (read_only).
Ne touche ni au clone partagé ni au compte Desktop (vérification seule).
"""

import os
import secrets
import sys
import time

import pytest

import orch_protocol as P
from orch_runner.policy import RuntimeConf, Workspace
from tests.test_runner_windows import Broker, free_port, make_runner, start_runner, wait_state

PROFILE = os.environ.get("ORCH_DESKTOP_PROFILE", os.environ.get("ORCH_CLAUDE_DESKTOP_PROFILE", "Caroline"))
ENABLED = os.environ.get("ORCH_DESKTOP_E2E", "").lower() in ("1", "true", "yes")


@pytest.fixture
def stack(tmp_path):
    if not ENABLED:
        pytest.skip("ORCH_DESKTOP_E2E=1 requis (pilote le vrai Desktop)")
    port = free_port()
    broker = Broker(tmp_path / "orch.db", port)
    runner = make_runner(tmp_path, port, max_parallel=1)
    ws = tmp_path / "workspace"
    ws.mkdir(parents=True, exist_ok=True)
    runner.config.workspaces = {"e2e": Workspace("e2e", str(ws), ["read_only"], "live-desktop")}
    runner.config.runtimes = {
        "claude-desktop": RuntimeConf("claude-desktop", True, sys.executable, {"profile": PROFILE})
    }
    start_runner(runner)
    end = time.time() + 120
    while True:
        runners = broker.store.runners()
        if runners and runners[0]["status"] == "online":
            break
        assert time.time() < end, "runner local jamais online"
        time.sleep(0.5)
    rt = broker.store.runners()[0]["runtimes"]
    if not any(r["id"] == "claude-desktop" and r["available"] for r in rt):
        runner.stop()
        broker.stop()
        pytest.skip(f"Desktop indisponible pour le profil {PROFILE!r} : {rt}")
    yield broker, runner, ws
    runner.stop()
    broker.stop()


def test_desktop_live_neutral_prompt(stack):
    broker, _, _ = stack
    nonce = "LIVE-" + secrets.token_hex(4)
    prompt = f"Reply with exactly: {nonce} (and nothing else)."
    job, _ = broker.store.create_job("pc", "claude-desktop", "e2e", prompt, "read_only", 900)
    view = wait_state(broker.store, job["job_id"], P.TERMINAL, timeout=900)
    print({k: view[k] for k in ("state", "exit_code", "duration_s", "last_activity", "result_summary", "error")})
    assert view["state"] == "completed", view
    assert nonce in (view["result_summary"] or ""), view
