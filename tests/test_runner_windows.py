"""Tests Windows réels : Job Object, argv hostile, allowlist workspace, runner <-> broker HTTP."""

import _winapi
import hashlib
import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
import uvicorn

import orch_protocol as P
from orch_mcp.runner_api import RunnerAuth
from orch_mcp.server import build_app
from orch_mcp.store import Store
from orch_runner import winproc
from orch_runner.policy import Config, PolicyError, RuntimeConf, Workspace, child_env, resolve_workspace
from orch_runner.runner import BrokerClient, Runner

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows uniquement")

HOSTILE = [
    '"; echo PWNED & whoami | powershell -c calc',
    "a\\\"b c\\\\",
    "%PATH% ^& ^| `$(x)` $env:USERNAME > NUL < CON",
    "line1\nline2\r\n\ttab ☃ é 漢字",
    "'single' \"double\" \\",
    "-p --dangerously-skip-permissions",
    "",
]


def pid_alive(pid: int) -> bool:
    out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True).stdout
    return str(pid) in out


# ------------------------------------------------------------------ winproc
def test_argv_hostile_roundtrip(tmp_path):
    code = "import sys,json; sys.stdout.write(json.dumps(sys.argv[1:]))"
    jp = winproc.JobProcess([sys.executable, "-c", code, *HOSTILE], str(tmp_path), child_env("t"))
    jp.resume()
    out, _ = jp.proc.communicate(timeout=30)
    jp.close()
    assert json.loads(out) == HOSTILE
    assert not (tmp_path / "PWNED").exists()


@pytest.mark.parametrize("exe", ["C:\\Windows\\System32\\cmd.exe.cmd", "notepad.exe", "C:\\x\\run.bat"])
def test_non_exe_refused(tmp_path, exe):
    with pytest.raises(winproc.LaunchError):
        winproc.JobProcess([exe], str(tmp_path), {})


def test_suspended_process_never_runs_if_killed(tmp_path):
    marker = tmp_path / "ran.txt"
    code = f"open(r'{marker}','w').write('x')"
    jp = winproc.JobProcess([sys.executable, "-c", code], str(tmp_path), child_env("t"))
    time.sleep(1.0)
    jp.kill_tree()
    jp.proc.wait(timeout=10)
    jp.close()
    assert not marker.exists()


def test_kill_tree_kills_grandchildren(tmp_path):
    jp = winproc.JobProcess([sys.executable, "-u", str(Path(winproc.__file__).with_name("fake_agent.py")), "x"], str(tmp_path), child_env("t"))
    jp.resume()
    jp.proc.stdin.write(b"spawn 120\nsleep 120\n")
    jp.proc.stdin.close()
    child_pid = None
    for line in iter(jp.proc.stdout.readline, b""):
        if line.startswith(b"CHILD_PID:"):
            child_pid = int(line.split(b":")[1])
            break
    assert child_pid and pid_alive(child_pid)
    assert jp.active_processes() >= 2
    jp.kill_tree()
    jp.proc.wait(timeout=10)
    time.sleep(0.5)
    assert jp.active_processes() == 0
    assert not pid_alive(child_pid)
    jp.close()


def test_job_close_kills_survivors(tmp_path):
    jp = winproc.JobProcess([sys.executable, "-c", "import time; time.sleep(120)"], str(tmp_path), child_env("t"))
    jp.resume()
    pid = jp.pid
    jp.close()
    time.sleep(0.5)
    assert not pid_alive(pid)


def test_child_env_is_allowlisted(monkeypatch):
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "leak")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "leak")
    env = child_env("job1")
    assert "AWS_SECRET_ACCESS_KEY" not in env and "ANTHROPIC_API_KEY" not in env
    assert env["ORCH_JOB_ID"] == "job1" and "SYSTEMROOT" in env


# ---------------------------------------------------------------- workspace
def test_workspace_validation(tmp_path):
    real = tmp_path / "ws"
    real.mkdir()
    assert resolve_workspace(Workspace("ok", str(real), ["read_only"])) == os.path.realpath(real)
    outside = tmp_path / "outside"
    outside.mkdir()
    junction = tmp_path / "junction"
    _winapi.CreateJunction(str(outside), str(junction))
    nested = real / "link"
    _winapi.CreateJunction(str(outside), str(nested))
    bad = [
        str(junction),
        str(nested),
        "relative\\path",
        str(real / ".." / "outside"),
        "\\\\server\\share",
        "\\\\?\\C:\\Windows",
        "//server/share",
        str(tmp_path / "missing"),
    ]
    for path in bad:
        with pytest.raises(PolicyError):
            resolve_workspace(Workspace("x", path, ["read_only"]))


# ----------------------------------------------------------- E2E HTTP réel
TOKEN = "r" * 48


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Broker:
    def __init__(self, db: Path, port: int):
        self.store = Store(db)
        auth = RunnerAuth({hashlib.sha256(TOKEN.encode()).hexdigest(): "pc"}, ["127.0.0.0/8"])
        self.server = uvicorn.Server(uvicorn.Config(build_app(self.store, auth, reaper_interval_s=0.5), host="127.0.0.1", port=port, log_level="warning"))
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        for _ in range(100):
            if self.server.started:
                return
            time.sleep(0.05)
        raise RuntimeError("broker not started")

    def stop(self):
        self.server.should_exit = True
        # Un long-poll claim (25 s) peut être en vol : attendre sa fin avant de
        # fermer la DB, sinon ProgrammingError en tâche de fond (race teardown).
        self.thread.join(timeout=35)
        self.store.close()


def make_runner(tmp_path: Path, port: int, max_parallel=2, offline_kill_s=120) -> Runner:
    ws = tmp_path / "workspace"
    ws.mkdir(exist_ok=True)
    cfg = Config(
        runner_id="pc",
        broker_url=f"http://127.0.0.1:{port}",
        token_file=tmp_path / "unused",
        max_parallel=max_parallel,
        workspaces={"e2e": Workspace("e2e", str(ws), ["read_only", "workspace_write"], "fixture")},
        runtimes={"fake": RuntimeConf("fake", True, sys.executable)},
        home=tmp_path / "home",
        offline_kill_s=offline_kill_s,
    )
    cfg.home.mkdir(exist_ok=True)
    client = BrokerClient(cfg.broker_url, TOKEN)
    client.http.headers["x-real-ip"] = "127.0.0.1"  # posé par nginx en production
    return Runner(cfg, client)


def start_runner(runner: Runner) -> threading.Thread:
    t = threading.Thread(target=runner.run, daemon=True)
    t.start()
    return t


def wait_state(store: Store, job_id: str, states, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        job = store.get_job(job_id, tail_chars=8000)
        if job["state"] in states:
            return job
        time.sleep(0.2)
    raise AssertionError(f"timeout, état = {store.get_job(job_id)['state']}")


@pytest.fixture
def stack(tmp_path):
    port = free_port()
    broker = Broker(tmp_path / "orch.db", port)
    runner = make_runner(tmp_path, port)
    start_runner(runner)
    end = time.time() + 20
    while not broker.store.runners() or broker.store.runners()[0]["status"] != "online":
        assert time.time() < end, "runner jamais online"
        time.sleep(0.2)
    yield broker, runner, tmp_path, port
    runner.stop()
    broker.stop()


def submit(store, prompt, mode="workspace_write", timeout_s=None):
    job, _ = store.create_job("pc", "fake", "e2e", prompt, mode, timeout_s)
    return job["job_id"]


def test_e2e_success_with_hostile_prompt_and_file(stack):
    broker, _, tmp_path, _ = stack
    hostile = "\n".join(HOSTILE)
    prompt = f"print 3\nwrite E2E_AGENT_OK.txt {{JOB}}\necho-prompt-sha\n{hostile}\n"
    job_id = submit(broker.store, prompt)
    # le fichier doit contenir l'id : on régénère le prompt avec l'id réel impossible avant création -> 2e job
    done = wait_state(broker.store, job_id, P.TERMINAL)
    assert done["state"] == "completed" and done["exit_code"] == 0
    assert f"PROMPT_SHA256:{hashlib.sha256(prompt.encode()).hexdigest()}" in done["output_tail"]
    assert done["result_summary"] == "fake done code=0"
    assert (tmp_path / "workspace" / "E2E_AGENT_OK.txt").read_text() == "{JOB}"
    assert not (tmp_path / "workspace" / "PWNED").exists()
    transitions = [r[0] for r in broker.store._db.execute("SELECT dst FROM transitions WHERE job_id=? ORDER BY rowid", (job_id,))]
    assert transitions == ["queued", "claimed", "starting", "running", "completed"]


def test_e2e_failure_exit_code(stack):
    broker, *_ = stack
    job = wait_state(broker.store, submit(broker.store, "print 1\nfail 3\n"), P.TERMINAL)
    assert job["state"] == "failed" and job["exit_code"] == 3


def test_e2e_cancel_kills_tree(stack):
    broker, *_ = stack
    job_id = submit(broker.store, "spawn 300\nsleep 300\n")
    job = wait_state(broker.store, job_id, {"running"})
    end = time.time() + 20
    child = None
    while time.time() < end and child is None:
        tail = broker.store.get_job(job_id, 8000).get("output_tail") or ""
        if "CHILD_PID:" in tail:
            child = int(tail.split("CHILD_PID:")[1].split()[0])
        time.sleep(0.2)
    assert child and pid_alive(child)
    assert broker.store.cancel(job_id)["result"] == "cancel_requested"
    job = wait_state(broker.store, job_id, P.TERMINAL, timeout=30)
    assert job["state"] == "cancelled"
    time.sleep(0.5)
    assert not pid_alive(child)


def test_e2e_timeout(stack):
    broker, *_ = stack
    job = wait_state(broker.store, submit(broker.store, "sleep 120\n", timeout_s=30), P.TERMINAL, timeout=60)
    assert job["state"] == "timeout"


def test_e2e_concurrency_limit_and_queue(stack):
    broker, *_ = stack
    ids = [submit(broker.store, f"sleep 3\nprint {i}\n") for i in range(5)]
    peak = 0
    end = time.time() + 60
    while time.time() < end:
        states = [broker.store.get_job(i, 0)["state"] for i in ids]
        peak = max(peak, sum(s in P.ACTIVE for s in states))
        if all(s in P.TERMINAL for s in states):
            break
        time.sleep(0.1)
    assert peak <= 2
    assert all(broker.store.get_job(i, 0)["state"] == "completed" for i in ids)


def test_e2e_huge_output_bounded(stack):
    broker, *_ = stack
    job = wait_state(broker.store, submit(broker.store, "flood 6000000\n"), P.TERMINAL, timeout=120)
    assert job["state"] == "completed"
    assert job["output_chars"] <= P.MAX_OUTPUT_CHARS_PER_JOB
    assert len(job["output_tail"]) <= 8000


def test_e2e_workspace_refused_locally(stack):
    broker, runner, *_ = stack
    job_id = submit(broker.store, "print 1\n")
    runner.config.workspaces["e2e"].path = "\\\\evil\\share"  # dérive entre annonce et lancement
    job = wait_state(broker.store, job_id, P.TERMINAL)
    assert job["state"] == "failed" and "workspace_denied" in job["error"]


def test_e2e_broker_restart_job_survives(tmp_path):
    port = free_port()
    broker = Broker(tmp_path / "orch.db", port)
    runner = make_runner(tmp_path, port)
    start_runner(runner)
    time.sleep(2)
    job_id = submit(broker.store, "sleep 20\nprint 1\n")
    wait_state(broker.store, job_id, {"running"})
    broker.stop()
    time.sleep(8)  # broker absent, runner garde le processus
    broker = Broker(tmp_path / "orch.db", port)
    job = wait_state(broker.store, job_id, P.TERMINAL, timeout=60)
    assert job["state"] == "completed"
    runner.stop()
    broker.stop()


def test_e2e_runner_restart_resumes_fake_job(tmp_path):
    port = free_port()
    broker = Broker(tmp_path / "orch.db", port)
    r1 = make_runner(tmp_path, port)
    start_runner(r1)
    time.sleep(2)
    job_id = submit(broker.store, "sleep 8\nprint 42\n")
    wait_state(broker.store, job_id, {"running"})
    # crash brutal du runner : le journal local survit, le broker parque (pas lost)
    r1.stop()
    with r1.lock:
        victims = list(r1.workers.values())
        for w in victims:
            w.abandon_event.set()
            if w.proc is not None:
                w.proc.kill_tree()  # tuer AVANT close (close seul ne garantit rien en test)
    for w in victims:  # attendre la mort réelle avant de relancer (sinon l'orphelin lent parque)
        w.done.wait(timeout=30)
        assert w.done.is_set()
    with r1.lock:
        for w in victims:
            if w.proc is not None:
                w.proc.close()
    # le broker n'a pas encore parqué (bail 60 s) : le hello avec journal rattache
    r2 = make_runner(tmp_path, port)
    start_runner(r2)
    job = wait_state(broker.store, job_id, P.TERMINAL, timeout=90)
    assert job["state"] == "completed"  # fake = fixture idempotente : reprise contrôlée
    assert broker.store.get_job(job_id)["attempt"] == 1  # même job, pas de nouveau claim
    kinds = [e["kind"] for e in broker.store.read_events(job_id)["events"]]
    assert "runner_recovering" in kinds
    r2.stop()
    broker.stop()


def test_e2e_runner_restart_without_journal_parks_no_relaunch(tmp_path):
    """Sans journal (vieux runner / journal supprimé) : parking explicite, jamais relancé."""
    import shutil as _shutil

    port = free_port()
    broker = Broker(tmp_path / "orch.db", port)
    r1 = make_runner(tmp_path, port)
    start_runner(r1)
    time.sleep(2)
    job_id = submit(broker.store, "sleep 60\n")
    wait_state(broker.store, job_id, {"running"})
    r1.stop()
    with r1.lock:
        victims = list(r1.workers.values())
        for w in victims:
            w.abandon_event.set()
            if w.proc is not None:
                w.proc.kill_tree()
    for w in victims:
        w.done.wait(timeout=30)
    with r1.lock:
        for w in victims:
            if w.proc is not None:
                w.proc.close()
    _shutil.rmtree(tmp_path / "home" / "recovery", ignore_errors=True)  # simule l'absence de journal
    r2 = make_runner(tmp_path, port)
    start_runner(r2)
    end = time.time() + 20
    parked = None
    while time.time() < end:
        parked = broker.store.get_job(job_id)
        if parked["recovery_state"] == "suspended":
            break
        time.sleep(0.5)
    assert parked is not None and parked["state"] == "running"
    assert parked["recovery_state"] == "suspended"  # parqué, en attente de décision humaine
    assert broker.store.get_job(job_id)["attempt"] == 1  # jamais relancé
    r2.stop()
    broker.stop()


def test_e2e_runner_offline_keeps_job_no_kill(tmp_path):
    port = free_port()
    broker = Broker(tmp_path / "orch.db", port)
    runner = make_runner(tmp_path, port, offline_kill_s=15)
    start_runner(runner)
    time.sleep(2)
    job_id = submit(broker.store, "sleep 25\nprint 7\n")
    wait_state(broker.store, job_id, {"running"})
    broker.stop()
    time.sleep(20)  # runner isolé > offline_kill_s : processus CONSERVÉ, sortie bufferisée
    assert runner.workers  # plus de mise à mort sur perte réseau
    broker = Broker(tmp_path / "orch.db", port)
    job = wait_state(broker.store, job_id, P.TERMINAL, timeout=120)
    assert job["state"] == "completed"  # reprise automatique au lieu de lost
    runner.stop()
    broker.stop()
