"""Tests Linux/ POSIX du runner ARM64 + garde-fous transverses (exécutables sur Windows).

Conventions :
- les tests de processus POSIX réels sont `skip` sous Windows (pas de setsid/killpg)
  ; ils tournent sur le S9+ (Linux aarch64) via le smoke P6B ou pytest si disponible ;
- les tests transverses (config, garde workspace_write, allowlists, structure
  posproc, refus d'exécutables) tournent sur PC et protègent Windows ;
- aucun appel API payant : seuls `fake` (python local) et `--version` sont exercés ;
- aucun secret : jetons factices en tmp_path uniquement.
"""

import json
import os
import stat
import sys
import time
from pathlib import Path

import pytest

import orch_protocol as P
from orch_runner.adapters import ADAPTERS, _check_executable, _creationflags
from orch_runner.policy import (
    ENV_ALLOW_POSIX,
    ENV_ALLOW_WINDOWS,
    Config,
    PolicyError,
    RuntimeConf,
    Workspace,
    child_env,
    resolve_workspace,
)
from orch_runner.runner import workspace_write_allowed

posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX uniquement")

try:
    from orch_runner import posproc
    _POSPROC_OK = True
except ImportError:
    posproc = None  # type: ignore[assignment]
    _POSPROC_OK = False

needs_posproc = pytest.mark.skipif(not _POSPROC_OK, reason="posproc indisponible")


# ------------------------------------------------------- transverses (PC + S9)
def test_posproc_structure():
    assert _POSPROC_OK, "posproc doit s'importer (même sous Windows, fcntl dégradé)"
    assert posproc.ISOLATION == "process-group-best-effort"
    for attr in ("JobProcess", "LaunchError", "single_instance", "is_safe_path", "check_executable"):
        assert hasattr(posproc, attr), attr


def test_is_safe_path_refusals():
    assert not posproc.is_safe_path("")
    assert not posproc.is_safe_path("relative/path")
    assert not posproc.is_safe_path("/ok\x00nul")
    assert not posproc.is_safe_path("/")


def test_check_executable_refusals():
    assert _check_executable("") is not None
    assert _check_executable("relative/rt") is not None
    assert _check_executable("/ok\x00nul") is not None


def test_env_allowlists_sans_secret():
    for allow in (ENV_ALLOW_WINDOWS, ENV_ALLOW_POSIX):
        upper = {k.upper() for k in allow}
        for banned in ("ANTHROPIC_API_KEY", "AWS_SECRET_ACCESS_KEY", "ORCH_RUNNER_TOKENS", "GITHUB_TOKEN"):
            assert banned not in upper, banned
    assert "PATH" in ENV_ALLOW_POSIX and "HOME" in ENV_ALLOW_POSIX  # codex/opencode node
    assert "SYSTEMROOT" in ENV_ALLOW_WINDOWS  # Windows inchangé


def test_child_env_filtre_secrets(monkeypatch):
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "leak")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "leak")
    env = child_env("job1")
    assert "AWS_SECRET_ACCESS_KEY" not in env and "ANTHROPIC_API_KEY" not in env
    assert env["ORCH_JOB_ID"] == "job1"


def test_config_linux_flag(tmp_path):
    base = 'runner_id = "r1"\nbroker_url = "http://10.0.0.1:1"\n'
    (tmp_path / "a.toml").write_text(base)
    assert Config.load(tmp_path / "a.toml").linux_allow_workspace_write is False
    (tmp_path / "b.toml").write_text(base + "linux_allow_workspace_write = true\n")
    assert Config.load(tmp_path / "b.toml").linux_allow_workspace_write is True


def test_workspace_write_allowed_windows(monkeypatch):
    import orch_runner.runner as R

    monkeypatch.setattr(R, "PLATFORM", "nt")
    cfg = Config("r", "http://10.0.0.1:1", Path("t"), 1, {}, {}, Path("h"))
    assert workspace_write_allowed(cfg) is True


def test_workspace_write_allowed_posix_simulated(monkeypatch):
    import orch_runner.runner as R

    monkeypatch.setattr(R, "PLATFORM", "posix")
    cfg_closed = Config("r", "http://10.0.0.1:1", Path("t"), 1, {}, {}, Path("h"))
    assert workspace_write_allowed(cfg_closed) is False
    cfg_open = Config("r", "http://10.0.0.1:1", Path("t"), 1, {}, {}, Path("h"), linux_allow_workspace_write=True)
    assert workspace_write_allowed(cfg_open) is True


def test_claude_desktop_refuse_posix(monkeypatch, tmp_path):
    import orch_runner.adapters as A

    monkeypatch.setattr(A, "PLATFORM", "posix")
    info = ADAPTERS["claude-desktop"](sys.executable, {}).probe()
    assert info["available"] is False and "windows-only" in info.get("reason", "")
    with pytest.raises(ValueError):
        ADAPTERS["claude-desktop"](sys.executable, {}).build("hi", "read_only", str(tmp_path), tmp_path)


def test_creationflags_platform():
    import orch_runner.adapters as A

    assert _creationflags() == (0x08000000 if A.PLATFORM == "nt" else 0)


# ---------------------------------------------------------------- POSIX réels
def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return True


@posix_only
def test_posix_argv_hostile_roundtrip(tmp_path):
    hostile = ['"; echo PWNED', "a\"b c\\", "line1\nline2 França", "-p --flag", ""]
    code = "import sys,json; sys.stdout.write(json.dumps(sys.argv[1:]))"
    jp = posproc.JobProcess([sys.executable, "-c", code, *hostile], str(tmp_path), child_env("t"))
    jp.resume()
    out, _ = jp.proc.communicate(timeout=30)
    jp.close()
    assert json.loads(out) == hostile
    assert not (tmp_path / "PWNED").exists()


@posix_only
def test_posix_non_executable_refused(tmp_path):
    plain = tmp_path / "plain.txt"
    plain.write_text("x")
    with pytest.raises(posproc.LaunchError):
        posproc.JobProcess([str(plain)], str(tmp_path), {})
    with pytest.raises(posproc.LaunchError):
        posproc.JobProcess(["relative/rt"], str(tmp_path), {})
    with pytest.raises(posproc.LaunchError):
        posproc.JobProcess(["/etc/hostname"], str(tmp_path), {})


@posix_only
def test_posix_kill_tree_kills_grandchildren(tmp_path):
    fake = str(Path(posproc.__file__).with_name("fake_agent.py"))
    jp = posproc.JobProcess([sys.executable, "-u", fake, "x"], str(tmp_path), child_env("t"))
    jp.resume()
    jp.proc.stdin.write(b"spawn 120\nsleep 120\n")
    jp.proc.stdin.close()
    child_pid = None
    for line in iter(jp.proc.stdout.readline, b""):
        if line.startswith(b"CHILD_PID:"):
            child_pid = int(line.split(b":")[1])
            break
    assert child_pid and _pid_alive(child_pid)
    assert jp.active_processes() >= 2
    jp.kill_tree()
    jp.proc.wait(timeout=10)
    time.sleep(0.5)
    assert jp.active_processes() == 0
    assert not _pid_alive(child_pid)
    jp.close()


@posix_only
def test_posix_close_kills_survivors(tmp_path):
    jp = posproc.JobProcess([sys.executable, "-c", "import time; time.sleep(120)"], str(tmp_path), child_env("t"))
    jp.resume()
    pid = jp.pid
    jp.close()
    time.sleep(0.5)
    assert not _pid_alive(pid)


@posix_only
def test_posix_workspace_validation(tmp_path):
    real = tmp_path / "ws"
    real.mkdir()
    assert resolve_workspace(Workspace("ok", str(real), ["read_only"])) == os.path.realpath(real)
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "link").symlink_to(outside, target_is_directory=True)
    bad = [
        str(tmp_path / "link"),
        "relative/path",
        str(real / ".." / "outside"),
        "/proc/self",
        "/sys/kernel",
        "/",
        "/root/secret",
        str(tmp_path / "missing"),
    ]
    for path in bad:
        with pytest.raises(PolicyError):
            resolve_workspace(Workspace("x", path, ["read_only"]))


@posix_only
def test_posix_token_0600(tmp_path):
    from orch_runner.policy import generate_token, load_token

    tok_file = tmp_path / "runner.token"
    digest = generate_token(tok_file)
    assert len(digest) == 64
    assert stat.S_IMODE(os.stat(tok_file).st_mode) == 0o600
    assert load_token(tok_file) and len(load_token(tok_file)) >= 32
    os.chmod(tok_file, 0o644)
    with pytest.raises(PolicyError):
        load_token(tok_file)


@posix_only
def test_posix_child_env_posix():
    env = child_env("job1", {"PYTHONIOENCODING": "utf-8"})
    assert env["ORCH_JOB_ID"] == "job1" and env["PYTHONIOENCODING"] == "utf-8"
    assert "PATH" in env and "HOME" in env
    assert "AWS_SECRET_ACCESS_KEY" not in env


@posix_only
def test_posix_adapters_probe():
    fake = ADAPTERS["fake"](sys.executable, {}).probe()
    assert fake["available"], fake
    desk = ADAPTERS["claude-desktop"](sys.executable, {}).probe()
    assert desk["available"] is False


@posix_only
def test_posix_single_instance_lock(tmp_path):
    first = posproc.single_instance(tmp_path, "ut")
    assert first is not None
    # même processus : flock réentrant OK, mais un 2e descripteur doit échouer
    import fcntl as _fcntl

    fd2 = os.open(str(tmp_path / "runner-ut.lock"), os.O_CREAT | os.O_RDWR, 0o600)
    with pytest.raises(OSError):
        _fcntl.flock(fd2, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
    os.close(fd2)


# ------------------------------------------------- E2E POSIX (broker + fake)
@posix_only
def test_posix_e2e_read_only_ok_and_write_restricted(tmp_path):
    uvicorn = pytest.importorskip("uvicorn")
    _ = uvicorn
    import hashlib
    import socket
    import threading

    from orch_mcp.runner_api import RunnerAuth
    from orch_mcp.server import build_app
    from orch_mcp.store import Store
    from orch_runner.runner import BrokerClient, Runner

    TOKEN = "p" * 48

    def free_port() -> int:
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        return port

    port = free_port()
    store = Store(tmp_path / "orch.db")
    auth = RunnerAuth({hashlib.sha256(TOKEN.encode()).hexdigest(): "s9"}, ["127.0.0.0/8"])
    server = uvicorn.Server(uvicorn.Config(build_app(store, auth, reaper_interval_s=0.5), host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)

    ws = tmp_path / "workspace"
    ws.mkdir()
    cfg = Config(
        runner_id="s9",
        broker_url=f"http://127.0.0.1:{port}",
        token_file=tmp_path / "unused",
        max_parallel=2,
        workspaces={"e2e": Workspace("e2e", str(ws), ["read_only", "workspace_write"], "fixture")},
        runtimes={"fake": RuntimeConf("fake", True, sys.executable)},
        home=tmp_path / "home",
    )
    cfg.home.mkdir(exist_ok=True)
    client = BrokerClient(cfg.broker_url, TOKEN)
    client.http.headers["x-real-ip"] = "127.0.0.1"
    runner = Runner(cfg, client)
    rt = threading.Thread(target=runner.run, daemon=True)
    rt.start()
    try:
        end = time.time() + 20
        while not store.runners() or store.runners()[0]["status"] != "online":
            assert time.time() < end, "runner jamais online"
            time.sleep(0.2)
        info = store.runners()[0]
        assert info.get("isolation") == "process-group-best-effort", info

        job, _ = store.create_job("s9", "fake", "e2e", "print 2\n", "read_only", 60)
        jid = job["job_id"]
        end = time.time() + 60
        while time.time() < end:
            view = store.get_job(jid, tail_chars=8000)
            if view["state"] in P.TERMINAL:
                break
            time.sleep(0.2)
        assert view["state"] == "completed", view

        job2, _ = store.create_job("s9", "fake", "e2e", "print 1\n", "workspace_write", 60)
        jid2 = job2["job_id"]
        end = time.time() + 60
        while time.time() < end:
            view2 = store.get_job(jid2, tail_chars=8000)
            if view2["state"] in P.TERMINAL:
                break
            time.sleep(0.2)
        assert view2["state"] == "failed" and "workspace_write_restricted" in (view2["error"] or ""), view2
    finally:
        runner.stop()
        server.should_exit = True
        thread.join(timeout=35)
        store.close()
