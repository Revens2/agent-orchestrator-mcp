"""Reprise sur session corrompue : signature exacte, nouvelle session, anti-boucle."""

import tempfile
from pathlib import Path

import pytest

import orch_protocol as P
from orch_mcp.store import BrokerError, Store
from orch_runner.adapters import OpenCode


def test_match_corruption_exact_only():
    assert P.match_corruption("... failed to load plugin ...") == "failed to load plugin"
    assert P.match_corruption("database disk image is malformed") == "database disk image is malformed"
    assert P.match_corruption("Unexpected end of JSON input") == "Unexpected end of JSON input"
    assert P.match_corruption("une simple error quelque part") is None
    assert P.match_corruption("error 42") is None
    assert P.match_corruption(None) is None
    assert P.match_corruption("") is None


def _adapter():
    return OpenCode("C:\\opencode.exe", {"model": "m"})


def test_adapter_detects_corruption_in_tail():
    a = _adapter()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("Refondre la doc", "read_only", "C:\\w", Path(tmp))
    a.on_line("démarrage...\n")
    a.on_line('{"type":"error","error":{"name":"x","data":{"message":"failed to load plugin path=list"}}}\n')
    out = a.finish(1, Path(tmp))
    assert not out.ok and out.error == "session_corrupted:failed to load plugin"
    assert "mission_retry" in (out.summary or "") and "Refondre la doc" in (out.summary or "")


def test_adapter_plain_failure_is_not_corruption():
    a = _adapter()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("Refondre la doc", "read_only", "C:\\w", Path(tmp))
    a.on_line("Rate limit exceeded, retry later\n")
    out = a.finish(1, Path(tmp))
    assert not out.ok and not (out.error or "").startswith(P.SESSION_CORRUPTED_PREFIX)


def test_adapter_success_unaffected():
    a = _adapter()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("Refondre la doc", "read_only", "C:\\w", Path(tmp))
    a.on_line('{"type":"text","part":{"type":"text","text":"ok","sessionID":"ses_1"}}\n')
    out = a.finish(0, Path(tmp))
    assert out.ok


INFO = {
    "version": "test",
    "max_parallel": 2,
    "runtimes": [{"id": "fake", "available": True}],
    "workspaces": [{"id": "demo", "modes": ["read_only", "workspace_write"]}],
}


def _fail_running(store, epoch, job_id, fencing, error):
    store.transition("pc", epoch, job_id, fencing, "claimed", "starting")
    store.transition("pc", epoch, job_id, fencing, "starting", "running")
    return store.transition("pc", epoch, job_id, fencing, "running", "failed", exit_code=1, error=error)


def test_corruption_event_retry_recreated_then_antiloop(tmp_path):
    store = Store(tmp_path / "c.db")
    epoch = store.hello("pc", INFO, [])
    m = store.create_mission("Refondre la doc", ["ok"], max_attempts=3,
                             runner_id="pc", runtime="fake", workspace_id="demo")
    mid, job1 = m["mission_id"], m["current_job_id"]
    f1 = store.get_job(job1)["attempt"]
    claimed = store.claim("pc", epoch, 1)
    assert claimed[0]["job_id"] == job1
    _fail_running(store, epoch, job1, claimed[0]["fencing"],
                  "session_corrupted:failed to load plugin | stderr: ...")
    kinds = [e["kind"] for e in store.read_events(job1, -1, 50)["events"]]
    assert P.EV_SESSION_CORRUPTED in kinds  # événement structuré visible

    m2 = store.retry_mission(mid)  # nouvelle session propre, une seule fois
    job2 = m2["current_job_id"]
    assert job2 != job1
    kinds2 = [e["kind"] for e in store.read_events(job2, -1, 50)["events"]]
    assert kinds2.count(P.EV_SESSION_RECREATED) == 1
    evts = store.read_events(job2, -1, 50)["events"]
    ev2 = next(e for e in evts if e["kind"] == P.EV_SESSION_RECREATED)
    assert job1 in (ev2["detail"] or "")  # handoff pointe le job abandonné, pas son contenu

    claimed2 = store.claim("pc", epoch, 1)
    _fail_running(store, epoch, job2, claimed2[0]["fencing"], "session_corrupted:SQLITE_CORRUPT")
    with pytest.raises(BrokerError) as exc:
        store.retry_mission(mid)  # 2 corruptions d'affilée => stop, pas de boucle
    assert exc.value.code == "session_corruption_loop"
    _ = f1


def test_non_corruption_failure_retry_unaffected(tmp_path):
    store = Store(tmp_path / "c.db")
    epoch = store.hello("pc", INFO, [])
    m = store.create_mission("Doc", ["ok"], max_attempts=2, runner_id="pc", runtime="fake", workspace_id="demo")
    claimed = store.claim("pc", epoch, 1)
    _fail_running(store, epoch, m["current_job_id"], claimed[0]["fencing"], "exit code 1")
    m2 = store.retry_mission(m["mission_id"])
    kinds = [e["kind"] for e in store.read_events(m2["current_job_id"], -1, 50)["events"]]
    assert P.EV_SESSION_RECREATED not in kinds
