"""Reprise PC : journal local atomique, parking broker, reprise de session.

Cross-platform (pas de Job Object) : journal + broker (horloge factice) + argv resume.
Les scénarios Windows réels (fake agent + restart) vivent dans test_runner_windows.py.
"""

import json

import orch_protocol as P
from orch_mcp.store import Store
from orch_runner import recovery


class Clock:
    def __init__(self):
        self.t = 2_000_000.0

    def __call__(self):
        return self.t


INFO = {
    "version": "test",
    "max_parallel": 2,
    "runtimes": [{"id": "fake", "available": True}],
    "workspaces": [{"id": "demo", "modes": ["read_only", "workspace_write"]}],
}


def make_store(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "r.db", clock=clock)
    epoch = store.hello("pc", INFO, [])
    return store, clock, epoch


def to_running(store, epoch, c):
    store.transition("pc", epoch, c["job_id"], c["fencing"], "claimed", "starting")
    store.transition("pc", epoch, c["job_id"], c["fencing"], "starting", "running")


# ---------------------------------------------------------------- journal
def test_journal_atomic_and_required_fields(tmp_path):
    home = tmp_path / "home"
    job = {"job_id": "j1", "fencing": 3, "runtime": "fake", "workspace_id": "demo",
           "mode": "read_only", "timeout_s": 60}
    recovery.save(home, recovery.build_record(job=job, phase="claimed", prompt="print 1\n"))
    recs = recovery.load_all(home)
    assert len(recs) == 1
    rec = recs[0]
    assert rec["job_id"] == "j1" and rec["fencing"] == 3 and rec["phase"] == "claimed"
    assert rec["prompt"] == "print 1\n" and rec["prompt_sha256"] is not None
    assert rec["runtime"] == "fake" and rec["mode"] == "read_only"


def test_journal_partial_write_never_breaks_boot(tmp_path):
    home = tmp_path / "home"
    d = recovery.recovery_dir(home)
    d.mkdir(parents=True)
    (d / "half.json").write_text('{"job_id": "x", "fencing":', encoding="utf-8")
    (d / "notadict.json").write_text("[1,2]", encoding="utf-8")
    assert recovery.load_all(home) == []  # ignorés, pas d'exception
    leftovers = [p.suffixes for p in d.iterdir()]
    assert leftovers  # quarantaines .corrupt-* présentes, originaux renommés
    assert recovery.load_all(home) == []


def test_journal_clear(tmp_path):
    home = tmp_path / "home"
    job = {"job_id": "j9", "fencing": 1}
    recovery.save(home, recovery.build_record(job=job, phase="running", prompt="p"))
    assert len(recovery.load_all(home)) == 1
    recovery.clear(home, "j9")
    assert recovery.load_all(home) == []


# ---------------------------------------------------------------- broker
def test_broker_parks_before_lost_and_recovers(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    job, _ = store.create_job("pc", "fake", "demo", "print 1\n", "read_only")
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    clock.t += P.LEASE_S + 1
    assert store.reap() == {"requeued": 0, "lost": 0, "suspended": 1, "timeout_cancel": 0,
                            "failed": 0, "stalled": 0, "suspected_stall": 0}
    view = store.get_job(c["job_id"])
    assert view["state"] == "running" and view["recovery_state"] == P.SUSPENDED
    assert P.EV_JOB_SUSPENDED in [e["kind"] for e in store.read_events(c["job_id"])["events"]]
    # Reprise avec journal : rattachement + compteur, pas de nouveau job.
    e2 = store.hello("pc", INFO, [{"job_id": c["job_id"], "fencing": c["fencing"], "recovering": True}])
    assert store.get_job(c["job_id"])["recovery_state"] == P.RECOVERING
    store.transition("pc", e2, c["job_id"], c["fencing"], "running", "completed", exit_code=0)
    done = store.get_job(c["job_id"])
    assert done["state"] == "completed" and done["recovery_state"] is None and done["resume_count"] == 1


def test_broker_suspended_heartbeat_keeps_lease(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    job, _ = store.create_job("pc", "fake", "demo", "print 1\n", "read_only")
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    e2 = store.hello("pc", INFO, [{"job_id": c["job_id"], "fencing": 1, "suspended": True,
                                   "recovery_detail": "parqué test"}])
    assert store.get_job(c["job_id"])["recovery_state"] == P.SUSPENDED
    clock.t += P.LEASE_S - 5
    hb = store.heartbeat("pc", e2, [{"job_id": c["job_id"], "fencing": 1, "suspended": True}])
    assert hb["abandon"] == [] and hb["cancel"] == []
    clock.t += P.LEASE_S - 10  # bail entretenu par heartbeat : toujours actif, jamais lost
    stats = store.reap()
    assert stats["lost"] == 0 and stats["suspended"] == 0
    assert store.get_job(c["job_id"])["state"] == "running"


def test_broker_grace_expiry_then_lost(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    job, _ = store.create_job("pc", "fake", "demo", "print 1\n", "read_only")
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    store.hello("pc", INFO, [])  # sans journal : parking immédiat, pas lost
    assert store.get_job(c["job_id"])["recovery_state"] == P.SUSPENDED
    clock.t += P.RECOVERY_GRACE_S + 1
    assert store.reap()["lost"] == 1
    assert store.get_job(c["job_id"])["state"] == "lost"


def test_broker_migration_additive(tmp_path):
    store, _, _ = make_store(tmp_path)
    cols = {r["name"] for r in store._db.execute("PRAGMA table_info(jobs)").fetchall()}
    assert {"recovery_state", "recovery_detail", "resume_count"} <= cols
    store.close()
    # Rejeu idempotent sur la même base (rollback = ancien src : colonnes ignorées).
    store2 = Store(tmp_path / "r.db")
    cols2 = {r["name"] for r in store2._db.execute("PRAGMA table_info(jobs)").fetchall()}
    assert {"recovery_state", "recovery_detail", "resume_count"} <= cols2
    store2.close()


# ---------------------------------------------------------------- adapters
def test_adapter_resume_argv(tmp_path):
    from orch_runner.adapters import ADAPTERS

    oc = ADAPTERS["opencode"]("C:\\x\\opencode.exe", {"model": "m"}, "unattended")
    launch = oc.resume("sess-1", "workspace_write", str(tmp_path), tmp_path)
    assert "--session" in launch.argv and "sess-1" in launch.argv
    assert launch.argv[0].endswith(".exe")

    cl = ADAPTERS["claude-code"]("C:\\x\\claude.exe", {}, "unattended")
    launch_c = cl.resume("sess-9", "read_only", str(tmp_path), tmp_path)
    assert "--resume" in launch_c.argv and "sess-9" in launch_c.argv
    assert launch_c.stdin_text and "sess-9" in launch_c.stdin_text

    # Non résumables v1 : parking explicite, pas de relance.
    assert ADAPTERS["codex"]("C:\\x\\codex.exe", {}, "unattended").resume("s", "read_only", str(tmp_path), tmp_path) is None
    assert ADAPTERS["agy"]("C:\\x\\agy.exe", {}, "unattended").resume("s", "read_only", str(tmp_path), tmp_path) is None
    assert ADAPTERS["fake"](None, {}, "unattended").resume("s", "read_only", str(tmp_path), tmp_path) is None
    # opencode guarded + workspace_write : refusé (parking).
    ocg = ADAPTERS["opencode"]("C:\\x\\opencode.exe", {}, "guarded")
    assert ocg.resume("s", "workspace_write", str(tmp_path), tmp_path) is None


def test_recovery_record_serializes_prompt_hash():
    rec = recovery.build_record(
        job={"job_id": "a", "fencing": 1, "runtime": "opencode", "workspace_id": "e2e",
             "mode": "workspace_write", "timeout_s": 60},
        phase="running", prompt="do X", session_id="sess-5", pid=123, proc_started_at=1.5,
    )
    blob = json.dumps(rec, ensure_ascii=False)
    assert "do X" in blob and "sess-5" in blob  # le journal porte de quoi reprendre
