"""Supervision riche : santé structurée, journal d'événements, missions, stalls, wait.

Contrat central testé ici : `completed` = processus exit 0, PAS mission validée.
"""

import threading

import pytest

import orch_protocol as P
from orch_mcp.store import BrokerError, Store
from orch_runner.runner import _git_snapshot


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t


INFO = {
    "version": "test",
    "max_parallel": 2,
    "runtimes": [{"id": "fake", "available": True}],
    "workspaces": [{"id": "demo", "modes": ["read_only", "workspace_write"]}],
}


@pytest.fixture
def env(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "s.db", clock=clock)
    epoch = store.hello("pc", INFO, [])
    return store, clock, epoch


def start(store, **kw):
    args = dict(runner_id="pc", runtime="fake", workspace_id="demo", prompt="hello", mode="read_only")
    args.update(kw)
    return store.create_job(**args)


def to_running(store, epoch, claimed):
    store.transition("pc", epoch, claimed["job_id"], claimed["fencing"], "claimed", "starting")
    store.transition("pc", epoch, claimed["job_id"], claimed["fencing"], "starting", "running")


def tele(store, epoch, job_id, fencing, **kw):
    base = {"job_id": job_id, "fencing": fencing, "pid": 1234,
            "proc_alive": True, "proc_started_at": 1_000_000.0, "child_procs": 2}
    base.update(kw)
    return store.heartbeat("pc", epoch, [base])


# ------------------------------------------------------- job_get enrichi
def test_enriched_get_null_when_unobservable(env):
    store, _, epoch = env
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    view = store.get_job(job["job_id"])
    assert view["state"] == "claimed"
    assert view["process_alive"] is None and view["pid"] is None
    assert view["process_started_at"] is None and view["child_process_count"] is None
    assert view["last_output_at"] is None and view["last_event_at"] is None
    assert view["current_tool"] is None and view["current_command_sanitized"] is None
    assert view["runner_heartbeat_age_s"] == 0.0
    assert view["execution_health"] == "idle"
    assert view["broker_health"] == {"lease_expires_at": view["broker_health"]["lease_expires_at"], "lease_valid": True}
    assert view["runner_health"]["status"] == "online"
    assert view["runtime_process_health"] == {"alive": None, "pid": None, "started_at": None, "child_process_count": None}
    # compat : anciennes clés intactes
    for k in ("job_id", "state", "exit_code", "last_activity", "result_summary", "error",
              "output_chars", "output_truncated", "attempt", "duration_s"):
        assert k in view


def test_telemetry_flows_to_get_and_health(env):
    store, clock, epoch = env
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    tele(store, epoch, c["job_id"], c["fencing"], tool="tool: Bash")
    store.event("pc", epoch, c["job_id"], c["fencing"], "evt-t-00001", activity="tool: Bash", output="out\n")
    view = store.get_job(c["job_id"])
    assert view["process_alive"] is True and view["pid"] == 1234
    assert view["child_process_count"] == 2 and view["current_tool"] == "tool: Bash"
    assert view["current_activity"] == "tool: Bash"
    assert view["last_output_at"] is not None and view["last_event_at"] is not None
    assert view["execution_health"] == "healthy"
    clock.t += 1  # un ping de présence seul ne change pas updated_at
    before = store.get_job(c["job_id"])
    store.heartbeat("pc", epoch, [{"job_id": c["job_id"], "fencing": c["fencing"]}])
    after = store.get_job(c["job_id"])
    assert before["last_event_at"] == after["last_event_at"]


def test_health_layers(env):
    store, clock, epoch = env
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    tele(store, epoch, c["job_id"], c["fencing"])
    # runner muet -> runner_disconnected, cause lisible malgré bail encore valide
    clock.t += P.ONLINE_WINDOW_S + 1
    view = store.get_job(c["job_id"])
    assert view["execution_health"] == "runner_disconnected"
    assert view["runner_health"]["status"] == "offline"
    assert view["broker_health"]["lease_valid"] is True  # le bail n'est pas la cause ici
    # processus mort -> process_dead (runner revenu)
    store.heartbeat("pc", epoch, [{"job_id": c["job_id"], "fencing": c["fencing"]}])
    tele(store, epoch, c["job_id"], c["fencing"], proc_alive=False)
    assert store.get_job(c["job_id"])["execution_health"] == "process_dead"


def test_lost_keeps_unknown_issue_with_layers(env):
    store, clock, epoch = env
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    clock.t += P.LEASE_S + 1
    assert store.reap()["lost"] == 1
    view = store.get_job(c["job_id"])
    assert view["state"] == "lost"
    assert "inconnue" in (view["error"] or "")
    assert view["execution_health"] is None  # terminal : lire state, pas la santé
    assert view["runner_health"]["status"] == "offline"
    assert view["broker_health"]["lease_valid"] is False
    evts = store.read_events(c["job_id"])
    kinds = [e["kind"] for e in evts["events"]]
    assert P.EV_LEASE_EXPIRED in kinds and P.EV_PROCESS_EXIT in kinds


# ------------------------------------------------------------- journal
def test_event_kinds_and_pagination(env):
    store, _, epoch = env
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    store.event("pc", epoch, c["job_id"], c["fencing"], "evt-e-00001", activity="working", output="x")
    store.event("pc", epoch, c["job_id"], c["fencing"], "evt-e-00002", activity="working", output="y")
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "completed", exit_code=0)
    page = store.read_events(c["job_id"])
    kinds = [e["kind"] for e in page["events"]]
    assert kinds == [P.EV_JOB_CLAIMED, P.EV_RUNTIME_SPAWNED, P.EV_PROCESS_RUNNING,
                     P.EV_ACTIVITY, P.EV_PROCESS_EXIT]
    assert page["last_seq"] == 4
    seqs = [e["seq"] for e in page["events"]]
    assert seqs == sorted(seqs)
    p2 = store.read_events(c["job_id"], after_seq=1, limit=2)
    assert [e["seq"] for e in p2["events"]] == [2, 3]
    assert p2["next_seq"] == 4 and p2["last_seq"] == 4
    assert store.read_events("no-such-job") is None
    store.cancel(c["job_id"])  # terminal : already_finished, pas d'événement
    assert store.read_events("no-such-job", limit=999) is None


def test_event_journal_bounded(env):
    store, _, epoch = env
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    for i in range(P.MAX_EVENTS_PER_JOB + 20):
        store.event("pc", epoch, c["job_id"], c["fencing"], f"evt-b-{i:06d}", activity=f"step {i}")
    total = P.MAX_EVENTS_PER_JOB + 23  # 3 transitions + 520 activités
    p1 = store.read_events(c["job_id"], limit=200)
    assert len(p1["events"]) == 200  # pages bornées à 200
    assert p1["events"][0]["seq"] == total - P.MAX_EVENTS_PER_JOB  # vieux purgés
    p2 = store.read_events(c["job_id"], after_seq=p1["events"][-1]["seq"], limit=200)
    p3 = store.read_events(c["job_id"], after_seq=p2["events"][-1]["seq"], limit=200)
    got = p1["events"] + p2["events"] + p3["events"]
    assert len(got) == P.MAX_EVENTS_PER_JOB
    assert [e["seq"] for e in got] == list(range(total - P.MAX_EVENTS_PER_JOB, total))
    assert p3["last_seq"] == total - 1


def test_output_progress_event_on_threshold(env):
    store, _, epoch = env
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    store.event("pc", epoch, c["job_id"], c["fencing"], "evt-o-00001", output="small")
    assert P.EV_OUTPUT_PROGRESS not in [e["kind"] for e in store.read_events(c["job_id"])["events"]]
    for i in range(6):  # chunks bornés à 16 K : le seuil 64 K se franchit en cumulé
        store.event("pc", epoch, c["job_id"], c["fencing"], f"evt-o-000{i + 2}", output="z" * P.MAX_CHUNK_CHARS)
    assert P.EV_OUTPUT_PROGRESS in [e["kind"] for e in store.read_events(c["job_id"])["events"]]


def test_hello_reconcile_emits_runner_disconnect(env):
    store, _, epoch = env
    start(store, prompt="a")
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    store.hello("pc", INFO, [])  # restart : job perdu
    kinds = [e["kind"] for e in store.read_events(c["job_id"])["events"]]
    assert P.EV_RUNNER_DISCONNECT in kinds


def test_event_detail_redacted(env):
    store, _, epoch = env
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    secret = "sk-ant-" + "b" * 40
    store.event("pc", epoch, c["job_id"], c["fencing"], "evt-r-00001", activity=f"token {secret}")
    blob = str(store.read_events(c["job_id"]))
    assert secret not in blob


# --------------------------------------------------------------- stalls
def test_stall_detection_notify_only(env):
    store, clock, epoch = env
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    tele(store, epoch, c["job_id"], c["fencing"])
    # bails prolongés artificiellement : le runner est vivant, le process aussi, rien ne bouge
    for _ in range(30):
        clock.t += P.LEASE_S - 5
        store.heartbeat("pc", epoch, [{"job_id": c["job_id"], "fencing": c["fencing"],
                                       "pid": 7, "proc_alive": True, "child_procs": 1}])
        store.reap()
    assert store.get_job(c["job_id"])["state"] == "running"  # jamais annulé seul
    view = store.get_job(c["job_id"])
    assert view["execution_health"] == "suspected_stall"
    kinds = [e["kind"] for e in store.read_events(c["job_id"])["events"]]
    assert P.EV_SUSPECTED_STALL in kinds and kinds.count(P.EV_SUSPECTED_STALL) == 1  # edge-triggered
    for _ in range(60):
        clock.t += P.LEASE_S - 5
        store.heartbeat("pc", epoch, [{"job_id": c["job_id"], "fencing": c["fencing"],
                                       "pid": 7, "proc_alive": True, "child_procs": 1}])
        store.reap()
    assert store.get_job(c["job_id"])["execution_health"] == "stalled"
    assert store.get_job(c["job_id"])["state"] == "running"  # toujours pas d'action auto
    # une activité réarme la détection
    store.event("pc", epoch, c["job_id"], c["fencing"], "evt-s-00001", activity="back", output="o")
    assert store.get_job(c["job_id"])["execution_health"] == "healthy"


def test_no_stall_without_alive_telemetry(env):
    store, clock, epoch = env
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    for _ in range(60):  # vieux runner sans télémétrie : aucun faux signal
        clock.t += P.LEASE_S - 5
        store.heartbeat("pc", epoch, [{"job_id": c["job_id"], "fencing": c["fencing"]}])
        store.reap()
    assert store.get_job(c["job_id"])["state"] == "running"
    kinds = [e["kind"] for e in store.read_events(c["job_id"])["events"]]
    assert P.EV_SUSPECTED_STALL not in kinds and P.EV_STALLED not in kinds


# ------------------------------------------------------------- missions
def test_mission_completed_is_not_validated(env):
    store, _, epoch = env
    m = store.create_mission("faire X", ["X fait"], 2, "pc", "fake", "demo", "read_only")
    assert m["state"] == "executing" and m["attempts"] == 1
    assert m["current_job"] and m["current_job"]["state"] == "queued"
    [c] = store.claim("pc", epoch, 1)
    assert c["job_id"] == m["current_job_id"]
    to_running(store, epoch, c)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "completed", exit_code=0)
    m2 = store.get_mission(m["mission_id"])
    assert m2["state"] == "needs_validation"  # exit 0 -> PAS validated
    assert m2["validation_state"] is None
    validated = store.validate_mission(m["mission_id"], "validated", note="vu et ok")
    assert validated["state"] == "validated" and validated["validation_state"] == "validated"


def test_mission_failed_job_needs_explicit_retry(env):
    store, _, epoch = env
    m = store.create_mission("faire Y", ["Y fait"], 2, "pc", "fake", "demo", "read_only")
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "failed", exit_code=1)
    assert store.get_mission(m["mission_id"])["state"] == "incomplete"
    # aucun retry automatique : le reaper ne touche pas aux missions
    store.reap()
    assert store.get_mission(m["mission_id"])["state"] == "incomplete"
    # retry explicite seulement : nouvelle tentative de la même mission
    r = store.retry_mission(m["mission_id"])
    assert r["attempts"] == 2 and r["state"] == "executing"
    assert r["current_job_id"] != c["job_id"]


def test_mission_retry_and_limits(env):
    store, _, epoch = env
    m = store.create_mission("faire Z", ["Z fait"], 1, "pc", "fake", "demo", "read_only")
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "failed", exit_code=1)
    with pytest.raises(BrokerError) as e:
        store.retry_mission(m["mission_id"])
    assert e.value.code == "max_attempts_reached"
    m2 = store.create_mission("faire W", ["W fait"], 2, "pc", "fake", "demo", "read_only")
    [c2] = store.claim("pc", epoch, 1)
    with pytest.raises(BrokerError) as e2:
        store.retry_mission(m2["mission_id"])  # mission encore executing : retry refusé
    assert e2.value.code == "mission_not_retryable"
    to_running(store, epoch, c2)
    store.transition("pc", epoch, c2["job_id"], c2["fencing"], "running", "failed", exit_code=1)
    r = store.retry_mission(m2["mission_id"])
    assert r["attempts"] == 2 and r["state"] == "executing"
    assert r["current_job_id"] != c2["job_id"] and len(r["attempt_job_ids"]) == 2
    [c3] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c3)
    store.transition("pc", epoch, c3["job_id"], c3["fencing"], "running", "failed", exit_code=1)
    assert store.get_mission(m2["mission_id"])["state"] == "incomplete"
    store.validate_mission(m2["mission_id"], "blocked", note="bloqué : attendre humain")
    assert store.get_mission(m2["mission_id"])["state"] == "blocked"


def test_mission_lost_becomes_incomplete_never_retried(env):
    store, clock, epoch = env
    m = store.create_mission("faire L", ["L fait"], 3, "pc", "fake", "demo", "read_only")
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    clock.t += P.LEASE_S + 1
    assert store.reap()["lost"] == 1
    got = store.get_mission(m["mission_id"])
    assert got["state"] == "incomplete"  # cause inconnue : décision humaine requise
    assert got["current_job"]["state"] == "lost"


def test_mission_validation_rules(env):
    store, _, epoch = env
    m = store.create_mission("faire V", ["V fait"], 2, "pc", "fake", "demo", "read_only")
    with pytest.raises(BrokerError) as e:
        store.validate_mission(m["mission_id"], "validated")  # job encore actif
    assert e.value.code == "mission_not_validatable"
    with pytest.raises(BrokerError):
        store.validate_mission(m["mission_id"], "maybe")
    with pytest.raises(BrokerError) as e2:
        store.validate_mission("no-such-mission", "validated")
    assert e2.value.code == "unknown_mission"
    assert store.get_mission("no-such-mission") is None


# ------------------------------------------------------------------ wait
def test_wait_timeout_and_wakeup(env):
    store, _, epoch = env
    job, _ = start(store)
    res = store.wait_for_change(job["job_id"], timeout_s=0.5)
    assert res["woke_by"] == "timeout" and res["state"] == "queued"
    [c] = store.claim("pc", epoch, 1)
    holder = {}

    def waiter():
        holder["res"] = store.wait_for_change(c["job_id"], timeout_s=5)

    t = threading.Thread(target=waiter)
    t.start()
    import time as _t
    _t.sleep(0.6)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "claimed", "starting")
    t.join(timeout=10)
    assert not t.is_alive()
    assert holder["res"]["woke_by"] in ("state", "change")
    assert holder["res"]["state"] == "starting"
    assert store.wait_for_change("no-such-job") is None


# -------------------------------------------------------------- compat
def test_migrate_v1_db_and_old_insert_still_work(tmp_path):
    import sqlite3

    clock = Clock()
    db = sqlite3.connect(tmp_path / "old.db")
    db.executescript("""
        CREATE TABLE runners (id TEXT PRIMARY KEY, epoch INTEGER NOT NULL DEFAULT 0,
          last_seen REAL, hello_at REAL, version TEXT, max_parallel INTEGER NOT NULL DEFAULT 1,
          runtimes_json TEXT NOT NULL DEFAULT '[]', workspaces_json TEXT NOT NULL DEFAULT '[]');
        CREATE TABLE jobs (id TEXT PRIMARY KEY, runner_id TEXT NOT NULL, runtime TEXT NOT NULL,
          workspace_id TEXT NOT NULL, mode TEXT NOT NULL, prompt TEXT, prompt_chars INTEGER NOT NULL,
          idem_key TEXT UNIQUE, idem_hash TEXT, state TEXT NOT NULL, fencing INTEGER NOT NULL DEFAULT 0,
          attempt INTEGER NOT NULL DEFAULT 0, epoch INTEGER, lease_expires REAL,
          cancel_requested INTEGER NOT NULL DEFAULT 0, timeout_s INTEGER NOT NULL,
          created_at REAL NOT NULL, claimed_at REAL, started_at REAL, finished_at REAL,
          updated_at REAL NOT NULL, exit_code INTEGER, last_activity TEXT, result_summary TEXT,
          error TEXT, runtime_session_id TEXT, output_chars INTEGER NOT NULL DEFAULT 0,
          output_chunks INTEGER NOT NULL DEFAULT 0, output_truncated INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE events (job_id TEXT NOT NULL, event_id TEXT NOT NULL, received_at REAL NOT NULL,
          PRIMARY KEY (job_id, event_id));
        CREATE TABLE output (job_id TEXT NOT NULL, seq INTEGER NOT NULL, text TEXT NOT NULL,
          PRIMARY KEY (job_id, seq));
        CREATE TABLE transitions (job_id TEXT NOT NULL, at REAL NOT NULL, src TEXT, dst TEXT NOT NULL, actor TEXT NOT NULL);
    """)
    db.execute("INSERT INTO runners(id, epoch, last_seen) VALUES ('pc', 1, ?)", (clock.t,))
    db.execute(
        """INSERT INTO jobs(id, runner_id, runtime, workspace_id, mode, prompt, prompt_chars,
             state, timeout_s, created_at, updated_at)
           VALUES ('old-job-1','pc','fake','demo','read_only','hi',2,'queued',3600,?,?)""",
        (clock.t, clock.t),
    )
    db.commit()
    db.close()
    store = Store(tmp_path / "old.db", clock=clock)
    view = store.get_job("old-job-1")  # migration : nouveaux champs NULL
    assert view["state"] == "queued" and view["process_alive"] is None
    assert view["execution_health"] == "idle"
    # vieil INSERT explicite (sans nouvelles colonnes) toujours valide
    store._db.execute(
        """INSERT INTO jobs(id, runner_id, runtime, workspace_id, mode, prompt, prompt_chars,
             state, timeout_s, created_at, updated_at)
           VALUES ('old-job-2','pc','fake','demo','read_only','ho',2,'queued',3600,?,?)""",
        (clock.t, clock.t),
    )
    assert store.get_job("old-job-2")["state"] == "queued"


def test_git_snapshot_none_outside_repo(tmp_path):
    assert _git_snapshot(str(tmp_path)) is None
