import itertools
import threading

import pytest

import orch_protocol as P
from orch_mcp.store import BrokerError, Store


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t


INFO = {
    "version": "test",
    "max_parallel": 2,
    "runtimes": [{"id": "fake", "available": True}, {"id": "codex", "available": False}],
    "workspaces": [{"id": "demo", "modes": ["read_only", "workspace_write"]}, {"id": "ro", "modes": ["read_only"]}],
}


@pytest.fixture
def env(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "t.db", clock=clock)
    epoch = store.hello("pc", INFO, [])
    return store, clock, epoch


def start(store, **kw):
    args = dict(runner_id="pc", runtime="fake", workspace_id="demo", prompt="hello", mode="read_only")
    args.update(kw)
    return store.create_job(**args)


def run_to_running(store, epoch, job):
    store.transition("pc", epoch, job["job_id"], job["fencing"], "claimed", "starting")
    store.transition("pc", epoch, job["job_id"], job["fencing"], "starting", "running")


# ---------------------------------------------------------------- schéma
def test_transition_table_is_closed():
    for src, dst in itertools.product(P.ALL_STATES, P.ALL_STATES):
        allowed = P.transition_allowed(src, dst)
        if src in P.TERMINAL:
            assert not allowed
    for src, dst in P.RUNNER_TRANSITIONS:
        assert P.transition_allowed(src, dst)


@pytest.mark.parametrize(
    "kw,code",
    [
        ({"runtime": "powershell"}, "invalid_runtime"),
        ({"runtime": "codex"}, "runtime_unavailable"),
        ({"workspace_id": "..\\..\\Windows"}, "invalid_workspace"),
        ({"workspace_id": "C:/Users"}, "invalid_workspace"),
        ({"workspace_id": "\\\\host\\share"}, "invalid_workspace"),
        ({"workspace_id": "other"}, "workspace_denied"),
        ({"workspace_id": "ro", "mode": "workspace_write"}, "mode_denied"),
        ({"mode": "full"}, "invalid_mode"),
        ({"prompt": ""}, "invalid_prompt"),
        ({"prompt": "a\x00b"}, "invalid_prompt"),
        ({"prompt": "x" * (P.MAX_PROMPT_CHARS + 1)}, "invalid_prompt"),
        ({"runner_id": "ghost"}, "unknown_runner"),
        ({"runner_id": "Bad Id"}, "invalid_runner"),
        ({"timeout_s": 5}, "invalid_timeout"),
    ],
)
def test_create_refused_before_launch(env, kw, code):
    store, _, _ = env
    with pytest.raises(BrokerError) as e:
        start(store, **kw)
    assert e.value.code == code
    assert store.list_jobs() == []


def test_hostile_prompt_stored_verbatim(env):
    store, _, epoch = env
    hostile = '"; echo PWNED & whoami | powershell -c `$x` > NUL < %PATH% ^ \'q\'\n```bash\nrm -rf /\n```\n☃ é'
    job, _ = start(store, prompt=hostile)
    claimed = store.claim("pc", epoch, 1)
    assert claimed[0]["prompt"] == hostile


# ---------------------------------------------------------- idempotence
def test_idempotency_same_request_same_job(env):
    store, _, _ = env
    a, created_a = start(store, idempotency_key="key-12345")
    b, created_b = start(store, idempotency_key="key-12345")
    assert created_a and not created_b and a["job_id"] == b["job_id"]
    assert len(store.list_jobs()) == 1


def test_idempotency_conflict(env):
    store, _, _ = env
    start(store, idempotency_key="key-12345")
    with pytest.raises(BrokerError) as e:
        start(store, idempotency_key="key-12345", prompt="other")
    assert e.value.code == "idempotency_conflict"


# ------------------------------------------------------ claim / fencing
def test_happy_path_and_views(env):
    store, clock, epoch = env
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    assert c["fencing"] == 1
    run_to_running(store, epoch, c)
    store.event("pc", epoch, c["job_id"], 1, "evt-00000001", activity="working", output="line1\n")
    clock.t += 3
    store.transition("pc", epoch, c["job_id"], 1, "running", "completed", exit_code=0, result_summary="done")
    view = store.get_job(c["job_id"])
    assert view["state"] == "completed" and view["exit_code"] == 0 and view["result_summary"] == "done"
    assert view["duration_s"] == 3.0 and view["output_tail"] == "line1\n"


def test_completed_requires_exit_zero(env):
    store, _, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    run_to_running(store, epoch, c)
    with pytest.raises(BrokerError):
        store.transition("pc", epoch, c["job_id"], 1, "running", "completed", exit_code=1)


def test_invalid_transitions_refused(env):
    store, _, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    with pytest.raises(BrokerError) as e:
        store.transition("pc", epoch, c["job_id"], 1, "claimed", "completed", exit_code=0)
    assert e.value.code == "invalid_transition"
    with pytest.raises(BrokerError) as e:
        store.transition("pc", epoch, c["job_id"], 1, "starting", "running")
    assert e.value.code == "state_conflict"


def test_terminal_state_immutable_even_raw_sql(env):
    store, _, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    store.transition("pc", epoch, c["job_id"], 1, "claimed", "failed", error="x")
    import sqlite3

    with pytest.raises(sqlite3.DatabaseError):
        store._db.execute("UPDATE jobs SET state='completed' WHERE id=?", (c["job_id"],))


def test_retry_same_transition_is_idempotent(env):
    store, _, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    store.transition("pc", epoch, c["job_id"], 1, "claimed", "starting")
    again = store.transition("pc", epoch, c["job_id"], 1, "claimed", "starting")
    assert again["state"] == "starting"


def test_stale_fencing_rejected(env):
    store, clock, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    clock.t += P.LEASE_S + 1
    store.reap()  # claimed -> queued
    [c2] = store.claim("pc", epoch, 1)
    assert c2["fencing"] == 2
    with pytest.raises(BrokerError) as e:
        store.transition("pc", epoch, c["job_id"], 1, "claimed", "starting")
    assert e.value.code == "stale_fencing"


def test_concurrent_claims_single_winner(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "c.db", clock=clock)
    epoch = store.hello("pc", {**INFO, "max_parallel": 16}, [])
    for i in range(10):
        start(store, prompt=f"p{i}")
    got = []

    def worker():
        got.extend(store.claim("pc", epoch, 16))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    ids = [j["job_id"] for j in got]
    assert len(ids) == 10 and len(set(ids)) == 10


def test_concurrency_limit(env):
    store, _, epoch = env  # max_parallel = 2
    for i in range(5):
        start(store, prompt=f"p{i}")
    assert len(store.claim("pc", epoch, 10)) == 2
    assert store.claim("pc", epoch, 10) == []
    assert len(store.list_jobs(state="queued")) == 3


# ---------------------------------------------------- présence / epoch
def test_online_offline_by_heartbeat(env):
    store, clock, epoch = env
    assert store.runners()[0]["status"] == "online"
    clock.t += P.ONLINE_WINDOW_S + 1
    assert store.runners()[0]["status"] == "offline"
    store.heartbeat("pc", epoch, [])
    assert store.runners()[0]["status"] == "online"


def test_double_connection_supersedes_old_session(env):
    store, _, epoch = env
    new_epoch = store.hello("pc", INFO, [])
    with pytest.raises(BrokerError) as e:
        store.heartbeat("pc", epoch, [])
    assert e.value.code == "superseded"
    store.heartbeat("pc", new_epoch, [])


def test_runner_restart_reconciles_without_relaunch(env):
    store, clock, epoch = env
    start(store, prompt="a")
    start(store, prompt="b")
    c1, c2 = store.claim("pc", epoch, 2)
    run_to_running(store, epoch, c1)  # a running, b claimed
    store.hello("pc", INFO, [])  # restart, rien détenu : a parqué (pas lost), b requeued
    assert store.get_job(c1["job_id"])["state"] == "running"
    assert store.get_job(c1["job_id"])["recovery_state"] == "suspended"
    assert store.get_job(c2["job_id"])["state"] == "queued"
    # Sans rattachement, le parking ne se prolonge pas : grâce expirée -> lost.
    clock.t += P.RECOVERY_GRACE_S + 1
    assert store.reap()["lost"] == 1
    assert store.get_job(c1["job_id"])["state"] == "lost"


def test_runner_restart_with_recovering_journal_reattaches(env):
    store, _, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    run_to_running(store, epoch, c)
    e2 = store.hello("pc", INFO, [{"job_id": c["job_id"], "fencing": 1, "recovering": True}])
    view = store.get_job(c["job_id"])
    assert view["state"] == "running" and view["recovery_state"] == "recovering"
    assert view["resume_count"] == 1
    store.transition("pc", e2, c["job_id"], 1, "running", "completed", exit_code=0)
    done = store.get_job(c["job_id"])
    assert done["state"] == "completed" and done["recovery_state"] is None


def test_reconnect_keeps_held_job(env):
    store, _, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    run_to_running(store, epoch, c)
    e2 = store.hello("pc", INFO, [{"job_id": c["job_id"], "fencing": 1}])
    store.transition("pc", e2, c["job_id"], 1, "running", "completed", exit_code=0)
    assert store.get_job(c["job_id"])["state"] == "completed"


def test_lease_expiry_parks_then_lost_after_grace(env):
    store, clock, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    run_to_running(store, epoch, c)
    clock.t += P.LEASE_S + 1
    stats = store.reap()
    assert stats["suspended"] == 1 and stats["lost"] == 0
    parked = store.get_job(c["job_id"])
    assert parked["state"] == "running" and parked["recovery_state"] == "suspended"
    # Pendant la grâce, la reprise reste possible (même fencing, epoch rattaché).
    e2 = store.hello("pc", INFO, [{"job_id": c["job_id"], "fencing": 1, "recovering": True}])
    store.transition("pc", e2, c["job_id"], 1, "running", "completed", exit_code=0)
    assert store.get_job(c["job_id"])["state"] == "completed"


def test_lease_expiry_grace_expired_is_lost_never_completed(env):
    store, clock, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    run_to_running(store, epoch, c)
    clock.t += P.LEASE_S + 1
    assert store.reap()["suspended"] == 1
    clock.t += P.RECOVERY_GRACE_S + 1
    assert store.reap()["lost"] == 1
    with pytest.raises(BrokerError):
        store.transition("pc", epoch, c["job_id"], 1, "running", "completed", exit_code=0)
    assert store.get_job(c["job_id"])["state"] == "lost"


def test_heartbeat_extends_lease_and_flags_abandon(env):
    store, clock, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    run_to_running(store, epoch, c)
    for _ in range(5):
        clock.t += P.LEASE_S - 5
        hb = store.heartbeat("pc", epoch, [{"job_id": c["job_id"], "fencing": 1}, {"job_id": "zzz", "fencing": 1}])
        store.reap()
    assert store.get_job(c["job_id"])["state"] == "running"
    assert hb["abandon"] == ["zzz"]


# ------------------------------------------------------------ annulation
def test_cancel_semantics(env):
    store, _, epoch = env
    q, _ = start(store, prompt="queued")
    assert store.cancel(q["job_id"])["result"] == "cancelled"
    assert store.cancel(q["job_id"])["result"] == "already_finished"
    assert store.cancel("nope")["result"] == "unknown_job"
    start(store, prompt="run")
    [c] = store.claim("pc", epoch, 1)
    store.transition("pc", epoch, c["job_id"], 1, "claimed", "starting")
    assert store.cancel(c["job_id"])["result"] == "cancel_requested"
    hb = store.heartbeat("pc", epoch, [{"job_id": c["job_id"], "fencing": 1}])
    assert hb["cancel"] == [c["job_id"]]
    store.transition("pc", epoch, c["job_id"], 1, "starting", "cancelled")
    assert store.get_job(c["job_id"])["state"] == "cancelled"


def test_cancel_while_claimed_and_lease_expires(env):
    store, clock, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    store.cancel(c["job_id"])
    clock.t += P.LEASE_S + 1
    store.reap()
    assert store.get_job(c["job_id"])["state"] == "cancelled"


def test_hard_timeout_requests_cancel(env):
    store, clock, epoch = env
    start(store, timeout_s=60)
    [c] = store.claim("pc", epoch, 1)
    run_to_running(store, epoch, c)
    clock.t += 200
    store.heartbeat("pc", epoch, [{"job_id": c["job_id"], "fencing": 1}])
    assert store.reap()["timeout_cancel"] == 1


# ------------------------------------------------------ sortie / events
def test_event_dedup_and_output_bounds(env):
    store, _, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    run_to_running(store, epoch, c)
    store.event("pc", epoch, c["job_id"], 1, "evt-dup-0001", output="A")
    assert store.event("pc", epoch, c["job_id"], 1, "evt-dup-0001", output="A")["duplicate"]
    big = "x" * P.MAX_CHUNK_CHARS
    for i in range(200):
        store.event("pc", epoch, c["job_id"], 1, f"evt-big-{i:04d}", output=big)
    view = store.get_job(c["job_id"], tail_chars=100000)
    assert view["output_chars"] <= P.MAX_OUTPUT_CHARS_PER_JOB
    assert view["output_truncated"] and len(view["output_tail"]) <= 8000
    page = store.read_output(c["job_id"], 0, 999999)
    assert len(page["text"]) <= P.MAX_OUTPUT_PAGE and page["next_cursor"] is not None


def test_secrets_redacted(env):
    store, _, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    run_to_running(store, epoch, c)
    secret = "sk-ant-" + "a" * 40
    store.event("pc", epoch, c["job_id"], 1, "evt-sec-0001", output=f"key {secret}\nAuthorization: Bearer abc", activity=secret)
    store.transition("pc", epoch, c["job_id"], 1, "running", "failed", exit_code=1, error=f"GITHUB_TOKEN={'g'*30}")
    view = store.get_job(c["job_id"])
    blob = str(view)
    assert secret not in blob and "g" * 30 not in blob and "Bearer abc" not in blob


def test_purge_retention(env):
    store, clock, epoch = env
    start(store)
    [c] = store.claim("pc", epoch, 1)
    run_to_running(store, epoch, c)
    store.event("pc", epoch, c["job_id"], 1, "evt-p-000001", output="out")
    store.transition("pc", epoch, c["job_id"], 1, "running", "completed", exit_code=0)
    clock.t += 8 * 86400
    store.purge()
    assert store._db.execute("SELECT prompt FROM jobs").fetchone()[0] is None
    assert store.get_job(c["job_id"])["state"] == "completed"
    clock.t += 90 * 86400
    store.purge()
    assert store.get_job(c["job_id"]) is None


def test_persistence_across_restart(tmp_path):
    clock = Clock()
    s1 = Store(tmp_path / "p.db", clock=clock)
    epoch = s1.hello("pc", INFO, [])
    job, _ = s1.create_job("pc", "fake", "demo", "x", "read_only")
    s1.close()
    s2 = Store(tmp_path / "p.db", clock=clock)
    assert s2.get_job(job["job_id"])["state"] == "queued"
    assert s2.claim("pc", epoch, 1)[0]["job_id"] == job["job_id"]
