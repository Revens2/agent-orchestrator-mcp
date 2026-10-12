"""E2E Windows réels (issue #6) : pause-quota de mission, réveil, annulation,
redémarrage broker. Runner + broker HTTP réels, runtime `fake`."""

import os
import time

import pytest

if os.name != "nt":
    pytest.skip("Windows uniquement", allow_module_level=True)

import orch_protocol as P
from orch_mcp import store as store_mod
from orch_mcp.store import Store
from tests import test_runner_windows as _w
from tests.test_runner_windows import Broker, free_port, make_runner, pid_alive, start_runner, wait_state

stack = _w.stack  # fixture E2E réutilisée (broker uvicorn + runner réel)

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows uniquement")

QUOTA_PROMPT = "session ses_abc\nprovider-error insufficient_quota|1\n"
PAUSE_EVENTS = {P.EV_PROVIDER_QUOTA_DETECTED, P.EV_MISSION_PAUSED, P.EV_QUOTA_RETRY_SCHEDULED}


@pytest.fixture(autouse=True)
def fast_claim_poll(monkeypatch):
    # Le runtime fake annonce resume_session (comme opencode) : la reprise
    # passe ORCH_FAKE_RESUME_SESSION -> RESUMED_SESSION:<sid>.
    # Long-poll claim court : arrêt du broker (join du long-poll en vol) rapide.
    monkeypatch.setattr(P, "CLAIM_POLL_S", 2)


def new_mission(store: Store, prompt: str) -> dict:
    return store.create_mission(
        "Mission pause-quota E2E", ["le job termine exit 0"], max_attempts=2,
        runner_id="pc", runtime="fake", workspace_id="e2e", mode="workspace_write", prompt=prompt,
    )


def wait_mission(store: Store, mission_id: str, states, timeout=60) -> dict:
    end = time.time() + timeout
    while time.time() < end:
        m = store.get_mission(mission_id)
        if m["state"] in states:
            return m
        time.sleep(0.2)
    raise AssertionError(f"timeout, mission = {store.get_mission(mission_id)['state']}")


def force_due(store: Store, mission_id: str) -> None:
    """resume_after dans le passé : le prochain tick du reaper réveille la mission."""
    with store._tx() as db:
        db.execute("UPDATE missions SET resume_after=? WHERE id=?", (store.clock() - 1, mission_id))


def event_kinds(store: Store, job_id: str) -> list[str]:
    with store._lock:
        return [r[0] for r in store._db.execute("SELECT kind FROM job_events WHERE job_id=? ORDER BY seq", (job_id,))]


def job_count(store: Store) -> int:
    with store._lock:
        return store._db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]


def pause_mission(store: Store) -> tuple[str, str]:
    m = new_mission(store, QUOTA_PROMPT)
    first = m["current_job_id"]
    job = wait_state(store, first, P.TERMINAL)
    assert job["state"] == "failed"
    assert job["error"].startswith("provider_unavailable:quota_exhausted"), job["error"]
    m = wait_mission(store, m["mission_id"], {P.MISSION_WAITING_FOR_QUOTA}, timeout=10)
    assert m["pause"]["reason"] == P.PROVIDER_QUOTA and m["pause"]["resume_after"]
    return m["mission_id"], first


# --------------------------------------------------------------------- tests
def test_quota_pause_then_scheduled_resume_same_session(stack):
    broker, *_ = stack
    store = broker.store
    mission_id, first = pause_mission(store)

    w = store.wait_for_mission(mission_id, -1, timeout_s=20)
    assert w["must_follow"] is False and w["woke_by"] == "paused" and w["until"] == "state_change"
    assert w["mission_state"] == P.MISSION_WAITING_FOR_QUOTA and w["terminal"] is False

    # Pause plancher 60 s : on avance l'échéance au lieu d'attendre.
    t0 = time.time()
    force_due(store, mission_id)
    m = wait_mission(store, mission_id, {P.MISSION_NEEDS_VALIDATION, P.MISSION_INCOMPLETE}, timeout=60)
    assert m["state"] == P.MISSION_NEEDS_VALIDATION, m
    assert time.time() - t0 < 30
    assert len(m["attempt_job_ids"]) == 2 and m["attempt_job_ids"][0] == first
    assert m["resumes"] == 1 and m["pause"] is None
    second = m["attempt_job_ids"][1]
    job2 = store.get_job(second, tail_chars=8000)
    assert job2["state"] == "completed"
    assert "RESUMED_SESSION:ses_abc" in job2["output_tail"]
    # première tentative : terminale et immuable
    assert store.get_job(first)["state"] == "failed"
    kinds = event_kinds(store, first) + event_kinds(store, second)
    assert PAUSE_EVENTS | {P.EV_MISSION_RESUMED} <= set(kinds), kinds


def test_provider_block_kills_early(stack):
    broker, *_ = stack
    store = broker.store
    m = new_mission(store, "spawn 300\nprovider-block You exceeded your current quota\n")
    job_id = m["current_job_id"]
    t0 = time.time()
    child = None
    end = time.time() + 20
    while time.time() < end and child is None:
        tail = store.get_job(job_id, 8000).get("output_tail") or ""
        if "CHILD_PID:" in tail:
            child = int(tail.split("CHILD_PID:")[1].split()[0])
        time.sleep(0.2)
    assert child
    job = wait_state(store, job_id, P.TERMINAL, timeout=30)
    assert time.time() - t0 < 30
    assert job["state"] == "failed"
    assert job["error"].startswith("provider_unavailable:quota_exhausted"), job["error"]
    m = wait_mission(store, m["mission_id"], {P.MISSION_WAITING_FOR_QUOTA}, timeout=10)
    assert PAUSE_EVENTS <= set(event_kinds(store, job_id))
    time.sleep(0.5)
    assert not pid_alive(child)  # aucun orphelin (arbre tué via Job Object)


def test_cancel_during_pause_never_resumes(stack):
    broker, *_ = stack
    store = broker.store
    mission_id, first = pause_mission(store)
    out = store.cancel_mission(mission_id)
    assert out["state"] == P.MISSION_CANCELLED and out["result"] == "cancelled"
    assert P.EV_PAUSE_CANCELLED in event_kinds(store, first)
    force_due(store, mission_id)
    time.sleep(2.5)  # ~5 ticks du reaper
    assert store.wake_paused_missions() == 0
    m = store.get_mission(mission_id)
    assert m["state"] == P.MISSION_CANCELLED and m["attempt_job_ids"] == [first] and m["resumes"] == 0
    assert job_count(store) == 1
    with pytest.raises(store_mod.BrokerError):
        store.resume_mission(mission_id)
    assert store.cancel_mission(mission_id)["result"] == "already_finished"


def test_broker_restart_during_pause_resumes_once(tmp_path):
    db = tmp_path / "orch.db"
    port = free_port()
    broker = Broker(db, port)
    runner = make_runner(tmp_path, port)
    start_runner(runner)
    try:
        end = time.time() + 20
        while not broker.store.runners() or broker.store.runners()[0]["status"] != "online":
            assert time.time() < end, "runner jamais online"
            time.sleep(0.2)
        mission_id, first = pause_mission(broker.store)
        broker.stop()

        # Broker absent : échéance avancée directement en base.
        offline = Store(db)
        force_due(offline, mission_id)
        offline.close()

        broker = Broker(db, port)
        m = wait_mission(broker.store, mission_id, {P.MISSION_NEEDS_VALIDATION, P.MISSION_INCOMPLETE}, timeout=60)
        assert m["state"] == P.MISSION_NEEDS_VALIDATION, m
        time.sleep(2)  # plusieurs ticks : aucune reprise en double
        m = broker.store.get_mission(mission_id)
        assert m["resumes"] == 1 and len(m["attempt_job_ids"]) == 2 and m["attempt_job_ids"][0] == first
        assert job_count(broker.store) == 2
        assert broker.store.get_job(first)["state"] == "failed"
        assert "RESUMED_SESSION:ses_abc" in broker.store.get_job(m["attempt_job_ids"][1], 8000)["output_tail"]
    finally:
        runner.stop()
        broker.stop()
