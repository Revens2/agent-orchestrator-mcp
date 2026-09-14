"""Recovery bornée, liveness fiable, transitions déterministes (broker).

- Un heartbeat continu sans preuve positive ne prolonge pas indéfiniment :
  première observation négative -> `lost` après PROCESS_RECOVERY_S.
- Seule une preuve positive fraîche (proc_alive=1 + superviseur vivant)
  referme la fenêtre de recovery.
- Télémétrie absente = non observé : ne referme ni n'annule une recovery.
- Transitions starting/running -> lost autorisées et terminales.
- `runner_job_state` : lecture fencée sans fuite de prompt/sortie.
"""
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
    "runtimes": [{"id": "fake", "available": True}],
    "workspaces": [{"id": "demo", "modes": ["read_only", "workspace_write"]}],
}


@pytest.fixture
def env(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "t.db", clock=clock)
    epoch = store.hello("pc", INFO, [])
    return store, clock, epoch


def _running(store, epoch):
    job, _ = store.create_job("pc", "fake", "demo", "hello", "read_only")
    [c] = store.claim("pc", epoch, 1)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "claimed", "starting")
    store.transition("pc", epoch, c["job_id"], c["fencing"], "starting", "running")
    return c


def test_recovery_is_bounded_despite_heartbeat(env):
    store, clock, epoch = env
    c = _running(store, epoch)
    jid = c["job_id"]
    # Première observation négative : processus mort.
    store.heartbeat("pc", epoch, [{"job_id": jid, "fencing": 1, "proc_alive": False}])
    assert store.get_job(jid)["state"] == "running"
    # Heartbeats aveugles (sans télémétrie) n'annulent pas la recovery.
    for _ in range(5):
        clock.t += 10
        store.heartbeat("pc", epoch, [{"job_id": jid, "fencing": 1}])
        store.reap()
        assert store.get_job(jid)["state"] == "running"
    # Fenêtre expirée -> lost déterministe, jamais relancé en silence.
    clock.t += P.PROCESS_RECOVERY_S
    assert store.reap()["lost"] == 1
    assert store.get_job(jid)["state"] == "lost"
    with pytest.raises(BrokerError):
        store.transition("pc", epoch, jid, 1, "running", "completed", exit_code=0)


def test_positive_evidence_closes_recovery(env):
    store, clock, epoch = env
    c = _running(store, epoch)
    jid = c["job_id"]
    store.heartbeat("pc", epoch, [{"job_id": jid, "fencing": 1, "proc_alive": False}])
    clock.t += 10
    # Preuve positive fraîche : processus vivant + superviseur vivant.
    store.heartbeat("pc", epoch, [{"job_id": jid, "fencing": 1, "proc_alive": True,
                                   "supervisor_alive": True}])
    row = store._db.execute("SELECT recovery_since FROM jobs WHERE id=?", (jid,)).fetchone()
    assert row["recovery_since"] is None
    # Au-delà de l'ancienne fenêtre, avec lease entretenu : toujours running.
    for _ in range(7):
        clock.t += 10
        store.heartbeat("pc", epoch, [{"job_id": jid, "fencing": 1, "proc_alive": True,
                                       "supervisor_alive": True}])
        store.reap()
    assert store.get_job(jid)["state"] == "running"


def test_stale_supervisor_counts_as_negative(env):
    store, clock, epoch = env
    c = _running(store, epoch)
    jid = c["job_id"]
    store.heartbeat("pc", epoch, [{"job_id": jid, "fencing": 1, "supervisor_alive": False}])
    clock.t += P.PROCESS_RECOVERY_S + 1
    assert store.reap()["lost"] == 1
    assert store.get_job(jid)["state"] == "lost"


def test_lost_transitions_are_deterministic(env):
    assert P.transition_allowed("starting", "lost")
    assert P.transition_allowed("running", "lost")
    store, _, epoch = env
    job, _ = store.create_job("pc", "fake", "demo", "s", "read_only")
    [c] = store.claim("pc", epoch, 1)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "claimed", "starting")
    store.transition("pc", epoch, c["job_id"], c["fencing"], "starting", "lost")
    assert store.get_job(c["job_id"])["state"] == "lost"


def test_runner_job_state_is_fenced_readonly(env):
    store, _, epoch = env
    c = _running(store, epoch)
    jid = c["job_id"]
    view = store.runner_job_state("pc", epoch, jid, 1)
    assert view == {"state": "running", "cancel_requested": False}
    assert "prompt" not in view
    with pytest.raises(BrokerError) as e:
        store.runner_job_state("pc", epoch, jid, 999)
    assert e.value.code == "stale_fencing"
    with pytest.raises(BrokerError) as e:
        store.runner_job_state("pc", epoch, "nope", 1)
    assert e.value.code == "unknown_job"
