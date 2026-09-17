"""Contrat de follow-through : après create/start, le caller DOIT continuer
jusqu'au terminal dans le même tour (waits répétés), puis inspecter/valider.

Un simple timeout de wait pendant que le process continue N'EST PAS une raison
de répondre : le retour machine-lisible l'impose (should_continue=true).
"""

import threading

import orch_protocol as P
from orch_mcp.store import Store, follow_for_job, follow_for_wait


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


def make_store(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "f.db", clock=clock)
    epoch = store.hello("pc", INFO, [])
    return store, clock, epoch


def start(store, **kw):
    args = dict(runner_id="pc", runtime="fake", workspace_id="demo", prompt="hello", mode="read_only")
    args.update(kw)
    return store.create_job(**args)


def to_running(store, epoch, claimed):
    store.transition("pc", epoch, claimed["job_id"], claimed["fencing"], "claimed", "starting")
    store.transition("pc", epoch, claimed["job_id"], claimed["fencing"], "starting", "running")


# ------------------------------------------------------- blocs de suivi
def test_follow_helpers_contract(tmp_path):
    f = follow_for_job("running", 7)
    assert f == {"must_follow": True, "terminal": False, "should_continue": True,
                "detached": False,
                "next_tool": "agent_job_wait", "wait_timeout_s": 25,
                "until": "terminal", "since_seq": 7}
    f_done = follow_for_job("completed", 9)
    assert f_done["terminal"] is True and f_done["should_continue"] is False
    assert f_done["must_follow"] is False and f_done["next_tool"] == "agent_job_get"
    assert f_done["detached"] is False
    w = follow_for_wait("running", "timeout", 3)
    assert w["should_continue"] is True and w["next_tool"] == "agent_job_wait"
    assert w["woke_by"] == "timeout" and w["since_seq"] == 3
    assert w["detached"] is False
    # budget épuisé => détaché (job)
    d = follow_for_wait("running", "timeout", 3, waits_done=2, job_id="jid")
    assert d["detached"] is True and d["terminal"] is False
    assert d["should_continue"] is False and d["must_follow"] is False
    assert d["next_tool"] == "agent_job_get" and "resume_hint" in d


def test_wait_timeout_carries_recall_contract(tmp_path):
    """Un timeout sur job actif impose le rappel : should_continue + curseur."""
    store, _, _ = make_store(tmp_path)
    job, _ = start(store)
    res = store.wait_for_change(job["job_id"], timeout_s=0.5)
    assert res["woke_by"] == "timeout" and res["state"] == "queued"
    assert res["terminal"] is False
    assert res["should_continue"] is True and res["must_follow"] is True
    assert res["next_tool"] == "agent_job_wait"
    assert res["until"] == "terminal"
    assert res["since_seq"] == res["last_event_seq"]
    # le curseur est réutilisable tel quel pour le rappel
    res2 = store.wait_for_change(job["job_id"], since_seq=res["last_event_seq"], timeout_s=0.5)
    assert res2["should_continue"] is True and res2["woke_by"] == "timeout"


def test_terminal_already_reached_is_done(tmp_path):
    """Job déjà terminal : pas de rappel, inspection du résultat."""
    import time as _t

    store, _, epoch = make_store(tmp_path)
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "completed", exit_code=0)
    t0 = _t.monotonic()
    res = store.wait_for_change(c["job_id"], since_seq=-1, timeout_s=10)
    assert res["woke_by"] == "terminal" and res["terminal"] is True
    assert res["should_continue"] is False and res["must_follow"] is False
    assert res["next_tool"] == "agent_job_get"
    assert _t.monotonic() - t0 < 2


def test_repeated_waits_then_terminal(tmp_path):
    """Plusieurs waits successifs puis terminal : la boucle du contrat."""
    store, _, epoch = make_store(tmp_path)
    job, _ = start(store)
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    seq = store._last_seq(c["job_id"])
    # wait 1 : rien ne bouge -> timeout MAIS should_continue
    w1 = store.wait_for_change(c["job_id"], since_seq=seq, timeout_s=0.5)
    assert w1["woke_by"] == "timeout" and w1["should_continue"] is True
    # wait 2 (rappel immédiat avec le curseur) en thread, puis terminal
    holder = {}

    def waiter():
        holder["res"] = store.wait_for_change(c["job_id"], since_seq=w1["last_event_seq"], timeout_s=10)

    t = threading.Thread(target=waiter)
    t.start()
    import time as _t

    _t.sleep(0.6)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "completed", exit_code=0)
    t.join(timeout=12)
    assert not t.is_alive()
    w2 = holder["res"]
    assert w2["woke_by"] == "terminal" and w2["terminal"] is True
    assert w2["should_continue"] is False and w2["next_tool"] == "agent_job_get"


def test_full_job_sequence_to_result(tmp_path):
    """create/start -> waits répétés -> terminal -> get/output/events."""
    store, _, epoch = make_store(tmp_path)
    job, created = start(store)
    assert created and job["state"] == "queued"
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    store.event("pc", epoch, c["job_id"], c["fencing"], "evt-f-00001", activity="work", output="part1 ")
    w = store.wait_for_change(c["job_id"], since_seq=-1, timeout_s=0.5)
    assert w["should_continue"] is True  # events non vus : pas une fin
    store.event("pc", epoch, c["job_id"], c["fencing"], "evt-f-00002", activity="work", output="part2")
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "completed",
                     exit_code=0, result_summary="fait X")
    done = store.wait_for_change(c["job_id"], since_seq=w["last_event_seq"], timeout_s=5)
    assert done["terminal"] is True and done["next_tool"] == "agent_job_get"
    got = store.get_job(c["job_id"])
    assert got["state"] == "completed" and got["exit_code"] == 0
    out = store.read_output(c["job_id"])
    assert "part1" in out["text"] and "part2" in out["text"]
    kinds = [e["kind"] for e in store.read_events(c["job_id"])["events"]]
    assert P.EV_PROCESS_EXIT in kinds


def test_mission_wait_full_sequence_with_validation(tmp_path):
    """mission : create -> waits -> needs_validation -> validate (completed != validated)."""
    store, _, epoch = make_store(tmp_path)
    m = store.create_mission("faire X", ["X fait"], 2, "pc", "fake", "demo", "read_only")
    assert m["state"] == "executing"
    w1 = store.wait_for_mission(m["mission_id"], timeout_s=0.5)
    assert w1["mission_state"] == "executing" and w1["terminal"] is False
    assert w1["should_continue"] is True and w1["must_follow"] is True
    assert w1["next_tool"] == "agent_mission_wait"
    [c] = store.claim("pc", epoch, 1)
    assert c["job_id"] == m["current_job_id"]
    to_running(store, epoch, c)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "completed", exit_code=0)
    w2 = store.wait_for_mission(m["mission_id"], since_seq=w1["last_event_seq"], timeout_s=5)
    assert w2["terminal"] is True and w2["job_state"] == "completed"
    assert w2["mission_state"] == "needs_validation"
    assert w2["should_continue"] is False  # plus d'attente : place à l'examen
    assert w2["next_tool"] == "agent_mission_validate"
    # completed process != mission validée : validation explicite requise
    assert store.get_mission(m["mission_id"])["state"] == "needs_validation"
    v = store.validate_mission(m["mission_id"], "validated", note="critères vérifiés")
    assert v["state"] == "validated" and v["validation_state"] == "validated"


def test_mission_wait_failed_job_points_to_validate(tmp_path):
    store, _, epoch = make_store(tmp_path)
    m = store.create_mission("faire Y", ["Y fait"], 2, "pc", "fake", "demo", "read_only")
    [c] = store.claim("pc", epoch, 1)
    to_running(store, epoch, c)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "failed", exit_code=1)
    w = store.wait_for_mission(m["mission_id"], timeout_s=2)
    assert w["terminal"] is True and w["mission_state"] == "incomplete"
    assert w["next_tool"] == "agent_mission_validate"  # examen humain, retry explicite éventuel
    assert store.wait_for_mission("no-such-mission") is None
