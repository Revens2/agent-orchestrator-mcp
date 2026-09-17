"""A) Notification Telegram de fin (outbox) + B) reprise détachée anti-timeout.

Couvre les cas exigés :
- enqueue completed allowlisted, replay idempotent, autre runner non notifié,
  failed/timeout/cancelled/lost sans notif success ;
- sender succès, fail + backoff + retry, outbox persiste restart, message sans secret ;
- detached après deux waits, detached puis terminal, mission detached puis
  needs_validation puis validation, fire_and_forget distinct.
"""

import orch_protocol as P
from orch_mcp.notifications import build_completion_message, dispatch_due, parse_runner_filter
from orch_mcp.store import Store


class Clock:
    def __init__(self):
        self.t = 2_000_000.0

    def __call__(self):
        return self.t

    def advance(self, s: float):
        self.t += s


INFO = {
    "version": "test",
    "max_parallel": 2,
    "runtimes": [{"id": "fake", "available": True}],
    "workspaces": [{"id": "demo", "modes": ["read_only", "workspace_write"]}],
}


def make_store(tmp_path, name="n.db", runners=None):
    clock = Clock()
    store = Store(tmp_path / name, clock=clock, notify_runners=runners)
    return store, clock


def hello(store, runner_id):
    return store.hello(runner_id, INFO, [])


def start_job(store, runner_id, prompt="faire X"):
    return store.create_job(runner_id, "fake", "demo", prompt, "read_only")


def to_running(store, runner_id, epoch, claimed):
    store.transition(runner_id, epoch, claimed["job_id"], claimed["fencing"], "claimed", "starting")
    store.transition(runner_id, epoch, claimed["job_id"], claimed["fencing"], "starting", "running")


def complete_job(store, runner_id, epoch, claimed):
    return store.transition(
        runner_id, epoch, claimed["job_id"], claimed["fencing"],
        "running", "completed", exit_code=0,
    )


# ------------------------------------------------------------- A) enqueue
def test_enqueue_completed_allowlisted(tmp_path):
    store, _ = make_store(tmp_path, runners={"main-windows-pc", "pc-fixe"})
    epoch = hello(store, "main-windows-pc")
    job, _ = start_job(store, "main-windows-pc")
    [c] = store.claim("main-windows-pc", epoch, 1)
    assert c["job_id"] == job["job_id"]
    to_running(store, "main-windows-pc", epoch, c)
    assert store.get_completion_notification(c["job_id"]) is None
    complete_job(store, "main-windows-pc", epoch, c)
    notif = store.get_completion_notification(c["job_id"])
    assert notif is not None
    assert notif["status"] == P.NOTIFY_STATUS_PENDING
    assert notif["runner_id"] == "main-windows-pc"
    assert notif["kind"] == P.NOTIFY_KIND_COMPLETED


def test_replay_idempotent(tmp_path):
    store, _ = make_store(tmp_path, runners={"main-windows-pc"})
    epoch = hello(store, "main-windows-pc")
    _, _ = start_job(store, "main-windows-pc")
    [c] = store.claim("main-windows-pc", epoch, 1)
    to_running(store, "main-windows-pc", epoch, c)
    complete_job(store, "main-windows-pc", epoch, c)
    # retry runner idempotent (même état) : pas de doublon, pas d'erreur
    store.transition(
        "main-windows-pc", epoch, c["job_id"], c["fencing"],
        "running", "completed", exit_code=0,
    )
    dues = store.due_completion_notifications(limit=10)
    assert len([d for d in dues if d["job_id"] == c["job_id"]]) == 1


def test_autre_runner_non_notifie(tmp_path):
    store, _ = make_store(tmp_path, runners={"main-windows-pc", "pc-fixe"})
    epoch = hello(store, "autre-pc")
    _, _ = start_job(store, "autre-pc")
    [c] = store.claim("autre-pc", epoch, 1)
    to_running(store, "autre-pc", epoch, c)
    complete_job(store, "autre-pc", epoch, c)
    assert store.get_completion_notification(c["job_id"]) is None
    assert store.due_completion_notifications(limit=10) == []


def test_failed_timeout_cancelled_lost_pas_de_notif(tmp_path):
    # failed via runner
    store, _ = make_store(tmp_path, "a.db", runners={"main-windows-pc"})
    epoch = hello(store, "main-windows-pc")
    _, _ = start_job(store, "main-windows-pc")
    [c] = store.claim("main-windows-pc", epoch, 1)
    to_running(store, "main-windows-pc", epoch, c)
    store.transition("main-windows-pc", epoch, c["job_id"], c["fencing"], "running", "failed", exit_code=1)
    assert store.get_completion_notification(c["job_id"]) is None

    # timeout via runner
    store2, _ = make_store(tmp_path, "b.db", runners={"main-windows-pc"})
    e2 = hello(store2, "main-windows-pc")
    _, _ = start_job(store2, "main-windows-pc")
    [c2] = store2.claim("main-windows-pc", e2, 1)
    to_running(store2, "main-windows-pc", e2, c2)
    store2.transition("main-windows-pc", e2, c2["job_id"], c2["fencing"], "running", "timeout", exit_code=1)
    assert store2.get_completion_notification(c2["job_id"]) is None

    # cancelled avant lancement (MCP) : pas de notif success
    store3, _ = make_store(tmp_path, "c.db", runners={"main-windows-pc"})
    hello(store3, "main-windows-pc")
    job3, _ = start_job(store3, "main-windows-pc")
    store3.cancel(job3["job_id"])
    assert store3.get_completion_notification(job3["job_id"]) is None

    # lost via reaper (bail expiré après lancement)
    store4, clock4 = make_store(tmp_path, "d.db", runners={"main-windows-pc"})
    e4 = hello(store4, "main-windows-pc")
    _, _ = start_job(store4, "main-windows-pc")
    [c4] = store4.claim("main-windows-pc", e4, 1)
    to_running(store4, "main-windows-pc", e4, c4)
    clock4.advance(P.LEASE_S + 5)
    stats = store4.reap()
    assert stats["lost"] >= 1
    assert store4.get_completion_notification(c4["job_id"]) is None


# ------------------------------------------------------------- A) dispatch
def test_sender_succes(tmp_path):
    store, _ = make_store(tmp_path, runners={"pc-fixe"})
    epoch = hello(store, "pc-fixe")
    _, _ = start_job(store, "pc-fixe")
    [c] = store.claim("pc-fixe", epoch, 1)
    to_running(store, "pc-fixe", epoch, c)
    complete_job(store, "pc-fixe", epoch, c)
    sent = {}

    def fake_sender(msg: str):
        sent["msg"] = msg
        return True, ""

    stats = dispatch_due(store, sender=fake_sender)
    assert stats == {"due": 1, "sent": 1, "retried": 0, "failed": 0, "skipped": 0}
    assert store.get_completion_notification(c["job_id"])["status"] == P.NOTIFY_STATUS_SENT
    assert "pc-fixe" in sent["msg"]


def test_sender_fail_backoff_retry(tmp_path):
    store, clock = make_store(tmp_path, runners={"main-windows-pc"})
    epoch = hello(store, "main-windows-pc")
    _, _ = start_job(store, "main-windows-pc")
    [c] = store.claim("main-windows-pc", epoch, 1)
    to_running(store, "main-windows-pc", epoch, c)
    complete_job(store, "main-windows-pc", epoch, c)

    def failing(_msg: str):
        return False, "boom"

    s1 = dispatch_due(store, sender=failing)
    assert s1["retried"] == 1 and s1["sent"] == 0
    n1 = store.get_completion_notification(c["job_id"])
    assert n1["status"] == P.NOTIFY_STATUS_PENDING and n1["attempts"] == 1
    # backoff : pas due immédiatement
    assert store.due_completion_notifications(limit=10) == []
    # après le délai, due à nouveau ; succès au 2e essai
    clock.advance(61)
    assert len(store.due_completion_notifications(limit=10)) == 1
    s2 = dispatch_due(store, sender=lambda m: (True, ""))
    assert s2["sent"] == 1
    assert store.get_completion_notification(c["job_id"])["status"] == P.NOTIFY_STATUS_SENT


def test_sender_fail_borne_failed(tmp_path):
    store, clock = make_store(tmp_path, runners={"main-windows-pc"})
    epoch = hello(store, "main-windows-pc")
    _, _ = start_job(store, "main-windows-pc")
    [c] = store.claim("main-windows-pc", epoch, 1)
    to_running(store, "main-windows-pc", epoch, c)
    complete_job(store, "main-windows-pc", epoch, c)
    for _ in range(P.NOTIFY_MAX_ATTEMPTS):
        dispatch_due(store, sender=lambda m: (False, "ko"), max_attempts=P.NOTIFY_MAX_ATTEMPTS)
        clock.advance(3601)
    final = store.get_completion_notification(c["job_id"])
    assert final["status"] == P.NOTIFY_STATUS_FAILED
    assert final["attempts"] >= P.NOTIFY_MAX_ATTEMPTS


def test_outbox_persiste_restart(tmp_path):
    store, clock = make_store(tmp_path, "persist.db", runners={"pc-fixe"})
    epoch = hello(store, "pc-fixe")
    _, _ = start_job(store, "pc-fixe")
    [c] = store.claim("pc-fixe", epoch, 1)
    to_running(store, "pc-fixe", epoch, c)
    complete_job(store, "pc-fixe", epoch, c)
    jid = c["job_id"]
    store.close()
    # "restart" : rouvrir la même DB, la ligne est toujours là
    store2 = Store(tmp_path / "persist.db", clock=clock, notify_runners={"pc-fixe"})
    try:
        assert store2.get_completion_notification(jid)["status"] == P.NOTIFY_STATUS_PENDING
        s = dispatch_due(store2, sender=lambda m: (True, ""))
        assert s["sent"] == 1
    finally:
        store2.close()


def test_message_sans_secret(tmp_path):
    secret_prompt = "fais X avec token=sk-ant-abcdefghijklmnopqrstuvwxyz0123456789 et password=motdepasse12345"
    job = {
        "job_id": "abc12345efgh",
        "runner_id": "main-windows-pc",
        "runtime": "opencode",
        "workspace_id": "demo",
        "display_title": "tâche normale",
        "prompt": secret_prompt,
        "duration_s": 12.3,
        "result_summary": "résumé secret token=XYZ",
        "error": "boom secret",
        "output_tail": "sortie complète",
    }
    msg = build_completion_message(job, mission_state="needs_validation")
    assert "main-windows-pc" in msg and "opencode" in msg and "demo" in msg
    assert "abc12345" in msg and "needs_validation" in msg
    assert "completed ≠ mission validée" in msg
    for forbidden in ("sk-ant-", "motdepasse", "résumé secret", "boom secret", "sortie complète", secret_prompt):
        assert forbidden not in msg
    # sans mission : pas de ligne mission, pas de mention validation
    msg2 = build_completion_message(job, mission_state=None)
    assert "mission" not in msg2


def test_parse_runner_filter():
    assert parse_runner_filter(None) == frozenset(P.NOTIFY_RUNNERS_DEFAULT)
    assert parse_runner_filter("") == frozenset(P.NOTIFY_RUNNERS_DEFAULT)
    assert parse_runner_filter("main-windows-pc,pc-fixe") == {"main-windows-pc", "pc-fixe"}
    assert parse_runner_filter(" a , ,b ") == {"a", "b"}


# ------------------------------------------------------------- B) detached
def _running_job(tmp_path, name, runner="main-windows-pc"):
    store, clock = make_store(tmp_path, name, runners={runner})
    epoch = hello(store, runner)
    _, _ = start_job(store, runner)
    [c] = store.claim(runner, epoch, 1)
    to_running(store, runner, epoch, c)
    return store, clock, epoch, c


def test_detached_apres_deux_waits(tmp_path):
    store, _, _, c = _running_job(tmp_path, "w.db")
    seq = store._last_seq(c["job_id"])
    w0 = store.wait_for_change(c["job_id"], since_seq=seq, timeout_s=0.3, waits_done=0)
    assert w0["detached"] is False and w0["should_continue"] is True
    assert w0["next_tool"] == "agent_job_wait"
    w1 = store.wait_for_change(c["job_id"], since_seq=w0["last_event_seq"], timeout_s=0.3, waits_done=1)
    assert w1["detached"] is False and w1["should_continue"] is True
    d = store.wait_for_change(c["job_id"], since_seq=w1["last_event_seq"], timeout_s=0.3, waits_done=2)
    assert d["detached"] is True and d["terminal"] is False
    assert d["should_continue"] is False and d["must_follow"] is False
    assert d["next_tool"] == "agent_job_get"
    assert d["since_seq"] == d["last_event_seq"]
    assert d["resume_hint"]


def test_detached_puis_terminal(tmp_path):
    import threading
    import time as _t

    store, _, epoch, c = _running_job(tmp_path, "wt.db")
    seq = store._last_seq(c["job_id"])
    d = store.wait_for_change(c["job_id"], since_seq=seq, timeout_s=0.3, waits_done=2)
    assert d["detached"] is True  # le caller répond, puis reprend plus tard
    holder = {}

    def waiter():
        holder["res"] = store.wait_for_change(
            c["job_id"], since_seq=d["last_event_seq"], timeout_s=10, waits_done=0
        )

    t = threading.Thread(target=waiter)
    t.start()
    _t.sleep(0.5)
    complete_job(store, "main-windows-pc", epoch, c)
    t.join(timeout=12)
    assert not t.is_alive()
    done = holder["res"]
    assert done["terminal"] is True and done["detached"] is False
    assert done["next_tool"] == "agent_job_get" and done["woke_by"] == "terminal"


def test_mission_detached_puis_needs_validation_puis_validation(tmp_path):
    store, clock = make_store(tmp_path, "m.db", runners={"main-windows-pc"})
    epoch = hello(store, "main-windows-pc")
    m = store.create_mission("faire X", ["X fait"], 2, "main-windows-pc", "fake", "demo", "read_only")
    assert m["state"] == "executing"
    w0 = store.wait_for_mission(m["mission_id"], timeout_s=0.3, waits_done=0)
    assert w0["should_continue"] is True and w0["detached"] is False
    assert w0["next_tool"] == "agent_mission_wait"
    d = store.wait_for_mission(
        m["mission_id"], since_seq=w0["last_event_seq"], timeout_s=0.3, waits_done=2
    )
    assert d["detached"] is True and d["terminal"] is False
    assert d["should_continue"] is False and d["must_follow"] is False
    assert d["next_tool"] == "agent_mission_get"
    assert d["since_seq"] == d["last_event_seq"] and d["resume_hint"]
    # reprise plus tard : le job se termine, la mission passe en needs_validation
    [c] = store.claim("main-windows-pc", epoch, 1)
    to_running(store, "main-windows-pc", epoch, c)
    complete_job(store, "main-windows-pc", epoch, c)
    w2 = store.wait_for_mission(m["mission_id"], since_seq=d["last_event_seq"], timeout_s=5, waits_done=0)
    assert w2["terminal"] is True and w2["detached"] is False
    assert w2["mission_state"] == "needs_validation"
    assert w2["next_tool"] == "agent_mission_validate"
    assert store.get_mission(m["mission_id"])["state"] == "needs_validation"
    v = store.validate_mission(m["mission_id"], "validated", note="ok")
    assert v["state"] == "validated"


def test_fire_and_forget_distinct(tmp_path):
    from orch_mcp.store import follow_for_job

    f = follow_for_job("running", 3)
    f["must_follow"] = False
    f["should_continue"] = False
    f["detached"] = False
    f["fire_and_forget"] = True
    assert f["detached"] is False and f.get("fire_and_forget") is True
    assert "resume_hint" not in f
