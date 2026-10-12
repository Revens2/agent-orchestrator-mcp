"""Pause-quota de mission (issue #6) : classification fournisseur exacte, pause
durable, réveil borné, reprise liée (même session), annulation, suivi MCP."""

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

import orch_protocol as P
from orch_mcp.store import BrokerError, Store
from orch_runner.adapters import Fake, OpenCode


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t


INFO = {
    "version": "test",
    "max_parallel": 2,
    "runtimes": [{"id": "fake", "available": True}, {"id": "opencode", "available": True, "resume_session": True}],
    "workspaces": [{"id": "demo", "modes": ["read_only", "workspace_write"]}],
}


@pytest.fixture
def env(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "q.db", clock=clock)
    store._jitter = lambda: 0.0  # délais déterministes
    epoch = store.hello("pc", INFO, [])
    return store, clock, epoch, tmp_path / "q.db"


def mission(store, runtime="opencode", prompt=None):
    return store.create_mission("Refondre la doc", ["doc à jour"], 2, "pc", runtime, "demo", "workspace_write",
                                prompt=prompt)


def run_and_fail(store, epoch, error, session="ses_abc"):
    [c] = store.claim("pc", epoch, 1)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "claimed", "starting")
    store.transition("pc", epoch, c["job_id"], c["fencing"], "starting", "running", runtime_session_id=session)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "failed", exit_code=1, error=error)
    return c


QUOTA_ERR = P.format_provider_error(P.PROVIDER_QUOTA, "exit code 1, Quota exceeded. Check your plan and billing details.", 1800)


# ------------------------------------------------------------- protocole
def test_classify_exact_signatures_only():
    assert P.classify_provider_error("Quota exceeded. Check your plan and billing details.") == P.PROVIDER_QUOTA
    assert P.classify_provider_error('{"error":{"code":"insufficient_quota"}}') == P.PROVIDER_QUOTA
    assert P.classify_provider_error('{"error":{"type":"GoUsageLimitError"}}') == P.PROVIDER_QUOTA
    assert P.classify_provider_error("The usage limit has been reached") == P.PROVIDER_QUOTA
    # quota avant rate limit : un 429 insufficient_quota est un quota
    assert P.classify_provider_error("insufficient_quota", 429) == P.PROVIDER_QUOTA
    assert P.classify_provider_error("Too busy", 429) == P.PROVIDER_RATE_LIMIT
    assert P.classify_provider_error('{"type":"rate_limit_error"}') == P.PROVIDER_RATE_LIMIT
    assert P.classify_provider_error("x", 401) == P.PROVIDER_AUTH
    assert P.classify_provider_error("ProviderAuthError") == P.PROVIDER_AUTH
    # jamais de déduction depuis un texte libre ambigu / un 429 en texte
    assert P.classify_provider_error("Rate limit exceeded, retry later") is None
    assert P.classify_provider_error("GET /backend-api/conversations 429") is None
    assert P.classify_provider_error("limit error quota?") is None
    assert P.classify_provider_error(None) is None
    assert P.classify_provider_error("x", True) is None  # bool n'est pas un statut


def test_format_parse_roundtrip_and_non_pause_classes():
    err = P.format_provider_error(P.PROVIDER_QUOTA, "msg | avec pipe", 3600)
    assert err.startswith("provider_unavailable:quota_exhausted retry_after_s=3600 | ")
    assert P.parse_provider_pause(err) == (P.PROVIDER_QUOTA, 3600.0)
    assert P.parse_provider_pause(P.format_provider_error(P.PROVIDER_RATE_LIMIT, None)) == (P.PROVIDER_RATE_LIMIT, None)
    auth = P.format_provider_error(P.PROVIDER_AUTH, "401")
    assert auth.startswith("provider_error:auth_required") and P.parse_provider_pause(auth) is None
    assert P.parse_provider_pause("provider_unavailable:bogus | x") is None
    assert P.parse_provider_pause("exit code 1") is None


def test_pause_delay_bounded():
    assert P.pause_delay_s(P.PROVIDER_QUOTA, 0, None) == 900
    assert P.pause_delay_s(P.PROVIDER_QUOTA, 1, None) == 1800
    assert P.pause_delay_s(P.PROVIDER_QUOTA, 10, None) == P.PAUSE_BACKOFF_MAX_S
    assert P.pause_delay_s(P.PROVIDER_RATE_LIMIT, 0, None) == 120
    assert P.pause_delay_s(P.PROVIDER_QUOTA, 0, 5) == P.PAUSE_MIN_DELAY_S  # jamais de polling serré
    assert P.pause_delay_s(P.PROVIDER_QUOTA, 0, 19_380) == 19_380  # retry_after respecté
    assert P.pause_delay_s(P.PROVIDER_QUOTA, 0, 10**9) == P.MAX_PAUSE_TOTAL_S
    assert P.pause_delay_s(P.PROVIDER_QUOTA, 0, None, jitter=1.0) == pytest.approx(900 * 1.1)


def test_parse_reset_in():
    msg = "`5h usage limit` reached. It will reset in 5 hours 23 minutes. To continue..."
    assert P.parse_reset_in(msg) == 5 * 3600 + 23 * 60
    assert P.parse_reset_in("It will reset in 2 days") == 2 * 86_400
    assert P.parse_reset_in("no reset here") is None


# --------------------------------------------------------------- adapter
def _oc(extra=None):
    a = OpenCode("C:\\opencode.exe", {"model": "m", **(extra or {})})
    return a


def _err_line(name, message, status=None, headers=None, body=None):
    data = {"message": message, "isRetryable": False}
    if status is not None:
        data["statusCode"] = status
    if headers is not None:
        data["responseHeaders"] = headers
    if body is not None:
        data["responseBody"] = body
    return json.dumps({"type": "error", "sessionID": "ses_1", "error": {"name": name, "data": data}}) + "\n"


def test_opencode_quota_error_event_classified_with_retry_after():
    a = _oc()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("Refondre la doc", "read_only", "C:\\w", Path(tmp))
        a.on_line(_err_line("APIError", "You exceeded your current quota, please check your plan.", 429,
                            {"Retry-After": "120"}, '{"error":{"code":"insufficient_quota"}}'))
        out = a.finish(1, Path(tmp))
    assert not out.ok and out.failure_class == P.PROVIDER_QUOTA and out.retry_after_s == 120
    assert P.parse_provider_pause(out.error) == (P.PROVIDER_QUOTA, 120.0)


def test_opencode_go_usage_limit_reset_in():
    a = _oc()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("x", "read_only", "C:\\w", Path(tmp))
        a.on_line(_err_line("APIError", "`5h usage limit` reached. It will reset in 1 hours 2 minutes.", 429,
                            {}, '{"type":"error","error":{"type":"GoUsageLimitError"}}'))
        out = a.finish(1, Path(tmp))
    assert out.failure_class == P.PROVIDER_QUOTA and out.retry_after_s == 3720


def test_opencode_rate_limit_and_auth_and_unknown():
    for line, cls, pause in (
        (_err_line("APIError", "Too many requests", 429, {"retry-after-ms": "2500"}), P.PROVIDER_RATE_LIMIT, True),
        (_err_line("ProviderAuthError", "Missing API key"), P.PROVIDER_AUTH, False),
        (_err_line("UnknownError", "boom 429"), None, False),
        # statusCode ignoré hors APIError
        (_err_line("UnknownError", "boom", 429), None, False),
    ):
        a = _oc()
        with tempfile.TemporaryDirectory() as tmp:
            a.build("x", "read_only", "C:\\w", Path(tmp))
            a.on_line(line)
            out = a.finish(1, Path(tmp))
        assert not out.ok and out.failure_class == cls, line
        assert (P.parse_provider_pause(out.error) is not None) is pause, out.error
    a = _oc()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("x", "read_only", "C:\\w", Path(tmp))
        a.on_line(_err_line("APIError", "Too many requests", 429, {"retry-after-ms": "2500"}))
        assert a.finish(1, Path(tmp)).retry_after_s == 2.5


def test_opencode_resume_session_and_quota_watch_argv():
    a = _oc()
    a.resume_session_id = "ses_abc"
    with tempfile.TemporaryDirectory() as tmp:
        argv = a.build("x", "workspace_write", "C:\\w", Path(tmp)).argv
    i = argv.index("--session")
    assert argv[i + 1] == "ses_abc" and argv.index("--") > i
    assert "--print-logs" not in argv  # opt-in
    b = _oc({"quota_watch_logs": True})
    with tempfile.TemporaryDirectory() as tmp:
        argv = b.build("x", "read_only", "C:\\w", Path(tmp)).argv
    assert "--session" not in argv
    assert argv[argv.index("--log-level") + 1] == "ERROR" and "--print-logs" in argv


def test_opencode_stderr_quota_blocks_only_quota():
    a = _oc({"quota_watch_logs": True})
    a.on_stderr('timestamp=x level=ERROR run=ab message="stream error" error="Rate limit: rate_limit_error"\n')
    assert a.provider_block is None  # rate limit court : laissé au retry natif
    a.on_stderr("level=INFO message=GoUsageLimitError\n")
    assert a.provider_block is None  # seulement les lignes ERROR
    a.on_stderr('level=ERROR message="stream error" error="GoUsageLimitError ... It will reset in 3 hours"\n')
    assert a.provider_block == P.PROVIDER_QUOTA
    with tempfile.TemporaryDirectory() as tmp:
        out = a.finish(1, Path(tmp))
    assert P.parse_provider_pause(out.error) == (P.PROVIDER_QUOTA, 3 * 3600.0)
    off = _oc()
    off.on_stderr('level=ERROR error="GoUsageLimitError"\n')
    assert off.provider_block is None  # désactivé par défaut


def test_opencode_success_unaffected_by_classification():
    a = _oc()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("x", "read_only", "C:\\w", Path(tmp))
        a.on_line('{"type":"text","part":{"type":"text","text":"ok","sessionID":"ses_1"}}\n')
        assert a.finish(0, Path(tmp)).ok


def test_fake_provider_directives():
    f = Fake("")
    f.resume_session_id = "ses_z"
    with tempfile.TemporaryDirectory() as tmp:
        assert f.build("x", "read_only", "C:\\w", Path(tmp)).env_extra["ORCH_FAKE_RESUME_SESSION"] == "ses_z"
        f.on_line("SESSION:ses_1\n")
        f.on_line("PROVIDER_ERROR:insufficient_quota|42\n")
        out = f.finish(1, Path(tmp))
    assert f.session_id == "ses_1" and P.parse_provider_pause(out.error) == (P.PROVIDER_QUOTA, 42.0)


# ----------------------------------------------------------------- store
def test_quota_failure_pauses_mission_durably(env):
    store, clock, epoch, _ = env
    m = mission(store)
    c = run_and_fail(store, epoch, QUOTA_ERR)
    got = store.get_mission(m["mission_id"])
    assert got["state"] == P.MISSION_WAITING_FOR_QUOTA
    assert got["current_job"]["state"] == "failed"  # tentative close, jamais completed
    p = got["pause"]
    assert p["reason"] == P.PROVIDER_QUOTA and p["pause_count"] == 1
    assert p["resume_after"] == "1970-01-12T14:16:40Z"  # now + retry_after 1800 s
    kinds = [e["kind"] for e in store.read_events(c["job_id"])["events"]]
    for k in (P.EV_PROVIDER_QUOTA_DETECTED, P.EV_MISSION_PAUSED, P.EV_QUOTA_RETRY_SCHEDULED):
        assert k in kinds
    # pas validable, pas retryable (la pause n'est pas un échec à juger)
    with pytest.raises(BrokerError) as e:
        store.validate_mission(m["mission_id"], "validated")
    assert e.value.code == "mission_not_validatable"
    with pytest.raises(BrokerError):
        store.retry_mission(m["mission_id"])
    # aucun slot runner consommé : rien à réclamer tant que la pause dure
    assert store.claim("pc", epoch, 2) == []
    assert store.wake_paused_missions() == 0


def test_non_pause_failures_stay_incomplete(env):
    store, _, epoch, _ = env
    for err in ("exit code 1", P.format_provider_error(P.PROVIDER_AUTH, "401"), "session_corrupted:bad decrypt"):
        m = mission(store)
        run_and_fail(store, epoch, err)
        assert store.get_mission(m["mission_id"])["state"] == P.MISSION_INCOMPLETE, err


def test_mission_wait_returns_immediately_with_must_follow_false(env):
    store, _, epoch, _ = env
    m = mission(store)
    w0 = store.wait_for_mission(m["mission_id"], timeout_s=0.3)
    assert w0["must_follow"] is True
    c = run_and_fail(store, epoch, QUOTA_ERR)
    import time as _t
    t0 = _t.monotonic()
    w = store.wait_for_mission(m["mission_id"], since_seq=w0["last_event_seq"], timeout_s=30)
    assert _t.monotonic() - t0 < 5
    assert w["mission_state"] == P.MISSION_WAITING_FOR_QUOTA and w["woke_by"] == "paused"
    assert w["must_follow"] is False and w["should_continue"] is False and w["terminal"] is False
    assert w["until"] == "state_change" and w["next_tool"] == "agent_mission_get"
    assert w["resume_after"] and w["pause_reason"] == P.PROVIDER_QUOTA
    # agent_job_wait sur la tentative en pause : signale la mission (pas un échec définitif)
    jw = store.wait_for_change(c["job_id"], timeout_s=0.2)
    assert jw["state"] == "failed" and jw["mission_state"] == P.MISSION_WAITING_FOR_QUOTA
    assert jw["mission_id"] == m["mission_id"] and jw["next_tool"] == "agent_mission_get"


def test_wake_resumes_with_same_session_and_new_linked_job(env):
    store, clock, epoch, _ = env
    m = mission(store, prompt="PROMPT INITIAL LONG")
    c1 = run_and_fail(store, epoch, QUOTA_ERR, session="ses_abc")
    clock.t += 1799
    assert store.wake_paused_missions() == 0  # pas avant resume_after
    clock.t += 2
    assert store.wake_paused_missions() == 1
    got = store.get_mission(m["mission_id"])
    assert got["state"] == P.MISSION_EXECUTING and got["resumes"] == 1 and got["pause"] is None
    assert got["attempt_job_ids"] == [c1["job_id"], got["current_job_id"]]
    [c2] = store.claim("pc", epoch, 1)
    assert c2["job_id"] == got["current_job_id"] and c2["resume_session_id"] == "ses_abc"
    assert "NE REJOUE AUCUNE action" in c2["prompt"] and "PROMPT INITIAL" not in c2["prompt"]
    assert store.get_job(c1["job_id"])["state"] == "failed"  # immuable
    kinds2 = [e["kind"] for e in store.read_events(c2["job_id"])["events"]]
    assert P.EV_MISSION_RESUMED in kinds2
    # la reprise ne consomme pas de tentative : un retry reste possible ensuite
    store.transition("pc", epoch, c2["job_id"], c2["fencing"], "claimed", "starting")
    store.transition("pc", epoch, c2["job_id"], c2["fencing"], "starting", "running")
    store.transition("pc", epoch, c2["job_id"], c2["fencing"], "running", "failed", exit_code=1, error="exit code 1")
    got = store.get_mission(m["mission_id"])
    assert got["state"] == P.MISSION_INCOMPLETE
    assert store.retry_mission(m["mission_id"])["state"] == P.MISSION_EXECUTING


def test_resume_without_session_uses_handoff_with_initial_prompt(env):
    store, clock, epoch, _ = env
    m = mission(store, runtime="fake", prompt="PROMPT INITIAL")
    run_and_fail(store, epoch, QUOTA_ERR, session=None)
    store.resume_mission(m["mission_id"])
    [c2] = store.claim("pc", epoch, 1)
    assert "resume_session_id" not in c2
    assert "reprise exacte non garantie" in c2["prompt"] and "PROMPT INITIAL" in c2["prompt"]


def test_explicit_runtime_switch_is_handoff(env):
    store, _, epoch, _ = env
    m = mission(store)
    run_and_fail(store, epoch, QUOTA_ERR)
    store.resume_mission(m["mission_id"], runtime="fake")
    [c2] = store.claim("pc", epoch, 1)
    assert c2["runtime"] == "fake" and "resume_session_id" not in c2
    assert "Changement de runtime autorisé" in c2["prompt"]
    # runtime non annoncé par le runner : refusé (allowlist runner)
    m2 = mission(store)
    run_and_fail(store, epoch, QUOTA_ERR)
    with pytest.raises(BrokerError) as e:
        store.resume_mission(m2["mission_id"], runtime="codex")
    assert e.value.code == "runtime_unavailable"
    assert store.get_mission(m2["mission_id"])["state"] == P.MISSION_WAITING_FOR_QUOTA


def test_resume_refused_when_not_paused(env):
    store, _, _, _ = env
    m = mission(store)
    with pytest.raises(BrokerError) as e:
        store.resume_mission(m["mission_id"])
    assert e.value.code == "mission_not_paused"


def test_repeated_quota_backoff_then_cap_to_incomplete(env):
    store, clock, epoch, _ = env
    m = mission(store)
    err = P.format_provider_error(P.PROVIDER_QUOTA, "insufficient_quota")
    delays = []
    for i in range(P.MAX_CONSECUTIVE_PAUSES):
        run_and_fail(store, epoch, err)
        got = store.get_mission(m["mission_id"])
        assert got["state"] == P.MISSION_WAITING_FOR_QUOTA and got["pause"]["pause_count"] == i + 1
        with store._lock:
            ra = store._db.execute("SELECT resume_after FROM missions WHERE id=?", (m["mission_id"],)).fetchone()[0]
        delays.append(ra - clock.t)
        clock.t = ra
        assert store.wake_paused_missions() == 1
    assert delays[0] == 900 and delays[1] == 1800 and max(delays) <= P.PAUSE_BACKOFF_MAX_S
    run_and_fail(store, epoch, err)  # pause n°13 : plafond atteint
    got = store.get_mission(m["mission_id"])
    assert got["state"] == P.MISSION_INCOMPLETE  # humain : jamais de boucle infinie


def test_success_after_resume_resets_pause_series(env):
    store, clock, epoch, _ = env
    m = mission(store)
    run_and_fail(store, epoch, QUOTA_ERR)
    clock.t += 4000
    store.wake_paused_missions()
    [c] = store.claim("pc", epoch, 1)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "claimed", "starting")
    store.transition("pc", epoch, c["job_id"], c["fencing"], "starting", "running")
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "completed", exit_code=0)
    got = store.get_mission(m["mission_id"])
    assert got["state"] == P.MISSION_NEEDS_VALIDATION  # completed != validated
    with store._lock:
        row = store._db.execute("SELECT pause_count, pause_started_at FROM missions WHERE id=?", (m["mission_id"],)).fetchone()
    assert row["pause_count"] == 0 and row["pause_started_at"] is None
    assert store.validate_mission(m["mission_id"], "validated")["state"] == "validated"


def test_cancel_during_pause_is_immediate_and_durable(env):
    store, clock, epoch, _ = env
    m = mission(store)
    c = run_and_fail(store, epoch, QUOTA_ERR)
    res = store.cancel_mission(m["mission_id"])
    assert res["result"] == "cancelled" and res["state"] == P.MISSION_CANCELLED and res["pause"] is None
    clock.t += 10 * 86_400
    assert store.wake_paused_missions() == 0
    assert store.get_mission(m["mission_id"])["attempt_job_ids"] == [c["job_id"]]
    assert P.EV_PAUSE_CANCELLED in [e["kind"] for e in store.read_events(c["job_id"])["events"]]
    assert store.cancel_mission(m["mission_id"])["result"] == "already_finished"
    with pytest.raises(BrokerError):
        store.resume_mission(m["mission_id"])
    w = store.wait_for_mission(m["mission_id"], timeout_s=0.2)
    assert w["mission_state"] == P.MISSION_CANCELLED and w["must_follow"] is False


def test_cancel_running_mission_requests_job_cancel(env):
    store, _, epoch, _ = env
    m = mission(store)
    [c] = store.claim("pc", epoch, 1)
    store.transition("pc", epoch, c["job_id"], c["fencing"], "claimed", "starting")
    store.transition("pc", epoch, c["job_id"], c["fencing"], "starting", "running")
    res = store.cancel_mission(m["mission_id"])
    assert res["job_cancel"] == "cancel_requested"
    # la fin du job (même quota) ne ressuscite pas la mission annulée
    store.transition("pc", epoch, c["job_id"], c["fencing"], "running", "failed", exit_code=1, error=QUOTA_ERR)
    assert store.get_mission(m["mission_id"])["state"] == P.MISSION_CANCELLED
    q = mission(store)
    assert store.cancel_mission(q["mission_id"])["job_cancel"] == "cancelled"  # en file : annulé net
    assert store.get_job(q["current_job_id"])["state"] == "cancelled"


def test_resume_is_atomic_no_orphan_job_on_crash(env, monkeypatch):
    """Plantage au milieu de la reprise : rollback complet, aucun job `queued`
    orphelin qu'un runner pourrait prendre, mission toujours en pause."""
    store, clock, epoch, _ = env
    m = mission(store)
    run_and_fail(store, epoch, QUOTA_ERR)
    real_emit = store._emit

    def boom(db, job_id, kind, detail=None):
        if kind == P.EV_MISSION_RESUMED:
            raise RuntimeError("crash simulé")
        return real_emit(db, job_id, kind, detail)

    monkeypatch.setattr(store, "_emit", boom)
    with pytest.raises(RuntimeError):
        store.resume_mission(m["mission_id"])
    monkeypatch.setattr(store, "_emit", real_emit)
    assert store.claim("pc", epoch, 2) == []
    got = store.get_mission(m["mission_id"])
    assert got["state"] == P.MISSION_WAITING_FOR_QUOTA and len(got["attempt_job_ids"]) == 1
    clock.t += 4000
    assert store.wake_paused_missions() == 1 and len(store.claim("pc", epoch, 2)) == 1


def test_concurrent_resume_cancel_wake_single_outcome(env):
    import threading

    store, clock, epoch, _ = env
    for _ in range(5):
        m = mission(store)
        run_and_fail(store, epoch, QUOTA_ERR)
        clock.t += 4000

        def safe(fn):
            try:
                fn()
            except BrokerError:
                pass

        mid = m["mission_id"]
        ts = [threading.Thread(target=safe, args=(f,)) for f in (
            lambda mid=mid: store.resume_mission(mid),
            lambda mid=mid: store.cancel_mission(mid),
            store.wake_paused_missions,
            lambda mid=mid: store.resume_mission(mid),
        )]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        got = store.get_mission(m["mission_id"])
        claimed = store.claim("pc", epoch, 2)
        assert len(got["attempt_job_ids"]) <= 2  # jamais deux reprises
        if got["state"] == P.MISSION_CANCELLED:
            # annulée : au plus la tentative reprise, annulée en file ou marquée cancel
            for c in claimed:
                assert store.get_job(c["job_id"])["state"] == "claimed"
                store.transition("pc", epoch, c["job_id"], c["fencing"], "claimed", "cancelled")
        else:
            assert got["state"] == P.MISSION_EXECUTING and len(claimed) == 1
            store.cancel_mission(m["mission_id"])
            store.transition("pc", epoch, claimed[0]["job_id"], claimed[0]["fencing"], "claimed", "cancelled")


def test_old_runner_without_resume_capability_gets_full_handoff(env):
    store, _, epoch, _ = env
    m = mission(store, prompt="PROMPT INITIAL")
    run_and_fail(store, epoch, QUOTA_ERR, session="ses_abc")
    # runner v1 : n'annonce pas resume_session
    epoch = store.hello("pc", {**INFO, "runtimes": [{"id": "opencode", "available": True}]}, [])
    store.resume_mission(m["mission_id"])
    [c] = store.claim("pc", epoch, 1)
    assert "resume_session_id" not in c and "PROMPT INITIAL" in c["prompt"]


def test_resume_after_custom_retry_uses_retry_prompt(env):
    store, clock, epoch, _ = env
    m = mission(store, runtime="fake", prompt="PROMPT V1 ECARTE")
    run_and_fail(store, epoch, "exit code 1", session=None)
    store.retry_mission(m["mission_id"], prompt="PROMPT V2 CORRIGE")
    run_and_fail(store, epoch, QUOTA_ERR, session=None)
    clock.t += 30 * 86_400 - 1  # bien après la purge des prompts de jobs... borne pause
    store.purge()
    store.resume_mission(m["mission_id"])
    [c] = store.claim("pc", epoch, 1)
    assert "PROMPT V2 CORRIGE" in c["prompt"] and "PROMPT V1" not in c["prompt"]


def test_infinite_retry_after_is_ignored():
    assert P.format_provider_error(P.PROVIDER_QUOTA, "x", float("inf")) == "provider_unavailable:quota_exhausted | x"
    assert P.parse_provider_pause("provider_unavailable:quota_exhausted retry_after_s=inf | x") == (P.PROVIDER_QUOTA, None)
    assert P.pause_delay_s(P.PROVIDER_QUOTA, 0, float("nan")) == 900
    a = _oc()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("x", "read_only", "C:\\w", Path(tmp))
        a.on_line(_err_line("APIError", "insufficient_quota", 429, {"retry-after": "inf"}))
        out = a.finish(1, Path(tmp))
    assert out.failure_class == P.PROVIDER_QUOTA and out.retry_after_s is None


def test_free_text_tail_never_classified():
    """Un 429 passager journalisé puis un échec sans rapport : pas de pause."""
    a = _oc()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("x", "read_only", "C:\\w", Path(tmp))
        a.on_line("warn: rate_limit_exceeded, retrying (insufficient_quota?)\n")
        out = a.finish(1, Path(tmp))
    assert out.failure_class is None and P.parse_provider_pause(out.error) is None


def test_probe_announces_resume_capability():
    assert OpenCode.supports_resume_session and Fake.supports_resume_session
    info = OpenCode(r"C:\absent\opencode.exe", {"probe_generation": False}).probe()
    assert info["resume_session"] is True and info["available"] is False


def test_scheduled_resume_failure_reschedules(env):
    store, clock, epoch, _ = env
    m = mission(store)
    run_and_fail(store, epoch, QUOTA_ERR)
    # runner relancé avec opencode indisponible (probe en échec : quota toujours épuisé)
    store.hello("pc", {**INFO, "runtimes": [{"id": "fake", "available": True}, {"id": "opencode", "available": False}]}, [])
    epoch2 = store.hello("pc", {**INFO, "runtimes": [{"id": "opencode", "available": False}]}, [])
    clock.t += 1801
    assert store.wake_paused_missions() == 0
    got = store.get_mission(m["mission_id"])
    assert got["state"] == P.MISSION_WAITING_FOR_QUOTA and got["pause"]["pause_count"] == 2
    assert len(got["attempt_job_ids"]) == 1
    store.hello("pc", INFO, [])
    clock.t += 3600
    assert store.wake_paused_missions() == 1
    assert epoch2 is not None


def test_pause_survives_broker_restart(env):
    store, clock, epoch, db = env
    m = mission(store)
    run_and_fail(store, epoch, QUOTA_ERR)
    store.close()
    store2 = Store(db, clock=clock)
    got = store2.get_mission(m["mission_id"])
    assert got["state"] == P.MISSION_WAITING_FOR_QUOTA and got["pause"]["pause_count"] == 1
    clock.t += 1801
    assert store2.wake_paused_missions() == 1
    assert store2.wake_paused_missions() == 0  # jamais deux reprises
    assert len(store2.get_mission(m["mission_id"])["attempt_job_ids"]) == 2


def test_runner_restart_during_pause_no_relaunch(env):
    store, _, _, _ = env
    m = mission(store)
    run_and_fail(store, store.hello("pc", INFO, []), QUOTA_ERR)
    epoch = store.hello("pc", INFO, [])  # redémarrage runner
    assert store.claim("pc", epoch, 2) == []
    assert store.get_mission(m["mission_id"])["state"] == P.MISSION_WAITING_FOR_QUOTA


def test_migration_adds_pause_columns_to_old_db(tmp_path):
    db = tmp_path / "old.db"
    con = sqlite3.connect(db)
    con.executescript(
        """CREATE TABLE missions (id TEXT PRIMARY KEY, objective TEXT NOT NULL, acceptance_json TEXT NOT NULL DEFAULT '[]',
             max_attempts INTEGER NOT NULL DEFAULT 2, attempts INTEGER NOT NULL DEFAULT 0, current_job_id TEXT,
             state TEXT NOT NULL DEFAULT 'executing', validation_state TEXT, validation_note TEXT,
             created_at REAL NOT NULL, updated_at REAL NOT NULL);
           INSERT INTO missions(id, objective, created_at, updated_at, state) VALUES ('m1', 'old', 1, 1, 'incomplete');"""
    )
    con.commit()
    con.close()
    store = Store(db, clock=Clock())
    got = store.get_mission("m1")
    assert got["state"] == "incomplete" and got["resumes"] == 0 and got["pause"] is None
    Store(db, clock=Clock())  # idempotent


def test_stderr_secondary_model_failure_never_blocks():
    """Ligne réelle capturée (opencode 1.18.29, 2026-10-09) : le petit modèle de
    titre échoue pendant un run principal sain -> jamais d'arrêt."""
    a = _oc({"quota_watch_logs": True})
    a.on_stderr('timestamp=2026-10-09T20:03:52.092Z level=ERROR run=18cd40b0 message="stream error" '
                'providerID=opencode modelID=gpt-5.4-nano session.id=ses_x small=true agent=title mode=primary '
                'error.error="AI_APICallError: Upstream request failed: Insufficient account funds GoUsageLimitError"\n')
    a.on_stderr('timestamp=2026-10-09T20:03:49.600Z level=ERROR run=18cd40b0 message="failed to load plugin" '
                'path=list error="Plugin export is not a function"\n')
    assert a.provider_block is None
    a.on_stderr('timestamp=2026-10-09T20:04:00.000Z level=ERROR run=18cd40b0 message="stream error" providerID=opencode '
                'modelID=m session.id=ses_x small=false agent=build mode=primary '
                'error.error="AI_APICallError: GoUsageLimitError ... It will reset in 2 hours"\n')
    assert a.provider_block == P.PROVIDER_QUOTA and a.retry_after == 7200
