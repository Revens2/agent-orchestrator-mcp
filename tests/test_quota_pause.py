"""Pause-quota (issue #6) : quota fournisseur épuisé => tentative close, mission
suspendue durablement (waiting_for_quota), suivi MCP libéré, reprise explicite
de la même session runtime. Jamais un timeout, jamais un faux succès."""

import json
import tempfile
from pathlib import Path

import pytest

import orch_protocol as P
from orch_mcp.store import BrokerError, Store
from orch_runner.adapters import Fake, OpenCode

# Événement d'erreur réel de `opencode run --format json` (forme APIError).
QUOTA_EVENT = json.dumps({
    "type": "error",
    "sessionID": "ses_abc123",
    "error": {
        "name": "APIError",
        "data": {
            "message": "You exceeded your current quota, please check your plan and billing details.",
            "statusCode": 429,
            "isRetryable": False,
            "responseHeaders": {"Retry-After": "3600"},
            "responseBody": json.dumps({"error": {"code": "insufficient_quota", "type": "insufficient_quota"}}),
        },
    },
})
RATE_LIMIT_EVENT = json.dumps({
    "type": "error",
    "error": {"name": "APIError", "data": {"message": "Rate limit reached for requests", "statusCode": 429}},
})


# ------------------------------------------------------------------ protocole
def test_match_quota_exact_only():
    assert P.match_quota("code insufficient_quota") == "insufficient_quota"
    assert P.match_quota("You've hit your usage limit. Try again later") == "You've hit your usage limit"
    assert P.match_quota("HTTP 429 Too Many Requests") is None  # un 429 seul n'est PAS un quota
    assert P.match_quota("rate_limit_exceeded") is None
    assert P.match_quota("RESOURCE_EXHAUSTED") is None
    assert P.match_quota(None) is None


def test_quota_error_roundtrip():
    assert P.parse_quota_error(P.quota_error("insufficient_quota", 120)) == ("insufficient_quota", 120)
    assert P.parse_quota_error(P.quota_error("insufficient_quota")) == ("insufficient_quota", None)
    # stderr ajouté par le runner : ignoré
    assert P.parse_quota_error("quota_exhausted:x;retry_after_s=5 | stderr: boom") == ("x", 5)
    assert P.parse_quota_error("exit code 1") is None
    assert P.parse_quota_error(None) is None


# ------------------------------------------------------------------ adapter
def _opencode(resume=None):
    a = OpenCode("C:\\opencode.exe", {"model": "m"})
    a.resume_session_id = resume
    return a


def test_opencode_detects_quota_event_with_retry_after():
    a = _opencode()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("Refondre la doc", "read_only", "C:\\w", Path(tmp))
        a.on_line('{"type":"text","part":{"type":"text","text":"étape 1 faite","sessionID":"ses_abc123"}}\n')
        _, activity = a.on_line(QUOTA_EVENT + "\n")
        assert activity.startswith("quota:")
        assert a.quota_exhausted is not None and a.quota_retry_after_s == 3600
        out = a.finish(1, Path(tmp))
    assert not out.ok
    assert P.parse_quota_error(out.error) == (a.quota_exhausted, 3600)
    assert "agent_mission_retry" in out.summary and "étape 1 faite" in out.summary


def test_opencode_quota_never_completed_even_exit_0():
    a = _opencode()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("x", "read_only", "C:\\w", Path(tmp))
        a.on_line(QUOTA_EVENT + "\n")
        out = a.finish(0, Path(tmp))
    assert not out.ok and out.error.startswith(P.QUOTA_EXHAUSTED_PREFIX)


def test_opencode_rate_limit_is_not_quota():
    a = _opencode()
    with tempfile.TemporaryDirectory() as tmp:
        a.build("x", "read_only", "C:\\w", Path(tmp))
        a.on_line(RATE_LIMIT_EVENT + "\n")
        out = a.finish(1, Path(tmp))
    assert a.quota_exhausted is None
    assert not out.ok and not out.error.startswith(P.QUOTA_EXHAUSTED_PREFIX)


def test_opencode_resume_uses_same_session_without_title():
    a = _opencode(resume="ses_abc123")
    with tempfile.TemporaryDirectory() as tmp:
        launch = a.build("Refondre la doc", "read_only", "C:\\w", Path(tmp))
    i = launch.argv.index("--session")
    assert launch.argv[i + 1] == "ses_abc123" and "--title" not in launch.argv
    assert launch.argv[-1] == "Refondre la doc" and a.session_id == "ses_abc123"


@pytest.mark.parametrize("bad", ["--dangerous", "ses 1", "a" * 200, ""])
def test_opencode_resume_rejects_unsafe_session(bad):
    a = _opencode(resume=bad)
    with tempfile.TemporaryDirectory() as tmp, pytest.raises(ValueError):
        a.build("x", "read_only", "C:\\w", Path(tmp))


def test_fake_adapter_quota_and_resume():
    f = Fake("")
    f.on_line("SESSION:fake-ses-1\n")
    f.on_line("QUOTA:insufficient_quota\n")
    assert f.session_id == "fake-ses-1" and f.quota_exhausted == "insufficient_quota"
    assert P.parse_quota_error(f.finish(1, Path(".")).error) == ("insufficient_quota", None)
    g = Fake("")
    g.resume_session_id = "fake-ses-1"
    launch = g.build("print 1", "read_only", ".", Path("."))
    assert launch.env_extra["FAKE_RESUME_SESSION"] == "fake-ses-1"


# ------------------------------------------------------------------ broker
INFO = {
    "version": "test",
    "max_parallel": 2,
    "runtimes": [{"id": "fake", "available": True}],
    "workspaces": [{"id": "demo", "modes": ["read_only", "workspace_write"]}],
}


class Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def _run_to_quota(store, epoch, job_id, fencing, error, session="ses_abc123"):
    store.transition("pc", epoch, job_id, fencing, "claimed", "starting")
    store.transition("pc", epoch, job_id, fencing, "starting", "running", runtime_session_id=session)
    return store.transition("pc", epoch, job_id, fencing, "running", "failed", exit_code=1,
                            result_summary="quota", error=error, runtime_session_id=session)


def _paused_mission(tmp_path, clock, retry_after=600, max_attempts=1):
    store = Store(tmp_path / "q.db", clock=clock)
    epoch = store.hello("pc", INFO, [])
    m = store.create_mission("Refondre la doc", ["ok"], max_attempts=max_attempts,
                             runner_id="pc", runtime="fake", workspace_id="demo", mode="workspace_write")
    claimed = store.claim("pc", epoch, 1)
    _run_to_quota(store, epoch, m["current_job_id"], claimed[0]["fencing"],
                  P.quota_error("insufficient_quota", retry_after) + " | stderr: x")
    return store, epoch, m["mission_id"], m["current_job_id"]


def test_quota_pauses_mission_and_releases_follow(tmp_path):
    clock = Clock()
    store, _, mid, job1 = _paused_mission(tmp_path, clock)
    m = store.get_mission(mid)
    assert m["state"] == P.MISSION_WAITING_FOR_QUOTA  # ni incomplete, ni failed, ni validated
    assert m["pause"]["reason"] == "quota_exhausted:insufficient_quota"
    assert m["pause"]["resume_ready"] is False and m["pause"]["resume_after"]
    kinds = [e["kind"] for e in store.read_events(job1, -1, 50)["events"]]
    assert P.EV_PROVIDER_QUOTA_DETECTED in kinds and P.EV_MISSION_PAUSED in kinds
    assert P.EV_TIMEOUT_MARKED not in kinds
    # Le suivi MCP rend la main IMMÉDIATEMENT : le tour ChatGPT peut se terminer.
    w = store.wait_for_mission(mid, -1, 30)
    assert w["mission_state"] == P.MISSION_WAITING_FOR_QUOTA
    assert w["must_follow"] is False and w["should_continue"] is False
    assert w["paused"] is True and w["next_tool"] == "agent_mission_get"
    assert w["resume_tool"] == "agent_mission_retry"


def test_pause_survives_broker_restart(tmp_path):
    clock = Clock()
    store, *_ , mid, _ = _paused_mission(tmp_path, clock)
    store.close()
    store2 = Store(tmp_path / "q.db", clock=clock)  # nouvelle conversation / broker redémarré
    m = store2.get_mission(mid)
    assert m["state"] == P.MISSION_WAITING_FOR_QUOTA and m["pause"]["resume_tool"] == "agent_mission_retry"


def test_resume_respects_retry_after_then_reuses_session(tmp_path):
    clock = Clock()
    store, epoch, mid, job1 = _paused_mission(tmp_path, clock, retry_after=600, max_attempts=1)
    with pytest.raises(BrokerError) as exc:
        store.retry_mission(mid)
    assert exc.value.code == "quota_not_reset"
    clock.t += 601
    m2 = store.retry_mission(mid)  # max_attempts=1 : la reprise quota ne l'entame pas
    job2 = m2["current_job_id"]
    assert job2 != job1 and m2["state"] == P.MISSION_EXECUTING and m2["pause"] is None
    assert m2["quota_resumes"] == 1
    claimed = store.claim("pc", epoch, 1)
    assert claimed[0]["job_id"] == job2 and claimed[0]["resume_session_id"] == "ses_abc123"
    assert "Reprise après pause quota" in claimed[0]["prompt"]
    kinds = [e["kind"] for e in store.read_events(job2, -1, 50)["events"]]
    assert P.EV_MISSION_RESUMED in kinds
    # La reprise aboutit : needs_validation, jamais validated sur exit 0.
    f = claimed[0]["fencing"]
    store.transition("pc", epoch, job2, f, "claimed", "starting")
    store.transition("pc", epoch, job2, f, "starting", "running")
    store.transition("pc", epoch, job2, f, "running", "completed", exit_code=0, result_summary="ok")
    assert store.get_mission(mid)["state"] == P.MISSION_NEEDS_VALIDATION


def test_force_resume_before_reset_and_unknown_reset(tmp_path):
    clock = Clock()
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    store, _, mid, _ = _paused_mission(tmp_path / "a", clock, retry_after=None)
    assert store.get_mission(mid)["pause"]["resume_after"] is None  # date inconnue : pas inventée
    assert store.retry_mission(mid)["state"] == P.MISSION_EXECUTING
    store2, _, mid2, _ = _paused_mission(tmp_path / "b", clock, retry_after=3600)
    assert store2.retry_mission(mid2, force=True)["state"] == P.MISSION_EXECUTING


def test_paused_mission_can_be_abandoned_not_validated(tmp_path):
    clock = Clock()
    store, _, mid, _ = _paused_mission(tmp_path, clock)
    with pytest.raises(BrokerError):
        store.validate_mission(mid, "validated")
    m = store.validate_mission(mid, "blocked", "forfait non renouvelé")
    assert m["state"] == "blocked" and m["pause"] is None


def test_quota_resume_loop_bounded(tmp_path):
    clock = Clock()
    store, epoch, mid, _ = _paused_mission(tmp_path, clock, retry_after=None)
    for _ in range(P.MAX_QUOTA_RESUMES):
        job = store.retry_mission(mid)["current_job_id"]
        f = store.claim("pc", epoch, 1)[0]["fencing"]
        _run_to_quota(store, epoch, job, f, P.quota_error("insufficient_quota"))
        assert store.get_mission(mid)["state"] == P.MISSION_WAITING_FOR_QUOTA
    with pytest.raises(BrokerError) as exc:
        store.retry_mission(mid)
    assert exc.value.code == "quota_resume_loop"


def test_non_quota_failure_still_incomplete(tmp_path):
    store = Store(tmp_path / "n.db")
    epoch = store.hello("pc", INFO, [])
    m = store.create_mission("Doc", ["ok"], runner_id="pc", runtime="fake", workspace_id="demo")
    f = store.claim("pc", epoch, 1)[0]["fencing"]
    _run_to_quota(store, epoch, m["current_job_id"], f, "exit code 1, Rate limit reached (429)")
    m2 = store.get_mission(m["mission_id"])
    assert m2["state"] == P.MISSION_INCOMPLETE and m2["pause"] is None
    w = store.wait_for_mission(m["mission_id"], -1, 0)
    assert "paused" not in w and w["next_tool"] == "agent_mission_validate"
