"""Signal de vie : un agent lent, ou un utilisateur occupé, n'est pas une panne.

Trois régressions couvertes, dans l'ordre où elles font mal :

1. un `woke_by=timeout` ne dit rien de vivant -> ChatGPT conclut à l'échec ;
2. l'utilisateur change sa clé d'API après un quota épuisé -> le processus mort
   fait démarrer PROCESS_RECOVERY_S et le job tombe en `lost` en 60 s, alors que
   l'attente est VOULUE ;
3. le job meurt sur ce même quota -> il est annoncé comme un échec technique au
   lieu d'une action humaine à faire puis une relance explicite.
"""

import orch_protocol as P
from orch_mcp.store import Store, follow_for_wait


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
    store = Store(tmp_path / "k.db", clock=clock)
    epoch = store.hello("pc", INFO, [])
    return store, clock, epoch


def running_job(store, epoch, clock, alive=1):
    job, _ = store.create_job("pc", "fake", "demo", "bosse", "read_only", None, None)
    claimed = store.claim("pc", epoch, 1)[0]
    jid, fen = claimed["job_id"], claimed["fencing"]
    store.transition("pc", epoch, jid, fen, "claimed", "starting")
    store.transition("pc", epoch, jid, fen, "starting", "running",
                     telemetry={"proc_alive": alive, "pid": 4242})
    return jid, fen


def heartbeat(store, epoch, jid, fen):
    store.heartbeat("pc", epoch, [{"job_id": jid, "fencing": fen}])


# ---------------------------------------------------- 1. le timeout prouve la vie
def test_wait_timeout_carries_proof_of_life(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    jid, fen = running_job(store, epoch, clock)
    res = store.wait_for_change(jid, since_seq=99, timeout_s=0.3)
    assert res["woke_by"] == "timeout"
    assert res["terminal"] is False and res["should_continue"] is True
    live = res["liveness"]
    assert live["verdict"] == "working" and live["alive"] is True
    # Les preuves sont datées et réellement observées, jamais supposées.
    signals = {e["signal"] for e in live["evidence"]}
    assert "runner_heartbeat" in signals and "process_telemetry" in signals
    assert live["freshest_signal_age_s"] is not None
    assert "signe de vie" in live["message"]


def test_liveness_never_invents_a_signal(tmp_path):
    store, _, epoch = make_store(tmp_path)
    job, _ = store.create_job("pc", "fake", "demo", "en file", "read_only", None, None)
    live = store.liveness(job["job_id"])
    # Job jamais lancé : ni télémétrie ni sortie -> absentes, pas supposées bonnes.
    signals = {e["signal"] for e in live["evidence"]}
    assert "process_telemetry" not in signals and "agent_output" not in signals
    assert live["verdict"] == "starting" and live["keep_waiting"] is True


def test_liveness_reports_lost_contact(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    jid, _ = running_job(store, epoch, clock)
    clock.t += P.ONLINE_WINDOW_S + 5
    live = store.liveness(jid)
    assert live["verdict"] == "lost_contact" and live["alive"] is False


# ------------------------------- 2. la pause humaine suspend les comptes à rebours
def test_pause_prevents_lost_while_user_swaps_api_key(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    jid, fen = running_job(store, epoch, clock)
    # Le processus meurt (quota épuisé) : sans pause, recovery -> lost en 60 s.
    store.event("pc", epoch, jid, fen, "ev-dead-00", telemetry={"proc_alive": 0})
    store.pause_job(jid, P.PAUSE_QUOTA, "je change ma clé", 1800, P.PAUSE_SRC_CHATGPT)

    clock.t += P.PROCESS_RECOVERY_S * 4  # l'utilisateur prend son temps
    heartbeat(store, epoch, jid, fen)
    store.reap()

    job = store.get_job(jid)
    assert job["state"] == "running", "une attente humaine ne doit jamais devenir `lost`"
    assert job["execution_health"] == P.WAITING_FOR_HUMAN
    human = job["human_action_required"]
    assert human["reason"] == P.PAUSE_QUOTA and human["job_is_alive"] is True
    # Rien n'est falsifié : l'observation « processus mort » reste visible.
    assert job["runtime_process_health"]["alive"] is False


def test_pause_suppresses_stall_and_hard_timeout(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    jid, fen = running_job(store, epoch, clock)
    store.pause_job(jid, P.PAUSE_MANUAL, None, P.PAUSE_MAX_S, P.PAUSE_SRC_HUMAN)
    clock.t += P.STALL_S + 60
    heartbeat(store, epoch, jid, fen)
    stats = store.reap()
    assert stats["stalled"] == 0 and stats["suspected_stall"] == 0
    assert stats["timeout_cancel"] == 0
    assert store.get_job(jid)["execution_health"] == P.WAITING_FOR_HUMAN


def test_pause_is_bounded_and_supervision_comes_back(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    jid, fen = running_job(store, epoch, clock)
    store.pause_job(jid, P.PAUSE_AUTH, None, 120, P.PAUSE_SRC_HUMAN)
    clock.t += 121
    heartbeat(store, epoch, jid, fen)
    assert store.reap()["pause_expired"] == 1
    job = store.get_job(jid)
    assert job["human_action_required"] is None
    assert job["execution_health"] != P.WAITING_FOR_HUMAN
    kinds = [e["kind"] for e in store.read_events(jid)["events"]]
    assert P.EV_PAUSE_EXPIRED in kinds


def test_wait_returns_immediately_when_paused(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    jid, _ = running_job(store, epoch, clock)
    store.pause_job(jid, P.PAUSE_QUOTA, "quota OpenCode", None, P.PAUSE_SRC_CHATGPT)
    res = store.wait_for_change(jid, since_seq=-1, timeout_s=30)
    assert res["woke_by"] == "paused"
    # Seul arrêt intermédiaire légitime : parler à l'utilisateur, pas attendre.
    assert res["terminal"] is False and res["should_continue"] is False
    assert res["stop_reason"] == "waiting_for_human"
    assert res["resume_with"] == "agent_job_wait"
    assert res["human_action_required"]["reason"] == P.PAUSE_QUOTA
    assert res["liveness"]["verdict"] == "waiting_for_human"


def test_resume_restores_follow_through(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    jid, fen = running_job(store, epoch, clock)
    store.pause_job(jid, P.PAUSE_QUOTA, None, None, P.PAUSE_SRC_CHATGPT)
    assert store.resume_job(jid, P.PAUSE_SRC_CHATGPT, "nouvelle clé en place")["result"] == "resumed"
    assert store.resume_job(jid)["result"] == "not_paused"  # idempotent
    res = store.wait_for_change(jid, since_seq=999, timeout_s=0.3)
    assert res["should_continue"] is True and "human_action_required" not in res
    kinds = [e["kind"] for e in store.read_events(jid)["events"]]
    assert P.EV_PAUSED in kinds and P.EV_RESUMED in kinds


def test_pause_guards(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    jid, fen = running_job(store, epoch, clock)
    assert store.pause_job("fantome")["result"] == "unknown_job"
    assert store.pause_job(jid, P.PAUSE_MANUAL)["result"] == "paused"
    assert store.pause_job(jid, P.PAUSE_MANUAL)["result"] == "already_paused"
    store.resume_job(jid)
    store.transition("pc", epoch, jid, fen, "running", "completed", exit_code=0)
    assert store.pause_job(jid)["result"] == "already_finished"


# --------------------------- détection automatique sur la sortie de l'agent
def test_quota_in_output_pauses_without_any_runner_change(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    jid, fen = running_job(store, epoch, clock)
    store.event("pc", epoch, jid, fen, "ev-quota-1",
                output="Error: insufficient_quota - please check your plan")
    job = store.get_job(jid)
    assert job["execution_health"] == P.WAITING_FOR_HUMAN
    assert job["human_action_required"]["reason"] == P.PAUSE_QUOTA
    assert job["human_action_required"]["declared_by"] == P.PAUSE_SRC_RUNNER

    # L'agent reparle normalement : l'utilisateur a fait le nécessaire.
    store.event("pc", epoch, jid, fen, "ev-back-1", output="reprise du travail")
    assert store.get_job(jid)["human_action_required"] is None


def test_ordinary_output_never_triggers_a_pause(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    jid, fen = running_job(store, epoch, clock)
    store.event("pc", epoch, jid, fen, "ev-plain-1",
                output="warning: rate of progress is limited by an error budget")
    assert store.get_job(jid)["human_action_required"] is None


# ------------------- 3. mourir sur un quota n'est pas « l'agent a échoué »
def test_terminal_on_quota_is_human_action_not_failure(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    mission = store.create_mission(
        "livrer le correctif", ["tests verts"], 2, "pc", "fake", "demo", "read_only", None, None, None,
    )
    jid = mission["current_job_id"]
    claimed = store.claim("pc", epoch, 1)[0]
    fen = claimed["fencing"]
    store.transition("pc", epoch, jid, fen, "claimed", "starting")
    store.transition("pc", epoch, jid, fen, "starting", "running", telemetry={"proc_alive": 1})
    store.transition("pc", epoch, jid, fen, "running", "failed", exit_code=1,
                     error="opencode: You exceeded your current quota")

    job = store.get_job(jid)
    human = job["human_action_required"]
    assert human is not None and human["reason"] == P.PAUSE_QUOTA
    assert human["job_is_alive"] is False and human["resume_with"] == "agent_mission_retry"
    # La mission dit « il manque une action », pas « l'agent a échoué ».
    assert store.get_mission(mission["mission_id"])["state"] == P.MISSION_BLOCKED
    # Et une fois la clé changée, la relance explicite est autorisée.
    again = store.retry_mission(mission["mission_id"])
    assert again["current_job_id"] != jid


def test_terminal_failure_without_blocker_stays_incomplete(tmp_path):
    store, clock, epoch = make_store(tmp_path)
    mission = store.create_mission(
        "livrer", ["ok"], 2, "pc", "fake", "demo", "read_only", None, None, None,
    )
    jid = mission["current_job_id"]
    claimed = store.claim("pc", epoch, 1)[0]
    fen = claimed["fencing"]
    store.transition("pc", epoch, jid, fen, "claimed", "starting")
    store.transition("pc", epoch, jid, fen, "starting", "running")
    store.transition("pc", epoch, jid, fen, "running", "failed", exit_code=2,
                     error="AssertionError dans test_widget")
    assert store.get_job(jid)["human_action_required"] is None
    assert store.get_mission(mission["mission_id"])["state"] == P.MISSION_INCOMPLETE


# ------------------------------------------------------- bloc de suivi pur
def test_follow_for_wait_stops_only_on_human_action(tmp_path):
    ongoing = follow_for_wait("running", "timeout", 4)
    assert ongoing["should_continue"] is True and "stop_reason" not in ongoing
    blocked = follow_for_wait("running", "paused", 4, {"required": True, "reason": P.PAUSE_QUOTA})
    assert blocked["should_continue"] is False and blocked["must_follow"] is False
    assert blocked["stop_reason"] == "waiting_for_human"
    # Sur un job terminal, le blocage est une info, pas un arrêt de suivi.
    done = follow_for_wait("failed", "terminal", 4, {"required": True, "reason": P.PAUSE_QUOTA})
    assert done["terminal"] is True and "stop_reason" not in done
    assert done["human_action_required"]["reason"] == P.PAUSE_QUOTA
