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
    assert human["job_is_alive"] is False and human["resume_with"] == "agent_job_relaunch"
    # `fake` n'a pas d'identifiants à recharger : pas de redémarrage imposé.
    assert human["restart_required"] is False
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


# ------------- relance après changement de clé : il faut un PROCESSUS NEUF
def die_on_quota(store, epoch, jid, fen, session="ses_abc123"):
    store.event("pc", epoch, jid, fen, "ev-sid-1", output="je bosse", runtime_session_id=session)
    store.transition("pc", epoch, jid, fen, "running", "failed", exit_code=1,
                     error="opencode: insufficient_quota")


def claim_one(store, epoch, job_id):
    """Le claim peut ramener plusieurs jobs : on isole celui qu'on teste."""
    return next(c for c in store.claim("pc", epoch, 2) if c["job_id"] == job_id)


def opencode_job(store, epoch):
    job, _ = store.create_job("pc", "opencode", "demo", "analyse le projet", "read_only", None, None)
    claimed = claim_one(store, epoch, job["job_id"])
    jid, fen = claimed["job_id"], claimed["fencing"]
    store.transition("pc", epoch, jid, fen, "claimed", "starting")
    store.transition("pc", epoch, jid, fen, "starting", "running", telemetry={"proc_alive": 1})
    return jid, fen


INFO_OC = {
    "version": "test",
    "max_parallel": 2,
    "runtimes": [{"id": "fake", "available": True}, {"id": "opencode", "available": True}],
    "workspaces": [{"id": "demo", "modes": ["read_only", "workspace_write"]}],
}


def make_store_oc(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "oc.db", clock=clock)
    epoch = store.hello("pc", INFO_OC, [])
    return store, clock, epoch


def test_pause_says_a_restart_is_required(tmp_path):
    store, clock, epoch = make_store_oc(tmp_path)
    jid, fen = opencode_job(store, epoch)
    store.pause_job(jid, P.PAUSE_QUOTA, None, None, P.PAUSE_SRC_CHATGPT)
    human = store.get_job(jid)["human_action_required"]
    # Une simple levée de pause ne suffit PAS : le processus a lu sa clé au démarrage.
    assert human["restart_required"] is True
    assert "relancer" in human["restart_note"]
    assert "reprise" in human["restart_note"]


def test_manual_pause_needs_no_restart(tmp_path):
    store, clock, epoch = make_store_oc(tmp_path)
    jid, _ = opencode_job(store, epoch)
    store.pause_job(jid, P.PAUSE_MANUAL, None, None, P.PAUSE_SRC_HUMAN)
    human = store.get_job(jid)["human_action_required"]
    assert human["restart_required"] is False and human["restart_note"] is None


def test_relaunch_continues_the_opencode_conversation(tmp_path):
    store, clock, epoch = make_store_oc(tmp_path)
    jid, fen = opencode_job(store, epoch)
    die_on_quota(store, epoch, jid, fen)

    out = store.relaunch_job(jid)
    assert out["result"] == "relaunched"
    assert out["conversation"] == "continued"
    assert out["resumed_session_id"] == "ses_abc123"
    assert out["should_continue"] is True and out["next_tool"] == "agent_job_wait"

    # Le runner reçoit la session à reprendre dans le claim : c'est ce qui
    # permet d'ouvrir un processus NEUF (nouvelle clé) sur la MÊME conversation.
    claimed = claim_one(store, epoch, out["job_id"])
    assert claimed["resume_session_id"] == "ses_abc123"


def test_relaunch_is_honest_when_the_runtime_cannot_resume(tmp_path):
    store, clock, epoch = make_store_oc(tmp_path)
    jid, fen = running_job(store, epoch, clock)  # runtime `fake` : pas de reprise
    store.event("pc", epoch, jid, fen, "ev-sid-2", output="x", runtime_session_id="s1")
    store.transition("pc", epoch, jid, fen, "running", "failed", exit_code=1,
                     error="invalid_api_key")
    out = store.relaunch_job(jid)
    assert out["conversation"] == "fresh" and out["resumed_session_id"] is None
    assert "repart de zéro" in out["note"]


def test_relaunch_refuses_while_the_job_still_holds_the_workspace(tmp_path):
    store, clock, epoch = make_store_oc(tmp_path)
    jid, _ = opencode_job(store, epoch)
    out = store.relaunch_job(jid)
    assert out["result"] == "job_still_active" and "agent_job_cancel" in out["hint"]
    assert store.relaunch_job("fantome")["result"] == "unknown_job"


def test_mission_retry_after_quota_resumes_the_session(tmp_path):
    store, clock, epoch = make_store_oc(tmp_path)
    mission = store.create_mission(
        "analyser", ["rapport"], 2, "pc", "opencode", "demo", "read_only", None, None, None,
    )
    jid = mission["current_job_id"]
    claimed = next(c for c in store.claim("pc", epoch, 2) if c["job_id"] == jid)
    fen = claimed["fencing"]
    store.transition("pc", epoch, jid, fen, "claimed", "starting")
    store.transition("pc", epoch, jid, fen, "starting", "running", telemetry={"proc_alive": 1})
    die_on_quota(store, epoch, jid, fen, session="ses_mission")

    assert store.get_mission(mission["mission_id"])["state"] == P.MISSION_BLOCKED
    again = store.retry_mission(mission["mission_id"])
    new_jid = again["current_job_id"]
    claimed2 = next(c for c in store.claim("pc", epoch, 2) if c["job_id"] == new_jid)
    assert claimed2["resume_session_id"] == "ses_mission"


def test_corrupted_session_is_never_resumed(tmp_path):
    """Règle existante préservée : une session corrompue repart TOUJOURS neuve,
    même si le runtime sait reprendre."""
    store, clock, epoch = make_store_oc(tmp_path)
    mission = store.create_mission(
        "analyser", ["rapport"], 2, "pc", "opencode", "demo", "read_only", None, None, None,
    )
    jid = mission["current_job_id"]
    claimed = next(c for c in store.claim("pc", epoch, 2) if c["job_id"] == jid)
    fen = claimed["fencing"]
    store.transition("pc", epoch, jid, fen, "claimed", "starting")
    store.transition("pc", epoch, jid, fen, "starting", "running")
    store.event("pc", epoch, jid, fen, "ev-sid-3", output="x", runtime_session_id="ses_pourrie")
    store.transition("pc", epoch, jid, fen, "running", "failed", exit_code=1,
                     error=P.SESSION_CORRUPTED_PREFIX + " failed to load plugin")
    again = store.retry_mission(mission["mission_id"])
    claimed2 = claim_one(store, epoch, again["current_job_id"])
    assert claimed2["resume_session_id"] is None


# ------------------------------------------------ qui est qui : machines et runtimes
def test_runtime_identity_separates_claude_code_from_desktop(tmp_path):
    store, _, _ = make_store_oc(tmp_path)
    cli = P.runtime_identity("claude-code")
    app = P.runtime_identity("claude-desktop")
    assert cli["kind"] == "cli" and app["kind"] == "desktop_app"
    # La confusion est nommée explicitement, dans les deux sens.
    assert cli["distinct_from"] == "claude-desktop"
    assert app["distinct_from"] == "claude-code"
    assert "headless" in cli["distinction"] and "FENÊTRE" in app["distinction"]
    assert P.runtime_identity("inconnu") is None


def test_runner_list_carries_machine_and_runtime_identity(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "id.db", clock=clock)
    store.hello("main-windows-pc", {
        **INFO_OC,
        "machine": {"label": "PC du bureau", "hostname": "CAROLINE-PC",
                    "os": "Windows 11", "role": "poste de travail"},
    }, [])
    runner = store.runners()[0]
    assert runner["machine"]["label"] == "PC du bureau"
    assert runner["machine"]["hostname"] == "CAROLINE-PC"
    assert runner["machine"]["description"] is None  # non déclaré : jamais deviné
    ids = {rt["id"]: rt["identity"] for rt in runner["runtimes"]}
    assert ids["opencode"]["vendor"] == "SST"
    assert store.runner_inspect("main-windows-pc")["machine"]["label"] == "PC du bureau"


def test_machine_identity_is_absent_when_not_declared(tmp_path):
    store, _, _ = make_store_oc(tmp_path)
    machine = store.runners()[0]["machine"]
    assert set(machine) == set(P.MACHINE_FIELDS)
    assert all(v is None for v in machine.values())


# ---------------- ouverture de conversation : skill de départ et sous-agents
def test_start_skill_is_the_very_first_line(tmp_path):
    lines = P.build_session_preamble("claude-code", "/caveman ultra", True, resuming=False)
    assert lines[0] == "/caveman ultra", "une commande /skill doit ouvrir le prompt"
    out = P.apply_session_preamble("corrige le bug", lines)
    assert out.startswith("/caveman ultra\n\n")
    assert out.endswith("corrige le bug"), "le prompt de l'utilisateur n'est jamais modifié"


def test_start_skill_is_not_sent_to_runtimes_that_would_read_it_as_text(tmp_path):
    for runtime in ("opencode", "codex", "agy"):
        lines = P.build_session_preamble(runtime, "/caveman ultra", True)
        assert "/caveman ultra" not in lines


def test_subagents_are_authorised_only_where_documented(tmp_path):
    assert P.subagent_support("claude-code") == "native"
    assert P.subagent_support("opencode") == "native"
    # Jamais annoncé comme acquis quand ce n'est pas vérifié.
    assert P.subagent_support("codex") == "unknown"
    assert P.subagent_support("inconnu") == "unknown"
    assert any("sous-agents" in line for line in P.build_session_preamble("opencode", "", True))
    assert P.build_session_preamble("codex", "", True) == []
    assert P.build_session_preamble("claude-code", "", False) == []


def test_nothing_is_replayed_when_resuming_a_conversation(tmp_path):
    assert P.build_session_preamble("claude-code", "/caveman ultra", True, resuming=True) == []
    assert P.apply_session_preamble("suite", []) == "suite"
