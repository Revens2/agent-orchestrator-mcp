"""Outils MCP exposés à ChatGPT (via orch-gateway).

Aucun outil n'accepte de commande, de chemin ou d'exécutable : uniquement des
identifiants d'allowlist (runner, runtime, workspace), un mode borné et un prompt
traité comme une donnée. Ce serveur n'est PAS le MCP Astra.
"""

from __future__ import annotations

from typing import Annotated, Literal

import anyio
from pydantic import Field

import orch_protocol as P
from orch_mcp.store import BrokerError, Store

RuntimeT = Literal["claude-code", "codex", "agy", "opencode", "fake"]
ModeT = Literal["read_only", "workspace_write"]
StateT = Literal["queued", "claimed", "starting", "running", "completed", "failed", "timeout", "cancelled", "lost"]
AlertSourceT = Literal["etude", "nexus"]
AlertSeverityT = Literal["info", "warning", "critical"]
AlertStateT = Literal["active", "acked", "resolved"]

STATE_HELP = (
    "États : queued (attend le PC ; reste en file si le PC est offline), claimed (pris, pas lancé), "
    "starting, running, completed (processus terminé avec exit 0 — PAS une mission validée), "
    "failed, timeout, cancelled, lost (PC perdu après lancement : "
    "issue inconnue, jamais relancé automatiquement). "
    "Seule une mission `validated` (agent_mission_validate) atteste un objectif atteint."
)


def _err(exc: BrokerError) -> dict:
    return {"error": exc.code, "message": exc.message}


def register(mcp, store: Store) -> None:
    @mcp.tool(
        name="agent_runner_list",
        description=(
            "Liste les PC runners de l'orchestrateur d'agents IA personnel, leur présence réelle "
            "(online si heartbeat < 30 s, sinon offline), les runtimes d'agents disponibles "
            "(claude-code, codex, agy, opencode) et le nombre de jobs actifs. À appeler avant "
            "agent_job_start."
        ),
    )
    async def agent_runner_list() -> dict:
        runners = await anyio.to_thread.run_sync(store.runners)
        for r in runners:
            r.pop("workspaces", None)
        return {"runners": runners}

    @mcp.tool(
        name="agent_workspace_list",
        description=(
            "Liste les workspaces (projets locaux) autorisés sur un runner, par identifiant, avec les "
            "modes permis (read_only, workspace_write). Les chemins réels restent sur le PC : "
            "utilisez uniquement workspace_id."
        ),
    )
    async def agent_workspace_list(
        runner_id: Annotated[str | None, Field(description="Runner ; défaut = tous.")] = None,
    ) -> dict:
        runners = await anyio.to_thread.run_sync(store.runners)
        return {
            "workspaces": [
                {"runner_id": r["id"], "runner_status": r["status"], **w}
                for r in runners
                if runner_id in (None, r["id"])
                for w in r["workspaces"]
            ]
        }

    @mcp.tool(
        name="agent_job_start",
        description=(
            "Lance un agent IA existant (Claude Code, Codex, Antigravity/agy ou OpenCode) sur le PC "
            "personnel autorisé, dans un workspace allowlisté, avec un prompt. Retourne IMMÉDIATEMENT "
            "un job asynchrone (job_id, state=queued) sans attendre la fin : suivez-le ensuite avec "
            "agent_job_get. Ce n'est pas un shell : aucune commande n'est exécutée, le prompt est "
            "transmis tel quel à l'agent. Fournissez idempotency_key pour qu'un retry ne crée pas un "
            "second job. " + STATE_HELP
        ),
    )
    async def agent_job_start(
        runner_id: Annotated[str, Field(description="Identifiant du runner (voir agent_runner_list).")],
        runtime: Annotated[RuntimeT, Field(description="Runtime d'agent à utiliser.")],
        workspace_id: Annotated[str, Field(description="Identifiant de workspace allowlisté (pas un chemin).")],
        prompt: Annotated[str, Field(description="Mission donnée à l'agent (texte libre, transmis intact).")],
        mode: Annotated[ModeT, Field(description="read_only = analyse sans écriture ; workspace_write = peut modifier le workspace.")] = "read_only",
        timeout_s: Annotated[int | None, Field(description="Durée max en secondes (30..14400, défaut 3600).")] = None,
        idempotency_key: Annotated[str | None, Field(description="Clé unique (8..128 car.) pour dédupliquer les retries.")] = None,
    ) -> dict:
        try:
            job, created = await anyio.to_thread.run_sync(
                lambda: store.create_job(runner_id, runtime, workspace_id, prompt, mode, timeout_s, idempotency_key)
            )
        except BrokerError as exc:
            return _err(exc)
        runner = next((r for r in await anyio.to_thread.run_sync(store.runners) if r["id"] == runner_id), None)
        out = {"job_id": job["job_id"], "state": job["state"], "created": created}
        if runner and runner["status"] != "online":
            out["warning"] = "runner offline : le job reste queued jusqu'à sa reconnexion (annulable)."
        return out

    @mcp.tool(
        name="agent_job_get",
        description=(
            "État structuré d'exécution d'un job : state, timestamps, durée, exit_code, dernière "
            "activité, fin de sortie bornée (output_tail), result_summary et erreur, PLUS supervision "
            "riche quand observable : heartbeat runner (runner_heartbeat_age_s), processus "
            "(process_alive, pid, child_process_count, telemetry_age_s — null + jamais observé "
            "= runner sans instrumentation, pas une panne), progression (output_chars/chunks, "
            "last_output_at/last_event_at), activité courante (current_activity/current_tool) et "
            "execution_health (healthy | idle | suspected_stall | stalled | runner_disconnected | "
            "process_dead) avec couches séparées broker_health/runner_health/runtime_process_health. "
            "Champ null = non observé (jamais inventé). Ne renvoie jamais le transcript complet "
            "(voir agent_job_output) ni secrets/commandes brutes. " + STATE_HELP
        ),
    )
    async def agent_job_get(
        job_id: Annotated[str, Field(description="Identifiant du job.")],
        tail_chars: Annotated[int, Field(description="Taille de output_tail (0..8000).")] = 1500,
    ) -> dict:
        job = await anyio.to_thread.run_sync(store.get_job, job_id, tail_chars)
        return job if job is not None else {"error": "unknown_job", "job_id": job_id}

    @mcp.tool(
        name="agent_job_output",
        description=(
            "Lit la sortie d'un job d'agent par pages (cursor, limit ≤ 20000 caractères). "
            "Utiliser next_cursor pour continuer ; null = fin actuelle."
        ),
    )
    async def agent_job_output(
        job_id: Annotated[str, Field(description="Identifiant du job.")],
        cursor: Annotated[int, Field(description="Curseur (0 = début).")] = 0,
        limit: Annotated[int, Field(description="Caractères max (1..20000).")] = 8000,
    ) -> dict:
        page = await anyio.to_thread.run_sync(store.read_output, job_id, cursor, limit)
        return page if page is not None else {"error": "unknown_job", "job_id": job_id}

    @mcp.tool(
        name="agent_job_cancel",
        description=(
            "Annule un job d'agent (idempotent). Résultats : cancelled (était en file), "
            "cancel_requested (le PC va tuer tout l'arbre de processus ; suivre avec agent_job_get), "
            "already_finished, unknown_job."
        ),
    )
    async def agent_job_cancel(job_id: Annotated[str, Field(description="Identifiant du job.")]) -> dict:
        return await anyio.to_thread.run_sync(store.cancel, job_id)

    @mcp.tool(
        name="agent_job_list",
        description="Liste compacte des jobs d'agents récents, filtrable par état, runtime, workspace.",
    )
    async def agent_job_list(
        state: Annotated[StateT | None, Field(description="Filtre d'état.")] = None,
        runtime: Annotated[RuntimeT | None, Field(description="Filtre runtime.")] = None,
        workspace_id: Annotated[str | None, Field(description="Filtre workspace.")] = None,
        limit: Annotated[int, Field(description="1..100, défaut 20.")] = 20,
    ) -> dict:
        jobs = await anyio.to_thread.run_sync(lambda: store.list_jobs(state, runtime, workspace_id, None, limit))
        return {"jobs": jobs, "count": len(jobs)}

    @mcp.tool(
        name="agent_job_events",
        description=(
            "Journal d'événements structuré et borné d'un job (job_claimed, runtime_spawned, "
            "output_progress, activity, process_exit, runner_disconnect, lease_expired, "
            "cancel_requested, suspected_stall, stalled, …), ordonné et paginé. Pas de transcript."
        ),
    )
    async def agent_job_events(
        job_id: Annotated[str, Field(description="Identifiant du job.")],
        after_seq: Annotated[int, Field(description="Ne renvoyer que seq > after_seq (-1 = depuis le début).")] = -1,
        limit: Annotated[int, Field(description="Événements max (1..200, défaut 50).")] = 50,
    ) -> dict:
        page = await anyio.to_thread.run_sync(store.read_events, job_id, after_seq, limit)
        return page if page is not None else {"error": "unknown_job", "job_id": job_id}

    @mcp.tool(
        name="agent_runner_inspect",
        description=(
            "Snapshot compact runner/environnement : version runner, runtimes (versions/capacités), "
            "workspaces allowlistés, état réseau/broker (last_seen), jobs actifs enrichis, et git par "
            "workspace (branch/HEAD/dirty) si observé. Jamais de secrets ni dump d'environnement."
        ),
    )
    async def agent_runner_inspect(
        runner_id: Annotated[str, Field(description="Identifiant du runner (voir agent_runner_list).")],
    ) -> dict:
        snap = await anyio.to_thread.run_sync(store.runner_inspect, runner_id)
        return snap if snap is not None else {"error": "unknown_runner", "runner_id": runner_id}

    @mcp.tool(
        name="agent_job_wait",
        description=(
            "Attend un changement significatif d'un job (état, sortie, événement, terminal) jusqu'au "
            "timeout borné (≤ 60 s). Évite le polling agressif pendant un tour actif. N'est PAS un "
            "fond de tâche : ne se réveille que pendant un tour ChatGPT actif."
        ),
    )
    async def agent_job_wait(
        job_id: Annotated[str, Field(description="Identifiant du job.")],
        timeout_s: Annotated[float, Field(description="Attente max en secondes (0..60, défaut 25).")] = 25,
        since_seq: Annotated[int, Field(description="Seq d'événement déjà connu (-1 = aucun).")] = -1,
    ) -> dict:
        res = await anyio.to_thread.run_sync(store.wait_for_change, job_id, since_seq, timeout_s)
        return res if res is not None else {"error": "unknown_job", "job_id": job_id}

    @mcp.tool(
        name="agent_mission_create",
        description=(
            "Crée une MISSION (objectif + critères d'acceptation) au-dessus des jobs et démarre sa "
            "première tentative. Rappel : `completed` (exit 0) ne valide jamais la mission : le job "
            "terminal exit 0 passe la mission en needs_validation, sinon incomplete. Aucun retry "
            "automatique. Validez avec agent_mission_validate."
        ),
    )
    async def agent_mission_create(
        objective: Annotated[str, Field(description="Objectif de la mission (1..4000 car.).")],
        acceptance_criteria: Annotated[list[str], Field(description="Critères d'acceptation (1..20, non vides).")],
        runner_id: Annotated[str, Field(description="Identifiant du runner.")],
        runtime: Annotated[RuntimeT, Field(description="Runtime d'agent à utiliser.")],
        workspace_id: Annotated[str, Field(description="Identifiant de workspace allowlisté.")],
        mode: Annotated[ModeT, Field(description="read_only ou workspace_write.")] = "read_only",
        max_attempts: Annotated[int, Field(description="Tentatives max (1..5, défaut 2).")] = 2,
        timeout_s: Annotated[int | None, Field(description="Durée max par tentative (30..14400).")] = None,
        prompt: Annotated[str | None, Field(description="Prompt de la 1re tentative (défaut = objectif).")] = None,
        idempotency_key: Annotated[str | None, Field(description="Clé unique (8..128 car.).")] = None,
    ) -> dict:
        try:
            return await anyio.to_thread.run_sync(
                lambda: store.create_mission(
                    objective, acceptance_criteria, max_attempts, runner_id, runtime,
                    workspace_id, mode, timeout_s, prompt, idempotency_key,
                )
            )
        except BrokerError as exc:
            return _err(exc)

    @mcp.tool(
        name="agent_mission_get",
        description="État d'une mission : objectif, critères, tentatives, job courant, validation.",
    )
    async def agent_mission_get(
        mission_id: Annotated[str, Field(description="Identifiant de la mission.")],
    ) -> dict:
        m = await anyio.to_thread.run_sync(store.get_mission, mission_id)
        return m if m is not None else {"error": "unknown_mission", "mission_id": mission_id}

    @mcp.tool(
        name="agent_mission_retry",
        description=(
            "Nouvelle tentative de la MÊME mission (nouveau job). Exige : mission needs_validation ou "
            "incomplete, tentative précédente terminale, attempts < max_attempts. Décision explicite "
            "après examen du journal — jamais de relance aveugle de mission d'écriture."
        ),
    )
    async def agent_mission_retry(
        mission_id: Annotated[str, Field(description="Identifiant de la mission.")],
        prompt: Annotated[str | None, Field(description="Prompt ajusté (défaut = objectif).")] = None,
    ) -> dict:
        try:
            return await anyio.to_thread.run_sync(store.retry_mission, mission_id, prompt)
        except BrokerError as exc:
            return _err(exc)

    @mcp.tool(
        name="agent_mission_validate",
        description=(
            "Validation humaine d'une mission (validated | incomplete | blocked | failed). Seule elle "
            "atteste un objectif atteint : le succès du processus (exit 0) ne suffit pas."
        ),
    )
    async def agent_mission_validate(
        mission_id: Annotated[str, Field(description="Identifiant de la mission.")],
        verdict: Annotated[str, Field(description="validated | incomplete | blocked | failed.")],
        note: Annotated[str | None, Field(description="Note de validation (bornée, redactée).")] = None,
    ) -> dict:
        try:
            return await anyio.to_thread.run_sync(store.validate_mission, mission_id, verdict, note)
        except BrokerError as exc:
            return _err(exc)


    @mcp.tool(
        name="infra_alert_list",
        description=(
            "Alertes infra sortantes unifiées [ETUDE]/[NEXUS] (persistence locale normalisée, "
            "pas l'historique Telegram) : service, sévérité, état, empreinte de déduplication, "
            "compteur d'occurrences. Vue compacte SANS le détail complet (voir infra_alert_get). "
            "Lecture seule."
        ),
    )
    async def infra_alert_list(
        source: Annotated[AlertSourceT | None, Field(description="Filtre source : etude | nexus (défaut = toutes).")] = None,
        severity: Annotated[AlertSeverityT | None, Field(description="Filtre sévérité.")] = None,
        state: Annotated[AlertStateT | None, Field(description="Filtre état (défaut = tous).")] = None,
        since: Annotated[float | None, Field(description="Alertes vues après ce timestamp Unix.")] = None,
        until: Annotated[float | None, Field(description="Alertes vues avant ce timestamp Unix.")] = None,
        limit: Annotated[int, Field(description="1..100, défaut 20.")] = 20,
    ) -> dict:
        try:
            return await anyio.to_thread.run_sync(
                lambda: store.list_alerts(source, severity, state, since, until, limit)
            )
        except BrokerError as exc:
            return _err(exc)

    @mcp.tool(
        name="infra_alert_get",
        description=(
            "Détail borné d'une alerte infra : titre, détail (erreur, contexte, redacté), "
            "service, source, sévérité, état, empreinte, occurrences, timestamps. Lecture seule."
        ),
    )
    async def infra_alert_get(
        alert_id: Annotated[str, Field(description="Identifiant de l'alerte (voir infra_alert_list).")],
    ) -> dict:
        alert = await anyio.to_thread.run_sync(store.get_alert, alert_id)
        return alert if alert is not None else {"error": "unknown_alert", "alert_id": alert_id}

    @mcp.tool(
        name="agent_question_list",
        description=(
            "Questions en attente de l'utilisateur (waiting_for_user explicite, jamais "
            "un `?` deviné) : titre de la conversation, runtime/session d'origine, question "
            "bornée, choix proposés, état, notification. Inbox corrélée : la réponse donnée "
            "via agent_question_answer (ou iMessage/Photon) est routée à LA session émettrice. "
            "Lecture seule."
        ),
    )
    async def agent_question_list(
        status: Annotated[str | None, Field(description="Filtre : open | answered | expired.")] = None,
        origin: Annotated[str | None, Field(description="Filtre : agent | chatgpt-web | mission.")] = None,
        limit: Annotated[int, Field(description="1..100, défaut 20.")] = 20,
    ) -> dict:
        try:
            return await anyio.to_thread.run_sync(lambda: store.list_questions(status, origin, limit))
        except BrokerError as exc:
            return _err(exc)

    @mcp.tool(
        name="agent_question_get",
        description=(
            "Détail d'une question en attente + réponse éventuelle (answer, answer_from, "
            "answered_at). Lecture seule."
        ),
    )
    async def agent_question_get(
        question_id: Annotated[str, Field(description="Correlation_id court (voir agent_question_list).")],
    ) -> dict:
        q = await anyio.to_thread.run_sync(store.get_question, question_id)
        return q if q is not None else {"error": "unknown_question", "question_id": question_id}

    @mcp.tool(
        name="agent_question_answer",
        description=(
            "Répond à UNE question en attente (single-use : replay refusé, expirée refusée). "
            "Réponse = choix proposé ou texte court (≤500 car.), jamais une commande shell : "
            "elle est stockée et routée à la session émettrice (session_ref), qui la lira "
            "à sa prochaine activité. Pour ChatGPT Web : c'est l'inbox corrélée (pas "
            "d'injection dans la conversation Web, techniquement non supportée)."
        ),
    )
    async def agent_question_answer(
        question_id: Annotated[str, Field(description="Correlation_id court (voir agent_question_list).")],
        answer: Annotated[str, Field(description="Choix (ex. '2') ou texte court (≤500 car.).")] = "",
        answer_from: Annotated[str, Field(description="Expéditeur (ex. 'chatgpt-web', 'imessage').")] = "chatgpt-web",
    ) -> dict:
        try:
            return await anyio.to_thread.run_sync(lambda: store.answer_question(question_id, answer, answer_from))
        except BrokerError as exc:
            return _err(exc)


TOOLS_READ = frozenset({"agent_runner_list", "agent_workspace_list", "agent_job_get", "agent_job_output", "agent_job_list",
                        "agent_job_events", "agent_runner_inspect", "agent_job_wait", "agent_mission_get",
                        "infra_alert_list", "infra_alert_get", "agent_question_list", "agent_question_get"})
TOOLS_WRITE = frozenset({"agent_job_start", "agent_job_cancel",
                         "agent_mission_create", "agent_mission_retry", "agent_mission_validate",
                         "agent_question_answer"})
_ = P
