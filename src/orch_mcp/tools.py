"""Outils MCP exposés à ChatGPT (via orch-gateway).

Aucun outil n'accepte de commande, de chemin ou d'exécutable : uniquement des
identifiants d'allowlist (runner, runtime, workspace), un mode borné et un prompt
traité comme une donnée. Ce serveur n'est PAS le MCP Astra.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

import anyio
from pydantic import Field

import orch_protocol as P
from orch_mcp.store import BrokerError, Store, follow_for_job

RuntimeT = Literal["claude-code", "claude-desktop", "codex", "agy", "opencode", "fake", "hermes"]

# Origine MCP réellement observable, posée par le middleware Starlette de
# orch_mcp.server (en-têtes relayés par la gateway : x-orch-mcp-acteur =
# client_id OAuth/CLI, x-orch-mcp-mode = cli/oauth). ContextVar (pas de global)
# : chaque requête HTTP porte la sienne ; hors HTTP (tests directs) => None.
# Jamais de secret ici (client_id + mode seuls, bornés).
import contextvars as _cv

_request_origin: _cv.ContextVar[tuple[str | None, str | None]] = _cv.ContextVar(
    "orch_request_origin", default=(None, None)
)


def set_request_origin(actor: str | None, mode: str | None) -> Any:
    """Pose l'origine pour la requête courante (middleware). Retourne le token
    de reset (le middleware le restaure après la requête)."""
    actor = (actor.strip()[:128] or None) if isinstance(actor, str) else None
    mode = (mode.strip()[:32] or None) if isinstance(mode, str) else None
    if mode not in (None, "cli", "oauth"):
        mode = None
    return _request_origin.set((actor, mode))


def _current_request_origin() -> tuple[str | None, str | None]:
    try:
        return _request_origin.get()
    except Exception:  # noqa: BLE001 - audit best-effort, jamais bloquant
        return None, None
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
            "(claude-code, claude-desktop [second compte, profil isolé], codex, agy, opencode) "
            "et le nombre de jobs actifs. À appeler avant agent_job_start."
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
            "Lance un agent IA existant (Claude Code, Claude Desktop via profil isolé claude-desktop, "
            "Codex, Antigravity/agy ou OpenCode) sur le PC "
            "personnel autorisé, dans un workspace allowlisté, avec un prompt. Retourne IMMÉDIATEMENT "
            "un job asynchrone (job_id, state=queued) SANS attendre la fin, PLUS un bloc de suivi "
            "machine-lisible (must_follow, terminal, should_continue, next_tool=agent_job_wait, "
            "wait_timeout_s, until=terminal, since_seq). CONTRAT DE SUIVI — sauf fire_and_forget=true "
            "explicitement demandé par l'utilisateur (« lance et oublie ») : vous DEVEZ rester dans le "
            "MÊME tour, appeler agent_job_wait (timeout 25 s, since_seq fourni) puis RAPPELER "
            "agent_job_wait après CHAQUE retour non terminal. Un woke_by=timeout pendant que le job "
            "continue N'EST PAS une fin et N'EST PAS une raison de répondre : rappelez immédiatement. "
            "Ne répondez à l'utilisateur qu'une fois terminal=true (puis agent_job_get / "
            "agent_job_output / agent_job_events pour le résultat). Arrêts intermédiaires autorisés "
            "UNIQUEMENT sur : question waiting_for_user ouverte (agent_question_list), entrée "
            "utilisateur réellement requise, ou job failed/timeout/cancelled/lost à remonter "
            "explicitement. Ne dites JAMAIS « lancé, repromptez-moi quand il a fini » : l'expérience "
            "normale est un seul message utilisateur puis une seule réponse finale avec le résultat. "
            "Ce n'est pas un shell : aucune commande n'est exécutée, le prompt est transmis tel quel "
            "à l'agent. Fournissez idempotency_key pour qu'un retry ne crée pas un second job. "
            + STATE_HELP
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
        fire_and_forget: Annotated[bool, Field(description="true = l'utilisateur demande explicitement à ne PAS attendre le résultat (lance-et-oublie) ; le contrat de suivi ne s'applique pas. Défaut false.")] = False,
        source_label: Annotated[str | None, Field(description="Libellé de corrélation optionnel (ex. titre/ID de conversation transmis par le client ; stocké tel quel, borné). Absent = non retrouvable après coup.")] = None,
        conversation_id: Annotated[str | None, Field(description="ID de conversation/chat optionnel si le client le transmet (jamais obligatoire, jamais inventé).")] = None,
    ) -> dict:
        try:
            _actor, _mode = _current_request_origin()
            job, created = await anyio.to_thread.run_sync(
                lambda: store.create_job(
                    runner_id, runtime, workspace_id, prompt, mode, timeout_s, idempotency_key,
                    origin_actor=_actor, origin_mode=_mode,
                    origin_label=source_label, conversation_id=conversation_id,
                )
            )
        except BrokerError as exc:
            return _err(exc)
        runner = next((r for r in await anyio.to_thread.run_sync(store.runners) if r["id"] == runner_id), None)
        seq = await anyio.to_thread.run_sync(store._last_seq, job["job_id"])
        follow = follow_for_job(job["state"], seq)
        if fire_and_forget:
            follow["must_follow"] = False
            follow["should_continue"] = False
        out = {"job_id": job["job_id"], "state": job["state"], "created": created,
               "follow_up": follow, "terminal": follow["terminal"],
               "should_continue": follow["should_continue"], "next_tool": follow["next_tool"],
               "since_seq": follow["since_seq"], "until": follow["until"]}
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
            "fond de tâche : ne se réveille que pendant un tour ChatGPT actif. Retour machine-lisible : "
            "state, woke_by, terminal (bool), should_continue/must_follow (= non terminal), next_tool "
            "(agent_job_wait tant que non terminal, agent_job_get sinon), since_seq/last_event_seq "
            "(curseur), until=terminal. RÈGLE : si terminal=false — Y COMPRIS woke_by=timeout — vous "
            "DEVEZ rappeler agent_job_wait avec since_seq=last_event_seq dans le MÊME tour, sans "
            "répondre à l'utilisateur. Un timeout pendant que le process continue n'est jamais une fin. "
            "Ne répondez qu'après terminal=true (puis agent_job_get / agent_job_output / "
            "agent_job_events). Seuls vrais arrêts : question waiting_for_user ouverte "
            "(agent_question_list), entrée utilisateur réellement requise, ou job "
            "failed/timeout/cancelled/lost à remonter explicitement."
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
            "première tentative. Retourne la mission PLUS un bloc de suivi machine-lisible "
            "(must_follow, terminal, should_continue, next_tool=agent_mission_wait, since_seq, "
            "until=terminal) sur la tentative courante. CONTRAT DE SUIVI — sauf fire_and_forget=true "
            "explicite : restez dans le MÊME tour, appelez agent_mission_wait (ou agent_job_wait sur "
            "current_job_id) puis RAPPELER après chaque retour non terminal ; un timeout pendant que "
            "la tentative continue impose de rappeler immédiatement, jamais de répondre. Rappel : "
            "`completed` (exit 0) ne valide jamais la mission : le job terminal exit 0 passe la mission "
            "en needs_validation (inspectez agent_job_get/output/events puis agent_mission_validate), "
            "sinon incomplete. Aucun retry automatique. Validez avec agent_mission_validate ; ne "
            "confondez jamais completed avec validated."
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
        fire_and_forget: Annotated[bool, Field(description="true = l'utilisateur demande explicitement à ne PAS attendre le résultat ; le contrat de suivi ne s'applique pas. Défaut false.")] = False,
        source_label: Annotated[str | None, Field(description="Libellé de corrélation optionnel (stocké, borné).")] = None,
        conversation_id: Annotated[str | None, Field(description="ID de conversation/chat optionnel si le client le transmet (jamais obligatoire).")] = None,
    ) -> dict:
        try:
            _actor, _mode = _current_request_origin()
            m = await anyio.to_thread.run_sync(
                lambda: store.create_mission(
                    objective, acceptance_criteria, max_attempts, runner_id, runtime,
                    workspace_id, mode, timeout_s, prompt, idempotency_key,
                    origin_actor=_actor, origin_mode=_mode,
                    origin_label=source_label, conversation_id=conversation_id,
                )
            )
        except BrokerError as exc:
            return _err(exc)
        if "error" not in m and m.get("current_job_id"):
            seq = await anyio.to_thread.run_sync(store._last_seq, m["current_job_id"])
            follow = follow_for_job(m.get("current_job", {}).get("state", "queued"), seq)
            if fire_and_forget:
                follow["must_follow"] = False
                follow["should_continue"] = False
            follow["next_tool"] = "agent_mission_wait" if follow["should_continue"] else follow["next_tool"]
            m["follow_up"] = follow
            m["should_continue"] = follow["should_continue"]
            m["next_tool"] = follow["next_tool"]
            m["since_seq"] = follow["since_seq"]
            m["until"] = follow["until"]
        return m

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
        name="agent_mission_wait",
        description=(
            "Attend la tentative COURANTE d'une mission (borné ≤ 60 s, même sémantique que "
            "agent_job_wait, pas un fond de tâche : réveil pendant un tour ChatGPT actif). Retour "
            "machine-lisible : mission_state, job_state, woke_by, terminal (tentative figée ou non), "
            "should_continue/must_follow, next_tool (agent_mission_wait tant que executing non "
            "terminal ; agent_mission_validate quand needs_validation/incomplete/blocked — inspectez "
            "d'abord agent_job_get / agent_job_output / agent_job_events puis validez : completed exit "
            "0 ≠ validated), since_seq/last_event_seq (curseur), until=terminal. RÈGLE : tant que "
            "should_continue=true — Y COMPRIS woke_by=timeout — RAPPELER agent_mission_wait avec "
            "since_seq=last_event_seq dans le MÊME tour, sans répondre à l'utilisateur. Ne répondez "
            "qu'après mission terminale validée (ou blocage réel : waiting_for_user, entrée requise, "
            "failed/timeout/cancelled/lost à remonter explicitement)."
        ),
    )
    async def agent_mission_wait(
        mission_id: Annotated[str, Field(description="Identifiant de la mission.")],
        timeout_s: Annotated[float, Field(description="Attente max en secondes (0..60, défaut 25).")] = 25,
        since_seq: Annotated[int, Field(description="Seq d'événement du job courant déjà connu (-1 = aucun).")] = -1,
    ) -> dict:
        res = await anyio.to_thread.run_sync(store.wait_for_mission, mission_id, since_seq, timeout_s)
        return res if res is not None else {"error": "unknown_mission", "mission_id": mission_id}

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
                        "agent_mission_wait",
                        "infra_alert_list", "infra_alert_get", "agent_question_list", "agent_question_get"})
TOOLS_WRITE = frozenset({"agent_job_start", "agent_job_cancel",
                         "agent_mission_create", "agent_mission_retry", "agent_mission_validate",
                         "agent_question_answer"})
_ = P
