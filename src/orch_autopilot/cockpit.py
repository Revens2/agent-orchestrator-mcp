"""Cockpit MCP pour ChatGPT Web : soumettre, suivre, piloter et reprendre un objectif par son id.

Exposé via une instance dédiée d'orch-gateway (politique `autopilot`, OAuth). Aucun outil
n'accepte de runtime, de chemin ou de commande. `autopilot_approve` ne consomme un
consentement qu'avec le code reçu HORS BANDE : un LLM qui lit le statut ne peut pas approuver.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

import anyio
from pydantic import Field

from orch_autopilot.supervisor import AutopilotError, Supervisor

INSTRUCTIONS = (
    "Autopilot personnel (plan de contrôle durable sur VPS). Un objectif = des étapes déclarées "
    "(read | local_write | decision) exécutées par OpenCode uniquement, dans des workspaces allowlistés. "
    "Le superviseur continue seul après la fermeture du chat : retrouver l'état avec autopilot_handoff(objective_id). "
    "completed d'un job n'est jamais une preuve : DONE exige le contrat de sortie. Publication, DM, paiement, "
    "push et effets externes sont interdits en V1. Les approbations exigent un code hors bande saisi par l'humain."
)


# Classification miroir de orch_gateway.politique (vérifiée par test).
TOOLS_READ = frozenset({"autopilot_status", "autopilot_list", "autopilot_handoff"})
TOOLS_WRITE = frozenset({"autopilot_submit", "autopilot_pause", "autopilot_resume", "autopilot_cancel", "autopilot_approve"})


def _err(exc: AutopilotError) -> dict:
    return {"error": exc.code, "message": exc.message}


def register(mcp, sup: Supervisor) -> None:
    async def run(fn, *args) -> dict[str, Any]:
        try:
            return await anyio.to_thread.run_sync(fn, *args)
        except AutopilotError as exc:
            return _err(exc)

    @mcp.tool(
        name="autopilot_submit",
        description=(
            "Soumet un objectif durable. spec = {title, goal, steps:[{key, kind: read|local_write|decision, "
            "workspace_id, prompt, depends_on:[keys], timeout_s (60..7200, défaut 1500), "
            "expect:{markers:[...], report_fields:[...]}, structural:bool}], budget:{max_jobs,...}}. "
            "client_key (8..128) rend la soumission idempotente : même clé = même objective_id."
        ),
    )
    async def autopilot_submit(
        spec: Annotated[dict, Field(description="Spécification de l'objectif (voir description).")],
        client_key: Annotated[str | None, Field(description="Clé d'idempotence choisie par le client.")] = None,
    ) -> dict:
        if client_key is not None and not 8 <= len(client_key) <= 128:
            return {"error": "invalid_client_key", "message": "8..128 caractères"}
        return await run(sup.submit, spec, client_key)

    @mcp.tool(name="autopilot_status", description="État complet d'un objectif : étapes, tentatives, preuves, budget, etag.")
    async def autopilot_status(objective_id: Annotated[str, Field(description="Identifiant d'objectif.")]) -> dict:
        return await run(sup.objective, objective_id)

    @mcp.tool(name="autopilot_list", description="Liste les objectifs récents (filtre d'état optionnel).")
    async def autopilot_list(
        state: Annotated[Literal["ACTIVE", "PAUSED", "DONE", "FAILED", "CANCELLED"] | None, Field(description="Filtre.")] = None,
        limit: Annotated[int, Field(description="1..100")] = 20,
    ) -> dict:
        return {"objectives": await anyio.to_thread.run_sync(sup.list_objectives, state, limit)}

    @mcp.tool(
        name="autopilot_pause",
        description="Met un objectif en pause (aucun nouvel envoi). objective_id='*' active le kill switch global.",
    )
    async def autopilot_pause(objective_id: Annotated[str, Field(description="Identifiant ou '*'.")]) -> dict:
        if objective_id == "*":
            return await run(sup.set_kill_switch, True, "cockpit")
        return await run(sup.set_objective_state, objective_id, "pause")

    @mcp.tool(
        name="autopilot_resume",
        description="Reprend un objectif en pause. Le kill switch global ne se désactive PAS depuis le cockpit (CLI VPS).",
    )
    async def autopilot_resume(objective_id: Annotated[str, Field(description="Identifiant d'objectif.")]) -> dict:
        if objective_id == "*":
            return {"error": "forbidden", "message": "désactivation du kill switch : CLI opérateur sur le VPS uniquement"}
        return await run(sup.set_objective_state, objective_id, "resume")

    @mcp.tool(name="autopilot_cancel", description="Annule un objectif et demande l'arrêt de ses jobs en cours.")
    async def autopilot_cancel(objective_id: Annotated[str, Field(description="Identifiant d'objectif.")]) -> dict:
        return await run(sup.set_objective_state, objective_id, "cancel")

    @mcp.tool(
        name="autopilot_approve",
        description=(
            "Approbation en 2 temps. (1) step_id + resolution : crée la demande liée au hash exact de l'action ; "
            "un code est envoyé HORS BANDE à l'humain. (2) action_hash + code saisi par l'humain : applique. "
            "Ne jamais deviner ni inventer le code ; le demander à l'utilisateur."
        ),
    )
    async def autopilot_approve(
        step_id: Annotated[str | None, Field(description="Étape à débloquer (phase 1).")] = None,
        resolution: Annotated[
            Literal["approve_gate", "reject", "retry", "mark_done", "mark_failed", "approve"] | None,
            Field(description="Résolution demandée (phase 1)."),
        ] = None,
        action_hash: Annotated[str | None, Field(description="Hash renvoyé par la phase 1 (phase 2).")] = None,
        code: Annotated[str | None, Field(description="Code hors bande saisi par l'humain (phase 2).")] = None,
    ) -> dict:
        if action_hash and code:
            return await run(sup.approve, action_hash, code)
        if step_id and resolution:
            return await run(sup.request_approval, step_id, resolution)
        return {"error": "invalid_arguments", "message": "phase 1 : step_id+resolution ; phase 2 : action_hash+code"}

    @mcp.tool(
        name="autopilot_handoff",
        description=(
            "Reprise sans transcript (nouveau chat) : faits attestés, hypothèses (rapports d'agents), blocages, "
            "prochaine action, etag. Ne contient jamais de secret ni de transcript complet."
        ),
    )
    async def autopilot_handoff(objective_id: Annotated[str, Field(description="Identifiant d'objectif.")]) -> dict:
        return await run(sup.handoff, objective_id)
