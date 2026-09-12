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

STATE_HELP = (
    "États : queued (attend le PC ; reste en file si le PC est offline), claimed (pris, pas lancé), "
    "starting, running, completed (exit 0), failed, timeout, cancelled, lost (PC perdu après lancement : "
    "issue inconnue, jamais relancé automatiquement)."
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
            "État compact d'un job d'agent : state, timestamps, durée, exit_code, dernière activité, "
            "fin de sortie bornée (output_tail), result_summary et erreur. Ne renvoie jamais le "
            "transcript complet (voir agent_job_output). " + STATE_HELP
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


TOOLS_READ = frozenset({"agent_runner_list", "agent_workspace_list", "agent_job_get", "agent_job_output", "agent_job_list"})
TOOLS_WRITE = frozenset({"agent_job_start", "agent_job_cancel"})
_ = P
