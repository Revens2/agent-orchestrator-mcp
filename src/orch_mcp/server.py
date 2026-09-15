"""Point d'entrée orch-mcp (`python -m orch_mcp.server`).

Un seul processus, boucle locale uniquement (ORCH_UPSTREAM_PORT, défaut 8802) :
- `/mcp` : MCP streamable-http stateless, joint seulement par orch-gateway (OAuth) ;
- `/runner/v1/*` : API runner, jointe seulement par nginx sur l'IP NetBird ;
- `/health` : santé (DB + reaper).
Un reaper interne expire les bails, applique les timeouts durs et purge la rétention.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from importlib.metadata import version as pkg_version
from pathlib import Path

import anyio
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Mount, Route

from orch_mcp import tools
from orch_mcp.runner_api import RunnerAuth, build_routes, parse_tokens
from orch_mcp.store import Store

log = logging.getLogger("orch_mcp")

INSTRUCTIONS = (
    "Orchestrateur d'agents IA personnels (≠ MCP Astra). Lance de vrais agents existants "
    "(Claude Code, Codex, Antigravity, OpenCode, Claude Desktop — ce dernier read_only uniquement) "
    "sur le PC Windows autorisé, dans des workspaces "
    "allowlistés. Flux : agent_runner_list -> agent_workspace_list -> agent_job_start (asynchrone, "
    "contrat de suivi : rappeler agent_job_wait jusqu'à terminal=true dans le même tour) "
    "-> agent_job_get/output/events jusqu'à un état terminal. Missions : agent_mission_create -> "
    "agent_mission_wait en boucle -> agent_mission_validate (completed exit 0 ≠ validated). "
    "Un timeout de wait non terminal impose de rappeler, jamais de répondre. Pas de shell distant : seul un prompt est transmis."
)


def build_app(store: Store, runner_auth: RunnerAuth, reaper_interval_s: float = 5.0):
    from mcp.server.mcpserver import MCPServer

    mcp = MCPServer("agent-orchestrator", version=pkg_version("mcp"), instructions=INSTRUCTIONS)
    tools.register(mcp, store)
    mcp_app = mcp.streamable_http_app(stateless_http=True)
    state = {"last_reap": 0.0, "last_purge": 0.0}

    async def health(_: Request) -> JSONResponse:
        ok = True
        try:
            await anyio.to_thread.run_sync(lambda: store.list_jobs(limit=1))
        except Exception:  # noqa: BLE001
            ok = False
        reap_age = time.time() - state["last_reap"] if state["last_reap"] else None
        ok = ok and reap_age is not None and reap_age < 60
        return JSONResponse(
            {"status": "ok" if ok else "degraded", "service": "orch-mcp", "reaper_age_s": reap_age},
            status_code=200 if ok else 503,
        )

    async def not_found(_: Request) -> PlainTextResponse:
        return PlainTextResponse("Not Found", status_code=404)

    async def reaper() -> None:
        while True:
            try:
                stats = await anyio.to_thread.run_sync(store.reap)
                if any(stats.values()):
                    log.info("reaper %s", stats)
                state["last_reap"] = time.time()
                if time.time() - state["last_purge"] > 3600:
                    log.info("purge %s", await anyio.to_thread.run_sync(store.purge))
                    state["last_purge"] = time.time()
            except Exception:
                log.exception("reaper_error")
            await asyncio.sleep(reaper_interval_s)

    @contextlib.asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(reaper())
        async with mcp_app.router.lifespan_context(app):
            yield
        task.cancel()

    routes = [
        Route("/health", health, methods=["GET"]),
        *build_routes(store, runner_auth),
        Mount("/", app=mcp_app),
    ]
    return Starlette(routes=routes, lifespan=lifespan)


def main() -> None:
    logging.basicConfig(level=os.environ.get("ORCH_LOG_LEVEL", "INFO"), format="%(levelname)s %(name)s %(message)s")
    data_dir = Path(os.environ.get("ORCH_DATA_DIR", "/srv/orch/data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    store = Store(data_dir / "orch.db")
    auth = RunnerAuth(
        parse_tokens(os.environ.get("ORCH_RUNNER_TOKENS", "")),
        os.environ.get("ORCH_RUNNER_CIDRS", "10.200.0.0/16").split(","),
    )
    uvicorn.run(
        build_app(store, auth),
        host="127.0.0.1",
        port=int(os.environ.get("ORCH_UPSTREAM_PORT", "8802")),
        log_level=os.environ.get("ORCH_LOG_LEVEL", "info").lower(),
        timeout_graceful_shutdown=5,
    )


if __name__ == "__main__":
    main()
