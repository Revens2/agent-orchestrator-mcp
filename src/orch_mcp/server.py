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
    "(Claude Code, Claude Desktop via profil isolé claude-desktop, Codex, Antigravity, OpenCode) "
    "sur le PC Windows autorisé, dans des workspaces allowlistés. Flux : agent_runner_list -> "
    "agent_workspace_list -> agent_job_start (asynchrone, "
    "contrat de suivi : rappeler agent_job_wait jusqu'à terminal=true dans le même tour) "
    "-> agent_job_get/output/events jusqu'à un état terminal. Missions : agent_mission_create -> "
    "agent_mission_wait en boucle -> agent_mission_validate (completed exit 0 ≠ validated). "
    "Un timeout de wait non terminal impose de rappeler, jamais de répondre. Pas de shell distant : seul un prompt est transmis."
)


def build_app(store: Store, runner_auth: RunnerAuth, reaper_interval_s: float = 5.0):
    from mcp.server.mcpserver import MCPServer

    from orch_mcp.tools import set_request_origin

    mcp = MCPServer("agent-orchestrator", version=pkg_version("mcp"), instructions=INSTRUCTIONS)
    tools.register(mcp, store)
    mcp_app = mcp.streamable_http_app(stateless_http=True)

    class _OriginMiddleware:
        """Capte l'identité MCP réellement observable (posée par la gateway :
        `x-orch-mcp-acteur` = client_id, `x-orch-mcp-mode` = cli/oauth) dans une
        ContextVar lue par agent_job_start/mission_create. Boucle locale directe
        => (None, None). Borné, jamais de secret, jamais bloquant."""

        def __init__(self, app) -> None:
            self.app = app

        @property
        def router(self):
            return self.app.router

        async def __call__(self, scope, receive, send):
            token = None
            try:
                if scope.get("type") == "http":
                    raw = {
                        k.decode("latin-1").lower(): v.decode("latin-1")
                        for k, v in scope.get("headers", [])
                    }
                    actor = (raw.get("x-orch-mcp-acteur") or "").strip()[:128] or None
                    mode = (raw.get("x-orch-mcp-mode") or "").strip()[:32] or None
                    if mode not in (None, "cli", "oauth"):
                        mode = None
                    token = set_request_origin(actor, mode)
            except Exception:  # noqa: BLE001 - audit best-effort
                token = None
            try:
                await self.app(scope, receive, send)
            finally:
                try:
                    if token is not None:
                        from orch_mcp.tools import _request_origin

                        _request_origin.reset(token)
                except Exception:  # noqa: BLE001 - reset best-effort, jamais bloquant
                    log.debug("origin_reset_failed")

    mcp_app = _OriginMiddleware(mcp_app)
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
