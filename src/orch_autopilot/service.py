"""Point d'entrée `python -m orch_autopilot.service [run|kill-switch on|off|status <id>]`.

Un processus : boucle superviseur (thread) + cockpit MCP + `/health` sur la boucle locale.
Watchdog systemd (sd_notify) alimenté par chaque tick réussi : un superviseur bloqué est redémarré.
"""

from __future__ import annotations

import contextlib
import hmac
import json
import logging
import os
import socket
import sys
import threading
import time
from pathlib import Path

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from orch_autopilot import cockpit
from orch_autopilot.broker import MCPBroker
from orch_autopilot.db import AutopilotDB
from orch_autopilot.gate import CommandNotifier, FileNotifier, OpusMCPGate, UnavailableGate
from orch_autopilot.policy import Config
from orch_autopilot.supervisor import Supervisor

log = logging.getLogger("orch_autopilot")


def sd_notify(message: str) -> None:
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    if addr.startswith("@"):
        addr = "\0" + addr[1:]
    with contextlib.suppress(OSError), socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
        s.connect(addr)
        s.sendall(message.encode())


def build_supervisor(env: dict[str, str] | None = None) -> Supervisor:
    env = dict(os.environ if env is None else env)
    cfg = Config.from_env(env)
    data = Path(env.get("AUTOPILOT_DATA_DIR", "/srv/orch/autopilot"))
    db = AutopilotDB(data / "autopilot.db")
    broker = MCPBroker(env.get("AUTOPILOT_BROKER_URL", "http://127.0.0.1:8802/mcp"))
    gate = UnavailableGate()
    if env.get("AUTOPILOT_OPUS_URL"):
        token = None
        token_file = env.get("AUTOPILOT_OPUS_TOKEN_FILE")
        if token_file and Path(token_file).exists():
            token = Path(token_file).read_text(encoding="utf-8").strip()
        gate = OpusMCPGate(env["AUTOPILOT_OPUS_URL"], token)
    notifier = None
    if env.get("AUTOPILOT_NOTIFY_CMD"):
        notifier = CommandNotifier(json.loads(env["AUTOPILOT_NOTIFY_CMD"]))
    elif env.get("AUTOPILOT_NOTIFY_DIR"):
        notifier = FileNotifier(env["AUTOPILOT_NOTIFY_DIR"])
    return Supervisor(db, broker, cfg, gate=gate, notifier=notifier, consent_key=_consent_key(data))


def _consent_key(data: Path) -> bytes:
    """Clé HMAC des codes d'approbation, créée une fois (0600) à côté de la base."""
    path = data / "consent.key"
    if not path.exists():
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as fh:
            fh.write(os.urandom(32))
    key = path.read_bytes()
    if len(key) != 32:
        raise RuntimeError(f"{path} : clé de 32 octets attendue")
    return key


def cockpit_token(env: dict[str, str] | None = None) -> str | None:
    """Jeton exigé sur le cockpit MCP (même en boucle locale : d'autres comptes du VPS y ont accès)."""
    env = dict(os.environ if env is None else env)
    path = env.get("AUTOPILOT_COCKPIT_TOKEN_FILE", "").strip()
    if not path:
        return None
    token = Path(path).read_text(encoding="utf-8").strip()
    if len(token) < 32:
        raise RuntimeError("AUTOPILOT_COCKPIT_TOKEN_FILE : jeton de 32 caractères minimum")
    return token


class BearerGuard:
    """ASGI : refuse (401) toute requête sans `Authorization: Bearer <jeton>`, sauf GET /health."""

    def __init__(self, app, token: str) -> None:
        self.app = app
        self.expected = f"Bearer {token}".encode()

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] == "http" and scope.get("path") != "/health":
            got = dict(scope.get("headers") or []).get(b"authorization", b"")
            if not hmac.compare_digest(got, self.expected):
                await send({"type": "http.response.start", "status": 401,
                            "headers": [(b"content-type", b"application/json"), (b"www-authenticate", b"Bearer")]})
                await send({"type": "http.response.body", "body": b'{"error":"unauthorized"}'})
                return
        await self.app(scope, receive, send)


TICK_HANG_S = 1_000.0  # > durée max d'un appel gate Opus (900 s) : au-delà, le watchdog laisse systemd redémarrer


def build_app(sup: Supervisor, run_loop: bool = True, token: str | None = None):
    from importlib.metadata import version as pkg_version

    from mcp.server.mcpserver import MCPServer

    mcp = MCPServer("autopilot", version=pkg_version("mcp"), instructions=cockpit.INSTRUCTIONS)
    cockpit.register(mcp, sup)
    mcp_app = mcp.streamable_http_app(stateless_http=True)
    stop = threading.Event()

    tick_started: list[float | None] = [None]

    def loop() -> None:
        sd_notify("READY=1")
        while not stop.is_set():
            tick_started[0] = time.monotonic()
            try:
                sup.tick()
            except Exception:  # la boucle ne meurt jamais ; le watchdog tranche si elle se bloque
                log.exception("tick_error")
            tick_started[0] = None
            stop.wait(sup.cfg.tick_s)

    def watchdog() -> None:
        # pings indépendants du tick : un appel Opus légitime (<= 900 s) ne tue pas le service,
        # un tick bloqué au-delà de TICK_HANG_S cesse d'alimenter le watchdog.
        while not stop.is_set():
            started = tick_started[0]
            if started is None or time.monotonic() - started < TICK_HANG_S:
                sd_notify("WATCHDOG=1")
            stop.wait(20)

    async def health(_: Request) -> JSONResponse:
        age = None if sup.last_tick_at is None else time.time() - sup.last_tick_at
        ok = (not run_loop) or (age is not None and age < max(60.0, sup.cfg.tick_s * 6))
        try:
            disk = sup.disk_status()
        except OSError:
            disk = {"low": True, "error": "disk_usage_failed"}
        return JSONResponse({"status": "ok" if ok else "degraded", "service": "orch-autopilot", "tick_age_s": age,
                             "kill_switch": sup.kill_switch_on(), "disk": disk}, status_code=200 if ok else 503)

    @contextlib.asynccontextmanager
    async def lifespan(app):
        threads = [threading.Thread(target=loop, name="autopilot-supervisor", daemon=True),
                   threading.Thread(target=watchdog, name="autopilot-watchdog", daemon=True)] if run_loop else []
        for thread in threads:
            thread.start()
        async with mcp_app.router.lifespan_context(app):
            yield
        stop.set()

    app = Starlette(routes=[Route("/health", health, methods=["GET"]), Mount("/", app=mcp_app)], lifespan=lifespan)
    return BearerGuard(app, token) if token else app


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    logging.basicConfig(level=os.environ.get("AUTOPILOT_LOG_LEVEL", "INFO"), format="%(levelname)s %(name)s %(message)s")
    cmd = argv[0] if argv else "run"
    sup = build_supervisor()
    if cmd == "kill-switch" and len(argv) == 2 and argv[1] in ("on", "off"):
        print(json.dumps(sup.set_kill_switch(argv[1] == "on", "cli")))
        return 0
    if cmd == "status" and len(argv) == 2:
        print(json.dumps(sup.handoff(argv[1]), ensure_ascii=False, indent=2))
        return 0
    if cmd != "run":
        print("usage: python -m orch_autopilot.service [run | kill-switch on|off | status <objective_id>]", file=sys.stderr)
        return 2
    port = int(os.environ.get("AUTOPILOT_PORT", "0"))
    if not port:
        print("AUTOPILOT_PORT requis (port local libre vérifié sur l'hôte)", file=sys.stderr)
        return 2
    token = cockpit_token()
    if token is None:
        print("AUTOPILOT_COCKPIT_TOKEN_FILE requis : le cockpit n'est jamais servi sans authentification", file=sys.stderr)
        return 2
    uvicorn.run(build_app(sup, token=token), host="127.0.0.1", port=port, log_level="info", timeout_graceful_shutdown=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
