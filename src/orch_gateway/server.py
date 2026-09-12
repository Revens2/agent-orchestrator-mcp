"""Point d'entree de la passerelle orch (`python -m orch_gateway.server`).

Ecoute sur la boucle locale uniquement : nginx (vhost mymcps /orch) est le
seul point d'entree depuis l'exterieur. L'upstream orch-mcp n'est joint qu'en
127.0.0.1 (8802). Meme compromis d'isolation que `vault-mcp.service` : voir l'unite systemd.
"""

from __future__ import annotations

import uvicorn

from orch_gateway.app import _config, construire_application


def main() -> None:
    _, _, port, _, _ = _config()
    # Arret borne : sans limite, uvicorn attend la fin des flux SSE GET /mcp relayes
    # (jusqu'a 3600 s) et systemd ne tue qu'apres TimeoutStopSec (90 s) -> restart ou
    # rollback bloques ~90 s avec 502 cote nginx (constate en repetition 2026-09-10).
    uvicorn.run(
        construire_application(),
        host="127.0.0.1",
        port=port,
        log_level="info",
        timeout_graceful_shutdown=5,
    )


if __name__ == "__main__":
    main()
