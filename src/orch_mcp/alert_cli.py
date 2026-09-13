"""CLI locale d'exploitation des alertes infra (VPS uniquement, jamais via MCP).

Écriture réservée aux ingesteurs locaux (cron/systemd, compte orch-app) :
la base broker est la persistance normalisée des alertes sortantes Telegram.

  PYTHONPATH=/srv/orch/src python3 -m orch_mcp.alert_cli --db /srv/orch/data/orch.db \\
      record --source etude --service orch-mcp --severity critical --title "..." [--detail ...]
  .../alert_cli list --source etude --limit 20
  .../alert_cli get --id <alert_id>
  .../alert_cli ack|resolve --id <alert_id>

Formats Telegram unifiés : tout message sortant porte `[ETUDE]` ou `[NEXUS]`,
la sévérité et un timestamp ; voir docs/OPERATIONS.md.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from orch_mcp.store import BrokerError, Store

TELEGRAM_PREFIX = {"etude": "[ETUDE]", "nexus": "[NEXUS]"}


def telegram_format(source: str, severity: str, title: str, detail: str | None = None) -> str:
    """Format canonique unifié des alertes Telegram (préfixe source explicite)."""
    import time

    head = f"{TELEGRAM_PREFIX[source]} [{severity}] {title}"
    when = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    body = f"{head}\n🕐 {when} UTC"
    if detail:
        body += f"\n{detail[:1500]}"
    return body


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Alertes infra : exploitation locale (VPS).")
    p.add_argument("--db", default=os.environ.get("ORCH_DATA_DIR", ".") + "/orch.db")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("record")
    r.add_argument("--source", required=True, choices=["etude", "nexus"])
    r.add_argument("--service", required=True)
    r.add_argument("--severity", required=True, choices=["info", "warning", "critical"])
    r.add_argument("--title", required=True)
    r.add_argument("--detail")
    r.add_argument("--fingerprint")
    r.add_argument("--telegram-format", action="store_true", help="affiche aussi le message Telegram unifié")
    lst = sub.add_parser("list")
    lst.add_argument("--source", choices=["etude", "nexus"])
    lst.add_argument("--severity", choices=["info", "warning", "critical"])
    lst.add_argument("--state", choices=["active", "acked", "resolved"])
    lst.add_argument("--limit", type=int, default=20)
    g = sub.add_parser("get")
    g.add_argument("--id", required=True)
    a = sub.add_parser("ack")
    a.add_argument("--id", required=True)
    rv = sub.add_parser("resolve")
    rv.add_argument("--id", required=True)
    return p


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 2)  # usage invalide : code borné, pas de traceback (cron)
    store = Store(Path(args.db))
    try:
        if args.cmd == "record":
            alert, created = store.record_alert(
                args.source, args.service, args.severity, args.title, args.detail, args.fingerprint
            )
            out: dict = {"alert": alert, "created": created}
            if args.telegram_format:
                out["telegram"] = telegram_format(args.source, args.severity, args.title, args.detail)
        elif args.cmd == "list":
            out = store.list_alerts(args.source, args.severity, args.state, None, None, args.limit)
        elif args.cmd == "get":
            found = store.get_alert(args.id)
            out = found if found is not None else {"error": "unknown_alert"}
        elif args.cmd == "ack":
            out = store.set_alert_state(args.id, "acked")
        elif args.cmd == "resolve":
            out = store.set_alert_state(args.id, "resolved")
        else:
            out = {"error": "unknown_command"}
    except BrokerError as exc:
        print(json.dumps({"error": exc.code, "message": exc.message}, ensure_ascii=False))
        return 2
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
