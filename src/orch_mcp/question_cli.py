"""CLI locale des questions en attente (VPS uniquement, jamais via MCP sauf
agent_question_answer qui est un outil MCP d'écriture étroit).

  .../question_cli record --origin mission --session-ref <job_id> --runtime opencode \\
      --title "..." --question "..." [--option "1: ..." --option "2: ..."] [--notify-after-s 300]
  .../question_cli due            # questions dues (délai dépassé, non notifiées)
  .../question_cli notify --id <qid> --status sent|deferred|failed
  .../question_cli answer --id <qid> --answer "2" --from imessage
  .../question_cli list [--status open]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from orch_mcp.store import BrokerError, Store


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Questions en attente : exploitation locale (VPS).")
    p.add_argument("--db", default=os.environ.get("ORCH_DATA_DIR", ".") + "/orch.db")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("record")
    r.add_argument("--origin", required=True, choices=["agent", "chatgpt-web", "mission"])
    r.add_argument("--session-ref", required=True)
    r.add_argument("--runtime", required=True)
    r.add_argument("--title", required=True)
    r.add_argument("--question", required=True)
    r.add_argument("--option", action="append", default=[])
    r.add_argument("--notify-after-s", type=int, default=None)
    sub.add_parser("due")
    n = sub.add_parser("notify")
    n.add_argument("--id", required=True)
    n.add_argument("--status", required=True, choices=["pending", "sent", "deferred", "failed"])
    a = sub.add_parser("answer")
    a.add_argument("--id", required=True)
    a.add_argument("--answer", required=True)
    a.add_argument("--from", dest="answer_from", required=True)
    lst = sub.add_parser("list")
    lst.add_argument("--status", choices=["open", "answered", "expired"])
    lst.add_argument("--origin", choices=["agent", "chatgpt-web", "mission"])
    lst.add_argument("--limit", type=int, default=20)
    g = sub.add_parser("get")
    g.add_argument("--id", required=True)
    return p


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 2)
    store = Store(Path(args.db))
    try:
        if args.cmd == "record":
            q, created = store.record_question(
                args.origin, args.session_ref, args.runtime, args.title,
                args.question, args.option, args.notify_after_s,
            )
            out: dict = {"question": q, "created": created}
        elif args.cmd == "due":
            out = {"questions": store.due_questions(), "count": len(store.due_questions())}
        elif args.cmd == "notify":
            out = store.mark_notified(args.id, args.status) or {"error": "unknown_question"}
        elif args.cmd == "answer":
            out = store.answer_question(args.id, args.answer, args.answer_from)
        elif args.cmd == "list":
            out = store.list_questions(args.status, args.origin, args.limit)
        elif args.cmd == "get":
            found = store.get_question(args.id)
            out = found if found is not None else {"error": "unknown_question"}
        else:
            out = {"error": "unknown_command"}
    except BrokerError as exc:
        print(json.dumps({"error": exc.code, "message": exc.message}, ensure_ascii=False))
        return 2
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
