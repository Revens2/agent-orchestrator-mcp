"""CLI locale des pauses humaines (VPS uniquement).

Permet de déclarer depuis le PC/VPS, sans passer par ChatGPT, qu'un job attend
une action de l'utilisateur — typiquement « je change ma clé d'API, le temps que
je fais ça ne dis pas que c'est en panne ». Le broker suspend alors ses comptes à
rebours (lost, stall, timeout dur) le temps annoncé, borné.

  .../pause_cli pause  --job <job_id> [--reason quota_exhausted] [--note "..."] [--expected-s 1800]
  .../pause_cli resume --job <job_id> [--note "nouvelle clé en place"]
  .../pause_cli status --job <job_id>     # signal de vie complet (verdict + preuves datées)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import orch_protocol as P
from orch_mcp.store import BrokerError, Store


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pauses humaines : exploitation locale (VPS).")
    p.add_argument("--db", default=os.environ.get("ORCH_DATA_DIR", ".") + "/orch.db")
    sub = p.add_subparsers(dest="cmd", required=True)
    pa = sub.add_parser("pause")
    pa.add_argument("--job", required=True)
    pa.add_argument("--reason", default=P.PAUSE_MANUAL, choices=list(P.PAUSE_REASONS))
    pa.add_argument("--note", default=None)
    pa.add_argument("--expected-s", type=int, default=None)
    re_ = sub.add_parser("resume")
    re_.add_argument("--job", required=True)
    re_.add_argument("--note", default=None)
    st = sub.add_parser("status")
    st.add_argument("--job", required=True)
    return p


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 2)
    store = Store(Path(args.db))
    try:
        if args.cmd == "pause":
            out: dict = store.pause_job(args.job, args.reason, args.note, args.expected_s, P.PAUSE_SRC_HUMAN)
        elif args.cmd == "resume":
            out = store.resume_job(args.job, P.PAUSE_SRC_HUMAN, args.note)
        elif args.cmd == "status":
            found = store.liveness(args.job)
            out = found if found is not None else {"error": "unknown_job", "job_id": args.job}
        else:
            out = {"error": "unknown_command"}
    except BrokerError as exc:
        print(json.dumps({"error": exc.code, "message": exc.message}, ensure_ascii=False))
        return 2
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
