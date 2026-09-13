#!/usr/bin/env bash
# photon_dispatch.sh — notifie UNE fois les questions dues via Photon/iMessage.
# INACTIF par défaut (aucun sender configuré) : les questions dues restent
# visibles en inbox (notify_status=pending) plutôt qu'un envoi mensonger.
#
# Senders pluggables (PHOTON_SENDER) :
#   none     : ne notifie jamais, marque deferred (défaut honnête).
#   file     : écrit le message corrélé dans $PHOTON_SPOOL (E2E/preuve locale).
#   hermes   : dépose dans la file d'envoi Hermes (à brancher : voir OPERATIONS).
#   photon   : appel sidecar local 127.0.0.1:8789 avec $PHOTON_SIDECAR_TOKEN
#              (non configuré : deferred tant que le token n'est pas posé en
#              0600 hors Git ; jamais de token en variable d'environnement globale).
#
# Format corrélé : [titre] (runtime/session) + question bornée + choix numérotés
# + correlation_id. Réponse attendue : numéro ou "Autre: ...".
# Dedup/replay : mark_notified idempotent (un seul message) ; answer single-use
# côté broker ; expiration via purge. Allowlist : vérifiée par Hermes/Photon à
# la réception (seul l'expéditeur autorisé) ; answer_from audité.
set -euo pipefail

SENDER="${PHOTON_SENDER:-none}"
SPOOL="${PHOTON_SPOOL:-/srv/orch/data/photon-outbox.jsonl}"
DB="${ORCH_DB:-/srv/orch/data/orch.db}"

qcli() { PYTHONPATH=/srv/orch/src /srv/orch/venv/bin/python -m orch_mcp.question_cli --db "$DB" "$@"; }

due="$(qcli due)"
count="$(printf '%s' "$due" | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])')"
[ "$count" -gt 0 ] || exit 0

printf '%s' "$due" | python3 -c 'import json,sys; [print(json.dumps(q, ensure_ascii=False)) for q in json.load(sys.stdin)["questions"]]' | while IFS= read -r q; do
    qid="$(printf '%s' "$q" | python3 -c 'import json,sys; print(json.load(sys.stdin)["question_id"])')"
    msg="$(printf '%s' "$q" | python3 -c '
import json, sys
q = json.loads(sys.stdin.read())
lines = ["❓ [%s] (%s / %s)" % (q["title"], q["runtime"], q["session_ref"][:8]),
         q["question"][:1200]]
for i, o in enumerate(q.get("options") or [], 1):
    lines.append("%d. %s" % (i, o))
lines.append("Répondre : numéro, ou Autre: ... (id %s)" % q["question_id"])
print("\n".join(lines))')"
    case "$SENDER" in
        file)
            printf '%s\n' "$msg" >> "$SPOOL"
            qcli notify --id "$qid" --status sent >/dev/null
            ;;
        hermes|photon)
            # Sender non branché : deferred honnête (jamais de promesse d'envoi).
            qcli notify --id "$qid" --status deferred >/dev/null
            ;;
        *)
            qcli notify --id "$qid" --status deferred >/dev/null
            ;;
    esac
done
