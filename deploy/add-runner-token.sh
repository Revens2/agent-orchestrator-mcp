#!/usr/bin/env bash
# Ajoute (ou remplace en rotation) UNE empreinte runner `runner_id:sha256hex`
# dans ORCH_RUNNER_TOKENS, sans toucher aux autres entrées (ex. main-windows-pc).
# Idempotent : relancer avec la même empreinte ne change rien (exit 0, "unchanged").
# Aucun secret dans Git : seule l'empreinte SHA-256 transite ici, jamais le jeton
# (généré et chiffré DPAPI sur le PC via `python -m orch_runner gen-token`).
#
# Usage (vps-etude) :
#   sudo bash deploy/add-runner-token.sh 'pc-fixe:<sha256hex>'
#   sudo bash deploy/add-runner-token.sh 'pc-fixe:<sha256hex>' --dry-run   # vérifie sans écrire
# Options :
#   --env-file PATH   fichier orch.env (défaut : /srv/orch/secrets/orch.env)
#   --no-restart      ne pas `systemctl restart orch-mcp` (le redémarrage reste
#                     requis en prod pour prendre en compte le nouveau jeton)
#   --dry-run         n'écrit rien, ne redémarre rien ; affiche le résultat
set -euo pipefail

ENV_FILE="/srv/orch/secrets/orch.env"
RESTART=1
DRY_RUN=0
ENTRY=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) ENV_FILE="$2"; shift 2;;
    --env-file=*) ENV_FILE="${1#--env-file=}"; shift;;
    --no-restart) RESTART=0; shift;;
    --dry-run) DRY_RUN=1; shift;;
    -h|--help) sed -n '2,15p' "$0"; exit 0;;
    *) ENTRY="$1"; shift;;
  esac
done

if [[ -z "$ENTRY" ]]; then
  echo "usage: $0 'runner_id:<sha256hex>' [--env-file PATH] [--no-restart] [--dry-run]" >&2
  exit 2
fi
if [[ ! "$ENTRY" =~ ^[a-z0-9][a-z0-9_-]{0,63}:[0-9a-f]{64}$ ]]; then
  echo "refus : entrée invalide (attendu runner_id:sha256hex minuscules)." >&2
  exit 2
fi
if [[ ! -f "$ENV_FILE" ]]; then
  echo "refus : $ENV_FILE introuvable." >&2
  exit 2
fi

NEW_ID="${ENTRY%%:*}"
CURRENT="$(grep -E '^ORCH_RUNNER_TOKENS=' "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true)"
if [[ -z "$CURRENT" ]]; then
  echo "refus : ORCH_RUNNER_TOKENS absent de $ENV_FILE." >&2
  exit 2
fi

# Reconstruction idempotente : remplace l'entrée du même runner_id, sinon ajoute.
RESULT=""
REPLACED=0
IFS=',' read -ra PARTS <<< "$CURRENT"
for part in "${PARTS[@]}"; do
  part="$(echo "$part" | tr -d '[:space:]')"
  [[ -z "$part" ]] && continue
  id="${part%%:*}"
  if [[ "$id" == "$NEW_ID" ]]; then
    RESULT="${RESULT:+$RESULT,}$ENTRY"
    REPLACED=1
  else
    RESULT="${RESULT:+$RESULT,}$part"
  fi
done
if [[ "$REPLACED" != "1" ]]; then
  RESULT="${RESULT:+$RESULT,}$ENTRY"
fi

if [[ "$RESULT" == "$CURRENT" ]]; then
  echo "unchanged : $NEW_ID déjà déclaré avec cette empreinte."
  exit 0
fi

if [[ "$DRY_RUN" == "1" ]]; then
  echo "dry-run : écrirait ORCH_RUNNER_TOKENS=$RESULT"
  exit 0
fi

TS="$(date +%Y%m%d-%H%M%S)"
cp -a "$ENV_FILE" "$ENV_FILE.bak-$TS"
TMP="$(mktemp)"
awk -v repl="ORCH_RUNNER_TOKENS=$RESULT" '
  { if ($0 ~ /^ORCH_RUNNER_TOKENS=/) { if (!done) { print repl; done=1 } } else { print } }
' "$ENV_FILE" > "$TMP"
cat "$TMP" > "$ENV_FILE"
rm -f "$TMP"
chmod 640 "$ENV_FILE"
echo "OK : ORCH_RUNNER_TOKENS mis à jour (backup $ENV_FILE.bak-$TS). Entrées préservées sauf $NEW_ID."

if [[ "$RESTART" == "1" ]]; then
  systemctl restart orch-mcp
  sleep 2
  curl -fsS http://127.0.0.1:8802/health >/dev/null
  echo "OK : orch-mcp redémarré, /health ok."
else
  echo "Rappel : \`systemctl restart orch-mcp\` requis pour prendre en compte le jeton."
fi
