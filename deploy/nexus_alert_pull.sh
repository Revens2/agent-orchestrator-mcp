#!/usr/bin/env bash
# nexus_alert_pull.sh — aspiration du spool d'alertes Nexus vers le broker Étude.
# NON ACTIF tant que l'accès SSH Nexus (ia_admin) n'est pas rétabli.
# Usage manuel : sudo -u orch-app /srv/orch/deploy/nexus_alert_pull.sh
# Cron (à activer après validation E2E) : */5 * * * * orch-app /srv/orch/deploy/nexus_alert_pull.sh
#
# Robustesse rotation : curseur = dernier ts traité (ISO UTC), pas un offset octet.
# Les doublons intra-fenêtre sont absorbés par la déduplication du broker.
set -euo pipefail

NEXUS_SSH="${NEXUS_SSH:-ia_admin@10.200.61.52}"
NEXUS_KEY="${NEXUS_KEY:-$HOME/.ssh/nexus_pull}"
SPOOL="/var/log/nexus-alerts.jsonl"
STATE_DIR="/srv/orch/data"
CURSOR_FILE="$STATE_DIR/nexus_alerts.cursor"
DB="$STATE_DIR/orch.db"

cursor="1970-01-01T00:00:00Z"
[ -f "$CURSOR_FILE" ] && cursor="$(cat "$CURSOR_FILE" 2>/dev/null || echo "$cursor")"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
ssh -i "$NEXUS_KEY" -o ConnectTimeout=15 -o BatchMode=yes "$NEXUS_SSH" \
    "tail -n 200 '$SPOOL' 2>/dev/null" > "$tmp" || exit 0

CURSOR="$cursor" DB="$DB" PYTHONPATH=/srv/orch/src /srv/orch/venv/bin/python - "$tmp" <<'EOF'
import hashlib
import json, os, subprocess, sys
db = os.environ["DB"]
cursor = os.environ.get("CURSOR", "")
new_cursor, n = cursor, 0
base_env = {"PYTHONPATH": "/srv/orch/src", "PATH": "/usr/bin:/bin"}
with open(sys.argv[1], encoding="utf-8", errors="replace") as f:
    lines = [ln.strip() for ln in f if ln.strip()]
for line in lines:
    try:
        a = json.loads(line)
    except ValueError:
        continue
    if a.get("source") != "nexus":
        continue
    ts = str(a.get("ts", ""))
    if ts <= cursor:
        continue
    sev = a.get("severity") if a.get("severity") in ("info", "warning", "critical") else "info"
    fp_src = "|".join((ts, str(a.get("service")), str(a.get("title")))).encode()
    fp = "nexus-pull-" + hashlib.sha256(fp_src).hexdigest()[:40]
    r = subprocess.run(
        ["/srv/orch/venv/bin/python", "-m", "orch_mcp.alert_cli", "--db", db,
         "record", "--source", "nexus",
         "--service", str(a.get("service", "nexus"))[:128],
         "--severity", sev, "--title", str(a.get("title", "alerte nexus"))[:500],
         "--detail", str(a.get("detail", ""))[:4000],
         "--fingerprint", fp],
        capture_output=True, text=True, env=base_env)
    if r.returncode == 0:
        n += 1
        if ts > new_cursor:
            new_cursor = ts
print("RECORDED:", n)
if new_cursor != cursor:
    open(os.path.join("/srv/orch/data", "nexus_alerts.cursor"), "w").write(new_cursor)
EOF
