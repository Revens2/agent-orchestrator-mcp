#!/usr/bin/env bash
# Rollback autopilot (root). La stack orch, nginx et orch.db restent intacts.
# Usage : sudo bash /srv/autopilot/deploy/autopilot/rollback.sh
# Rien n'est efface : unites et autopilot.db sont deplacees dans un dossier horodate.
set -euo pipefail
DEST="/root/autopilot-rollback-$(date +%Y%m%d%H%M%S)"
install -d -m 700 "$DEST"
systemctl disable --now autopilot-gateway.service autopilot.service 2>/dev/null || true
for u in autopilot.service autopilot-gateway.service; do
  if [ -f "/etc/systemd/system/$u" ]; then mv "/etc/systemd/system/$u" "$DEST/"; fi
done
systemctl daemon-reload
if [ -d /srv/autopilot/data ]; then mv /srv/autopilot/data "$DEST/data"; fi
echo "OK : autopilot retire ; unites et donnees conservees dans $DEST"
