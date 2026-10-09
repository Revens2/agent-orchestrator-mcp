#!/usr/bin/env bash
# Installation STAGING de l'autopilot (root). Idempotent. Ne touche NI /srv/orch NI nginx NI orch.db.
# Usage : sudo bash /srv/autopilot/deploy/autopilot/install.sh   (code deja copie dans /srv/autopilot)
# La passerelle publique (nginx + OAuth) n'est PAS installee ici : exposition = decision explicite.
set -euo pipefail
RACINE="/srv/autopilot"
id orch-app >/dev/null 2>&1 || { echo "compte orch-app absent (stack orch requise)" >&2; exit 1; }
test -x /srv/orch/venv/bin/python || { echo "venv /srv/orch/venv absent" >&2; exit 1; }
install -d -o orch-app -g orch-app -m 700 "$RACINE/data" "$RACINE/data/approvals" "$RACINE/gw-oauth"
install -d -o root -g orch-app -m 750 "$RACINE/secrets"
test -f "$RACINE/secrets/autopilot.env" || { echo "manque $RACINE/secrets/autopilot.env" >&2; exit 1; }
chown root:orch-app "$RACINE/secrets/autopilot.env"; chmod 640 "$RACINE/secrets/autopilot.env"
PORT="$(grep -E '^AUTOPILOT_PORT=' "$RACINE/secrets/autopilot.env" | cut -d= -f2)"
test -n "$PORT" || { echo "AUTOPILOT_PORT vide" >&2; exit 1; }
if ss -ltnH "sport = :$PORT" | grep -q .; then
  systemctl is-active --quiet autopilot.service || { echo "port $PORT deja pris" >&2; exit 1; }
fi
install -m 644 "$RACINE/deploy/autopilot/autopilot.service" /etc/systemd/system/autopilot.service
systemctl daemon-reload
systemctl enable --now autopilot.service
sleep 3
curl -fsS "http://127.0.0.1:$PORT/health" && echo && echo "OK : autopilot staging actif sur 127.0.0.1:$PORT"
