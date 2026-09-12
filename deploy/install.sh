#!/usr/bin/env bash
# Installation / mise a jour de la stack orch (root). Idempotent.
# Usage : sudo bash /srv/orch/deploy/install.sh
# Le vhost partage mymcps.duckdns.org.conf n'est modifie que pour y ajouter UNE ligne include
# (sauvegarde horodatee + nginx -t + restauration automatique si le test echoue).
set -euo pipefail
RACINE="/srv/orch"
VHOST="/etc/nginx/sites-available/mymcps.duckdns.org.conf"
INCLUDE="    include /etc/nginx/snippets/orch-public.conf;"

id orch-app >/dev/null 2>&1 || useradd --system --home "$RACINE" --shell /usr/sbin/nologin orch-app
install -d -o orch-app -g orch-app -m 700 "$RACINE/data" "$RACINE/data/oauth"
install -d -o root -g orch-app -m 750 "$RACINE/secrets"
test -f "$RACINE/secrets/orch.env" || { echo "manque $RACINE/secrets/orch.env" >&2; exit 1; }
chown root:orch-app "$RACINE/secrets/orch.env"
chmod 640 "$RACINE/secrets/orch.env"

echo "== unites systemd =="
install -m 644 "$RACINE/deploy/systemd/orch-mcp.service" /etc/systemd/system/orch-mcp.service
install -m 644 "$RACINE/deploy/systemd/orch-gateway.service" /etc/systemd/system/orch-gateway.service
systemctl daemon-reload

echo "== nginx =="
install -m 644 "$RACINE/deploy/nginx/orch-public.snippet.conf" /etc/nginx/snippets/orch-public.conf
install -m 644 "$RACINE/deploy/nginx/orch-runner.conf" /etc/nginx/sites-available/orch-runner.conf
ln -sfn /etc/nginx/sites-available/orch-runner.conf /etc/nginx/sites-enabled/orch-runner.conf
SAUVEGARDE=""
if ! grep -qF "include /etc/nginx/snippets/orch-public.conf;" "$VHOST"; then
  SAUVEGARDE="/root/mymcps.duckdns.org.conf.bak-orch-$(date +%Y%m%d%H%M%S)"
  cp -a "$VHOST" "$SAUVEGARDE"
  python3 "$RACINE/deploy/insert_include.py" "$VHOST" "$INCLUDE"
fi
if ! nginx -t; then
  echo "nginx -t KO : restauration" >&2
  if [ -n "$SAUVEGARDE" ]; then cp -a "$SAUVEGARDE" "$VHOST"; fi
  rm -f /etc/nginx/sites-enabled/orch-runner.conf
  nginx -t
  exit 1
fi

echo "== services =="
systemctl enable --now orch-mcp.service orch-gateway.service
systemctl reload nginx
echo "OK : stack orch installee ${SAUVEGARDE:+(sauvegarde vhost : $SAUVEGARDE)}"
