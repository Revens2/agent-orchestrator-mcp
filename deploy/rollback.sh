#!/usr/bin/env bash
# Rollback complet orch (root). Laisse intacts les autres MCP.
# Usage : sudo bash /srv/orch/deploy/rollback.sh [--purge-data]
set -euo pipefail
VHOST="/etc/nginx/sites-available/mymcps.duckdns.org.conf"
systemctl disable --now orch-gateway.service orch-mcp.service 2>/dev/null || true
rm -f /etc/nginx/sites-enabled/orch-runner.conf
cp -a "$VHOST" "/root/mymcps.duckdns.org.conf.bak-orch-rollback-$(date +%Y%m%d%H%M%S)"
sed -i '\#include /etc/nginx/snippets/orch-public.conf;#d' "$VHOST"
nginx -t
systemctl reload nginx
rm -f /etc/nginx/snippets/orch-public.conf /etc/systemd/system/orch-mcp.service /etc/systemd/system/orch-gateway.service
systemctl daemon-reload
if [ "${1:-}" = "--purge-data" ]; then rm -rf /srv/orch/data; fi
echo "OK : orch retire (nginx recharge, autres MCP intacts)"
