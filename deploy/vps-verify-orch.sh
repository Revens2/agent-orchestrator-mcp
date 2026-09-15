#!/bin/bash
# Verification canary orch : sante + fail-closed, prod intacte. Lecture seule.
set -u
export HOME=/home/juliann
echo "=== canary :18981 ==="
curl -s --max-time 5 http://127.0.0.1:18981/health; echo
curl -s --max-time 5 http://127.0.0.1:18981/ready; echo
curl -s -o /dev/null -w "POST /mcp sans auth -> %{http_code}\n" --max-time 5 \
  -X POST http://127.0.0.1:18981/mcp -H "content-type: application/json" -d '{}' || true
systemctl is-active orch-gateway-rs.service
echo "=== prod :8801 intacte ==="
curl -s --max-time 5 http://127.0.0.1:8801/health; echo
systemctl is-active orch-gateway.service orch-mcp.service
