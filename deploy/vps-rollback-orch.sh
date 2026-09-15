#!/bin/bash
# Rollback canary orch : stoppe le canary, verifie la prod intacte.
# Aucune modification du service prod (jamais pointe vers le binaire Rust).
set -euo pipefail
sudo systemctl stop orch-gateway-rs.service || true
sudo systemctl is-active orch-gateway.service
curl -s http://127.0.0.1:8801/health; echo
echo "[rollback] prod :8801 intacte, canary stoppe"
