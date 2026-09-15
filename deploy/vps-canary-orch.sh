#!/bin/bash
# Canary VPS orch-gateway-rs : toolchain + build release + install + demarrage.
# Idempotent. Aucune modification du service prod orch-gateway.service.
set -euo pipefail
export HOME=/home/juliann
export PATH="$HOME/.cargo/bin:$PATH"
BUILD=/home/juliann/build/mcp-rust-migration

cargo --version
rustup component add rustfmt clippy 2>/dev/null || true
cd "$BUILD/orch-gateway-rs"
echo "[canary] fmt/clippy/test…"
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
echo "[canary] build release…"
cargo build --release
echo "[canary] installation /opt/orch-gateway-rs…"
sudo install -d -o orch-app -g orch-app -m 0755 /opt/orch-gateway-rs
sudo install -m 0755 target/release/orch-gateway-rs /opt/orch-gateway-rs/orch-gateway-rs
sudo install -m 0644 deploy/orch-gateway-rs.service /etc/systemd/system/orch-gateway-rs.service
sudo systemctl daemon-reload
echo "[canary] demarrage orch-gateway-rs.service (:18981)…"
sudo systemctl enable --now orch-gateway-rs.service
sleep 3
sudo systemctl is-active orch-gateway-rs.service
curl -s http://127.0.0.1:18981/health; echo
curl -s http://127.0.0.1:18981/ready; echo
echo "[canary] OK"
