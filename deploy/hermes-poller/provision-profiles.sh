#!/bin/bash
# Provisionne le pool de profils Hermes dedies du poller 2.3 (hermes-vps).
# Idempotent : les profils existants sont conserves tels quels.
# Auth : --clone reprend config/.env/SOUL.md/skills (pas les channels).
# Rollback : supprimer un par un :
#   echo orch-slot-00 | docker exec -i hermes hermes profile delete orch-slot-00
set -euo pipefail
for i in $(seq -w 0 9); do
  slot="orch-slot-$i"
  if docker exec hermes hermes profile list 2>/dev/null | grep -q "$slot"; then
    echo "conserve: $slot"
  else
    docker exec hermes hermes profile create "$slot" --clone
    echo "cree: $slot"
  fi
done
docker exec hermes hermes profile list
