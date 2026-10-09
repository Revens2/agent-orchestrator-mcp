"""Autopilot V1 : superviseur durable au-dessus du broker orch (plan de contrôle).

Le broker (orch-mcp) reste l'autorité sur jobs/runners/leases/OAuth. L'autopilot garde
sa propre base (`autopilot.db`) et ne parle au broker que par ses outils MCP existants.
Runtime d'exécution du produit : OpenCode EXCLUSIVEMENT (aucun fallback).
"""

from __future__ import annotations

# Seul runtime agentique autorisé pour le produit. Ne pas élargir : voir docs/autopilot/plan.md.
PRODUCT_RUNTIMES: frozenset[str] = frozenset({"opencode"})

# Étapes : kind -> mode broker. `decision` ne lance aucun job (gate Opus). `external` refusé en V1.
KIND_MODE = {"read": "read_only", "local_write": "workspace_write"}
KINDS = ("read", "local_write", "decision")

# États d'étape.
PENDING = "PENDING"                    # dépendances non satisfaites
READY = "READY"                        # prête, pas encore dans l'outbox
DISPATCHING = "DISPATCHING"            # intention écrite dans l'outbox, envoi en cours ou à rejouer
RUNNING = "RUNNING"                    # job broker créé, suivi par réconciliation
NEEDS_VALIDATION = "NEEDS_VALIDATION"  # broker completed (exit 0) : PAS une preuve
COOLDOWN = "COOLDOWN"                  # rate-limit / backoff, aucune tentative consommée
BLOCKED_WORKER = "BLOCKED_WORKER"      # opencode indisponible sur le runner
PENDING_GATE = "PENDING_GATE"          # décision structurante sans gate Opus attesté
NEEDS_APPROVAL = "NEEDS_APPROVAL"      # attente d'un consentement humain (code hors bande)
NEEDS_AUDIT = "NEEDS_AUDIT"            # écriture locale à l'issue incertaine
NEEDS_EFFECT_AUDIT = "NEEDS_EFFECT_AUDIT"  # effet externe incertain : jamais de retry automatique
DONE = "DONE"
FAILED = "FAILED"
CANCELLED = "CANCELLED"

STEP_STATES = (
    PENDING, READY, DISPATCHING, RUNNING, NEEDS_VALIDATION, COOLDOWN, BLOCKED_WORKER, PENDING_GATE,
    NEEDS_APPROVAL, NEEDS_AUDIT, NEEDS_EFFECT_AUDIT, DONE, FAILED, CANCELLED,
)
STEP_TERMINAL = frozenset({DONE, FAILED, CANCELLED})
# États où une intervention humaine est attendue (affichés par status/handoff).
STEP_HUMAN = frozenset({PENDING_GATE, NEEDS_APPROVAL, NEEDS_AUDIT, NEEDS_EFFECT_AUDIT})

# États d'objectif.
OBJ_ACTIVE = "ACTIVE"
OBJ_PAUSED = "PAUSED"
OBJ_DONE = "DONE"
OBJ_FAILED = "FAILED"
OBJ_CANCELLED = "CANCELLED"
OBJ_STATES = (OBJ_ACTIVE, OBJ_PAUSED, OBJ_DONE, OBJ_FAILED, OBJ_CANCELLED)
OBJ_TERMINAL = frozenset({OBJ_DONE, OBJ_FAILED, OBJ_CANCELLED})

# États broker terminaux (orch_protocol).
BROKER_TERMINAL = frozenset({"completed", "failed", "timeout", "cancelled", "lost"})
