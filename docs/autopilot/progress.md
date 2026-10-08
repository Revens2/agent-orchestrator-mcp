# Autopilot V1 — progress (snapshot, réécrit en place)

Objectif : superviseur durable ChatGPT Web -> VPS -> broker -> OpenCode, staging + PR, pas de prod sans P0.
Étape courante : J3 (client broker réel, cockpit MCP, service, politique gateway).

## Fait
- J0 : clone propre `e2e/autopilot-v1`, branche `feat/autopilot-v1` (base main 883133d). Clone dirty
  `e2e/agent-orchestrator-mcp` (feat/pc-fixe-bootstrap) non touché. Baseline 225 pass / 8 skip.
- J1 : audits A (broker/API), B (OpenCode/PC/Brave), C (sécurité) en lecture seule ; revue Opus
  (session `45lmRkmrXFuA5CFQr1dx0guD`) : GO staging read_only, cockpit séparé, approve avec code hors bande.
- J2 : `src/orch_autopilot/{__init__,db,policy,broker,gate,supervisor}.py` + 31 tests (`tests/test_autopilot_supervisor.py`).

## Décisions
- Opus non attesté (`served_model_last=null`) => toute décision structurante `PENDING_GATE` ; repli humain par code.
- `kind=external` refusé ; Brave désactivé (profil personnel) ; desktop GUI : aucun moteur opérationnel.
- Timeout OpenCode = `timeout_s=300` trop court côté job (pas un bug runner) ; défaut autopilot 1500 s.
- `opencode-rotate` (rotation NetBird/providers sur 429) jamais utilisé par l'autopilot (évasion de quota exclue).
- Port cockpit configurable ; 8803 déjà pris (runner nginx).

## Blocages actifs (action utilisateur requise avant prod)
- Confinement OS absent : runner sous compte personnel `Juliann`, pas d'ACL/AppContainer. Compte Windows dédié requis.
- Deux processus `orch_runner` sur le PC (venv + Python global) : à vérifier.
- `runner.token` lisible par le groupe `CodexSandboxUsers`.

## Reste
- J3 cockpit MCP + service systemd + politique gateway + intégration vs vrai broker in-process.
- J4 chaos/sécurité, smoke e2e, staging VPS (rollback), PR non mergée.

## Validations
- `python -m pytest -q tests/test_autopilot_supervisor.py` : 31 passed. ruff OK.
