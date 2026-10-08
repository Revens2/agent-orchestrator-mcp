# Autopilot V1 — progress (snapshot, réécrit en place)

Objectif : P0 sécurité + E2E réels (mission « correction P0, intégration et validation E2E », 2026-10-09).
Étape courante : J2 code terminé ; J3 (runner isolé) **en attente d'autorisation utilisateur**. État : **BLOCKED**.

## Fait (branche feat/autopilot-v1)
- J0 : dépôt propre @2177473 ; audits A/B/C read-only → `docs/autopilot/audit/{windows,vps,arch}-audit.md`.
- J1 : Opus session `BzGGPrx51TyIUUefSvfPTj4s` (modèle servi NON attesté : `served_model_last=null`) :
  NO-GO design mono-compte → GO conditionnel : compte runner A + compte agent B, enveloppe Ed25519, pas de redéploiement broker.
- J2 `0bb427b` : preuves runner_observed signées (orch_protocol/evidence.py), local_write sans clé/SID refusé,
  variable déclarative supprimée, gate NO-GO/ambigu bloquant, 429 agent sans effet, balise non fiable neutralisée.
- J2 `5a7dd8f` : cockpit Bearer obligatoire (+ relais jeton gateway), codes HMAC à clé serveur, garde disque,
  watchdog compatible gate, réponses illisibles gérées.
- J2 (ce commit) : runner isolé `orch_runner/isolation.py` (CreateProcessWithLogonW sous B, Job Object,
  sondes OS sous B, manifeste avant/après sans suivre les reparse points, ADS, enveloppe zlib signée),
  CLI `evidence-keygen | set-agent-password | attest`, `deploy/windows/{install-autopilot-runner,fix-tools-acl}.ps1`.

## Décisions
- Broker /srv/orch NON redéployé (hotfix live non versionnés, audit B) ; preuve transportée en tête de result_summary.
- Opus local : 127.0.0.1:8796 à jeton (pas d'ouverture réseau) ; tant que modèle non attesté => PENDING_GATE.
- main-windows-pc : jamais local_write (Config refuse).

## Blocages (action / autorisation utilisateur)
1. `fix-tools-acl.ps1 -Execute` (C:\Tools, C:\projet\opencode-rotate écrits par tous, exécutés sous Juliann).
2. Python « all users » + `install-autopilot-runner.ps1 -Execute` (2 comptes) + auth OpenCode de B + clés/jeton sous A.
3. `b-fix-orch-runner-acl.ps1 -Execute` (CodexSandboxUsers lit .orch-runner ; option auth OpenCode).
4. VPS : ajout `autopilot-win-01` à ORCH_RUNNER_TOKENS = restart orch-mcp (broker partagé).
5. VPS : redéploiement staging (cockpit.token, opus.token, consent.key), garde nft 8812 optionnelle.
6. Exposition publique cockpit (nginx + OAuth) : non faite, décision séparée.

## Validations
- Suite complète : voir commit (pytest), ruff : 3 findings préexistants hors périmètre (oauth.py, hermes, real_runtimes).
- semgrep pre-commit : 0 finding.
- Machine réelle : identité jeton (SID, admin membre, intégrité), scan, ADS, jonction testés sous Juliann.
