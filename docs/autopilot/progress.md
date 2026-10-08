# Autopilot V1 — progress (snapshot, réécrit en place)

Objectif : superviseur durable ChatGPT Web -> VPS -> broker -> OpenCode, staging + PR, pas de prod sans P0.
Étape courante : J4 terminé (staging actif, kill switch ON, PR ouverte non mergée). Prod : NO-GO.

## Fait
- J0 clone propre `e2e/autopilot-v1`, branche `feat/autopilot-v1` (base main 883133d) ; clone dirty pc-fixe non touché.
- J1 audits A/B/C read-only ; revue Opus session `45lmRkmrXFuA5CFQr1dx0guD` (J1 + J4).
- J2 `8182736` superviseur/DB/FSM/outbox/consentements. J3 `c263467` client broker, cockpit, service, politique gateway.
- J4 `0223cac` chaos ; `b31093e` local_write désactivé par défaut + approbation par tentative.
- Staging VPS : `/srv/autopilot` (VERSION b31093e), `autopilot.service` 127.0.0.1:8812, schema v2, kill switch ON.
  `/srv/orch`, nginx, orch.db non modifiés. Sauvegarde précédente : `/srv/autopilot/backup-0223cac/`.
- Smoke réel `obj-da98f4e71ec94fb9` (OpenCode, runner main-windows-pc) : idee DONE, landing DONE
  (2 fichiers dans `e2e/autopilot-smoke/landing-1`, test relancé indépendamment : PASS), publier PENDING_GATE ;
  `systemctl restart` en plein job : 0 doublon (2 jobs, 2 clés).

## Décisions
- Opus non attesté (`served_model_last=null`) => PENDING_GATE ; repli humain par code hors bande.
- Dérogation datée 2026-10-08 : un local_write de smoke exécuté avant compte Windows dédié (prompt sans contenu web,
  résultat vérifié). Depuis `b31093e` : local_write refusé par défaut.
- external refusé ; Brave désactivé (profil personnel) ; aucun moteur desktop GUI opérationnel.
- `opencode-rotate` (rotation NetBird/providers) jamais utilisé : évasion de quota exclue.

## Blocages P0 (action utilisateur)
- Compte Windows dédié non admin + ACL pour le runner (aujourd'hui compte personnel, confinement par code).
- Exposition publique du cockpit (2e orch-gateway + nginx + OAuth) : décision.
- Canal hors bande opérateur (aujourd'hui fichiers 0600 `/srv/autopilot/data/approvals`).
- Egress runner + retrait technique d'opencode-rotate (recommandation Opus).
- Constatés : 2 processus orch_runner sur le PC ; `runner.token` lisible par `CodexSandboxUsers` ;
  code live broker ≠ main (store/tools du 21/09, pas de VERSION) ; disque VPS 92 %.

## Validations
- `python -m pytest -q` : 301 passed / 9 skipped (b31093e, base 225 + 76 autopilot).
- ruff OK ; semgrep pre-commit 0 finding.
