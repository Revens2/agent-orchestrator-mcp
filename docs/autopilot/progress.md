# Autopilot V1 — progress (snapshot, réécrit en place)

Objectif : P0 sécurité + E2E réels (mission « correction P0, intégration et validation E2E », 2026-10-09).
État : **BLOCKED** — code P0 corrigé et déployé en staging ; T1/T2/T5/T7-réel/T9 attendent des autorisations.

## Fait (branche feat/autopilot-v1, PR #5 draft)
- J0 : audits A/B/C read-only → `docs/autopilot/audit/`. J1 : Opus `BzGGPrx51TyIUUefSvfPTj4s` (consultatif,
  modèle servi NON attesté) : design 2 comptes + enveloppe signée, broker partagé non redéployé.
- `0bb427b` preuves runner_observed, gate NO-GO bloquant, 429 agent sans effet, balise neutralisée.
- `5a7dd8f` cockpit Bearer obligatoire, codes HMAC, garde disque, watchdog séparé.
- `aa2d8db` runner isolé (CreateProcessWithLogonW, sondes OS sous B, manifeste, ADS, jonctions), scripts Windows, snapshot/restore.
- `4849312` restore.sh corrigé (bug révélé par T10 réel). `9753172` revue J7 : SID processus agent fail-closed, lien prompt, hardlinks.
- Staging VPS `/srv/autopilot` = `9753172`, kill switch ON, `/health` OK, cockpit 401 sans jeton, `orch-mcp`/nginx intacts.
  Sauvegardes vérifiées : `/srv/autopilot-backups/` (b31093e, aa2d8db ×2).

## Tests
- Windows : 381 passed / 11 skipped @9753172. Linux VPS (autopilot+gateway+runner) : 154 passed / 5 skipped @aa2d8db.
- E2E réels : T3 OK (2 sessions MCP neuves, même handoff, 8 outils, aucun code), T4 OK sans job (restart => état identique),
  T10 OK aller-retour b31093e↔aa2d8db, T8 live = gate non configuré => PENDING_GATE.
- Non exécutés (pas de runner isolé) : T1, T2, T4/T5 avec job réel, T6 réel, T7 sous B, T9 complet.
  Décision : aucun job agent sur main-windows-pc (compte perso admin).

## Autorisations requises (ordre, revue Opus J7)
1. `deploy/windows/fix-tools-acl.ps1 -Execute` (élévation existante vers Juliann via C:\Tools).
2. `audit/windows/b-fix-orch-runner-acl.ps1 -Execute [-IncludeOpencodeAuth]` + rotation jeton main-windows-pc.
3. Nettoyage disque VPS (92 %) : /tmp/geniso 15G, rétention vault-mirror-trash, docker prune après inventaire.
4. Python 3.11 « all users ». 5. `install-autopilot-runner.ps1 -Execute` (comptes ap-runner/ap-agent).
6. Auth OpenCode de ap-agent (clé dédiée plafonnée). 7. VPS : ajout `autopilot-win-01` à ORCH_RUNNER_TOKENS (restart orch-mcp partagé).
8. Ensuite : T1 (`orch_runner attest`), T2, T5, T7 sous B, T9. Plus tard : jeton Opus 8796, exposition publique.

## Risques résiduels V1
Voir `SECURITY-P0.md` (egress de B, persistance profil B, état initial du workspace non vérifié, opus non attesté).
