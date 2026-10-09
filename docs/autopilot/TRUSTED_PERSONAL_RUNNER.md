# Pivot Autopilot V1 — mode `trusted_personal_runner` (2026-10-09)

Intention utilisateur explicite : AUCUN compte Windows supplémentaire, aucune séparation de
profils, aucun runner ap-runner/ap-agent. L'autopilot utilise le compte Windows personnel
actuel et ses fichiers/projets/connexions existantes, avec gouvernance des irréversibles.
Référence : PR Revens2/agent-orchestrator-mcp #5, branche `feat/autopilot-v1`
(base GitHub `9eb8c862`, commit local de départ `01cec4a`, PAS PUSH).

## 1. Faits vérifiés

- Runner local (lecture métadonnées `~/.orch-runner/runner.toml`, 0 octet de secret lu) :
  `runner_id=main-windows-pc`, `isolation` ABSENTE (runner historique, compte personnel),
  runtimes activés : `opencode=True` (binaire WinGet présent), `claude-code=True`, `agy=True` ;
  workspaces : `e2e` (`read_only`+`workspace_write`), `agent-orchestrator-mcp` (`read_only`),
  `watchy` (`read_only`+`workspace_write`).
- Compte Windows courant : `desktop-v30kq19\Juliann`, SID `S-1-5-21-3782191491-1246939613-2125819924-1001`
  (profil personnel ; aucune prétention d'isolation).
- SSH : aliases présents (`vps-ia`, `vps-nexus`, `vps-etude`, `pc-travail`, … — noms seuls, 0 clé lue).
  Accès staging VPS **non disponible depuis cette session** : `ssh BatchMode vps-nexus` =>
  `Permission denied (publickey)`. Donc : AUCUN déploiement staging appliqué, AUCUN restart
  (ni autopilot, ni orch-mcp partagé, ni nginx), kill switch staging non touché.
- MCP RAG / GitHub / Brave : aucun connecteur MCP disponible dans cette session OpenCode
  (outils fichiers + shell uniquement). Démonstration RAG non sensible et GitHub read-only
  NON exécutées ici : connecteurs à raccorder côté runner personnel, sans mensonge.
  Question RAG et `read-only GitHub` restent à démontrer quand les MCP seront branchés.
- `workspace_write` du broker N'EST PAS un sandbox (rappel) ; `cwd` n'est jamais une preuve
  d'enfermement ; aucun scan global du profil effectué (workspaces explicites uniquement).

## 2. Delta minimal implémenté (branche PR, PAS PUSH)

- `src/orch_autopilot/policy.py` : `Config.trust_mode` (`isolated` par défaut) +
  `Config.personal_ack` ; constante `PERSONAL_ACK=I-UNDERSTAND-NO-ISOLATION` ;
  env `AUTOPILOT_TRUST_MODE` / `AUTOPILOT_PERSONAL_ACK`. En mode personnel, `local_write`
  sur `legacy_runners` autorisé PAR CONFIGURATION explicite ; la branche `isolated`
  (blocage legacy + exigence pubkey+SID) est INCHANGÉE. `legacy_runners` conservé.
- `src/orch_protocol/evidence.py` : niveau `runner_observed_same_user` documenté ;
  `check_payload(..., allow_same_user=True)` réservé au mode personnel : contrôles de liaison
  et de manifeste intacts, contrôles d'isolation remplacés par contrôle même-utilisateur
  (`agent_sid == runner_sid`). `externally_verified` jamais produit (aucun chemin de code).
- `src/orch_autopilot/supervisor.py` : en mode personnel, `local_write` sans enveloppe
  (runner historique) suit le contrat de sortie honnête (`agent_reported`, jamais DONE sans
  marqueurs/artefacts) au lieu de `NEEDS_AUDIT` systématique ; enveloppe même-SID valide =>
  `runner_observed_same_user`. Approbation par tentative toujours exigée par défaut.
- `deploy/autopilot/autopilot.env.example` + `docs/autopilot/RUNBOOK.md` : activation et
  limites documentées. Sauvegardes pré-modification : `.bak-personal-20261009/` (3 fichiers,
  retiré du worktree avant commit — voir §6).

## 3. Tests exacts (Windows, 2026-10-09)

- `pytest tests/test_autopilot_personal.py` : **10 passed**. Négatifs : défaut isolé bloque
  toujours `local_write` sur `main-windows-pc` (`local_write_requires_isolated_runner`) ;
  isolé exige toujours pubkey+SID ; personnel sans accusé exact refusé (`personal_ack_required`,
  y compris valeur approchante) ; mode inconnu refusé ; faux DONE sans marqueur jamais DONE ;
  enveloppe rejouée d'un autre job rejetée (`job_id_mismatch`) ; gate d'approbation par défaut
  (`NEEDS_APPROVAL`, 0 job parti). Positifs : personnel `agent_reported` DONE ; même-SID signé
  `runner_observed_same_user` DONE.
- Non-régression : `test_autopilot_evidence + supervisor + integration + guards + chaos +
  personal` : **146 passed, 2 skipped** (mêmes 2 skips pré-existants).
- E2E local non destructif (FakeBroker, config personnelle `local_write_needs_approval=False`,
  fixture `.e2e-personal/` SOUS worktree) : `read` DONE (`agent_reported`) puis `local_write`
  DONE (`agent_reported`), artefact `out/personal-proof.txt` (82 o) écrit puis **mis en
  corbeille** via `trash.ps1` (journal : avant = absent, après = 82 o untracked, final = propre).
  Preuve d'effet = manifeste/git-status, AUCUNE attestation OS annoncée.

## 4. Activation sur main-windows-pc (ses données)

1. Ne rien changer au runner actuel (ni jeton, ni services, ni `runner.toml`).
2. Côté superviseur (staging `/srv/autopilot`, quand l'accès SSH sera disponible) :
   renseigner `AUTOPILOT_TRUST_MODE=trusted_personal_runner`,
   `AUTOPILOT_PERSONAL_ACK=I-UNDERSTAND-NO-ISOLATION`,
   `AUTOPILOT_ALLOWED_KINDS=read,decision,local_write`, redémarrer **le seul service
   Autopilot** (backup préalable via `deploy/autopilot/snapshot.sh`), vérifier `/health`,
   kill switch, et 1 objectif `read` puis 1 `local_write` sur fixture.
3. Mission type non sensible : recherche RAG → plan → artefact local → validation
   (marqueurs + artefacts déclarés), chaque `local_write` approuvé hors bande
   (fichier 0600 `/srv/autopilot/data/approvals/`).
4. Interdits automatisés dans cette mission : paiement, publication publique, IAM,
   destruction, divulgation de secrets, modifications prod, push branches protégées
   (confirmation explicite hors PC ou `action_hash` cockpit). Pas de bypass UAC.

## 5. Statut staging exact et blocages résiduels

- Staging `/srv/autopilot` : **inchangé dans cette session** (dernier état connu : code
  `9753172`, kill switch ON, cockpit 401 sans jeton). Vérification STOP/kill-switch :
  NON rejouée (pas d'accès SSH) — le kill switch n'est contourné par aucun chemin du delta
  (lu dans la transaction d'envoi ; tests chaos PASS).
- BLOCKED résiduels précis :
  a) accès SSH VPS sans clé dans cette session (commande à rejouer avec clé :
     `ssh vps-nexus "systemctl is-active autopilot"`) ;
  b) MCP RAG/GitHub/Brave non branchés dans cette session (démo RAG→plan→artefact à finir) ;
  c) déploiement staging + restart service Autopilot seul à faire avec accès (commandes dans
     RUNBOOK ; ne JAMAIS redémarrer orch-mcp/nginx partagés — sinon laisser BLOCKED).
- Rollback : `git revert` du commit local ci-dessous + `deploy/autopilot/rollback.sh`
  côté staging ; runner historique intact (aucune modification), autres projets non affectés.

## 6. Commit local (PAS PUSH, PAS MERGE)

Commit local dans le worktree `autopilot-v1` (fichiers : `policy.py`, `evidence.py`,
`supervisor.py`, `tests/test_autopilot_personal.py`, `autopilot.env.example`, `RUNBOOK.md`,
`TRUSTED_PERSONAL_RUNNER.md`, `progress.md`). Aucun secret commis (0 clé/token/SID privé :
le SID personnel ci-dessus est un identifiant de compte, pas un secret ; vérifié par
`git diff` relu). Backups `.bak-personal-20261009/` exclus du commit (mis en corbeille).
