# progress.md — reprise PC (runner 0.3.0 / broker recovery)

## Snapshot 2026-09-14 — Mission CRITICAL Windows (Muse Spark, exécution directe, sans sous-agents)

- Config effective : modèle de session `muse-spark-1.3-contributor-free` conservé (aucun changement) ;
  `opencode.jsonc` global sur `experiential/deepseek-v4.1-flash` + provider apinex `free/muse-spark-1.3`
  (constat, secrets non exposés). Workspace MCP `e2e` = `C:\Users\Juliann\orch-workspaces\e2e`.
  Dépôt modifié : `C:\Users\Juliann\missions\agent-orchestrator-mcp\repo` (aucun clone concurrent).
  HEAD local `1b2a201` (plus récent que `06805cfb` annoncé ; `8e39297` inclus dedans).

## Diagnostic
- `queued` survit au offline (SQLite VPS + claim au retour). `claimed/starting/running` mouraient :
  hello avec `held=[]` → `lost` immédiat ; reaper (bail 60 s) → `lost` ; `offline_kill_s=120 s` →
  kill local de l'arbre. Aucun journal local (`jobs/` effacé au boot), sortie non flushée perdue,
  aucune reprise de session, ré-exécution aveugle interdite par design (sécurité `workspace_write`).
- Observé en live : job `bad466bf` offline 124 s → `job_offline_abandon` → tué puis `lost`.

## Design retenu (compatible, protocole v1 inchangé)
- Pas de nouvel état : `recovery_state` (`recovering`/`suspended`) + `recovery_detail` + `resume_count`
  (migration `ALTER TABLE ADD COLUMN` idempotent), grâce `RECOVERY_GRACE_S = 4 h`, nouveaux events
  `runner_recovering/resume_attempt/resume_failed/job_suspended` (borne `job_get` : +3 champs).
- Premier bail expiré ou hello sans détention → parking `suspended` (bail prolongé, jamais relancé) ;
  seconde expiration → `lost`. Hello avec journal (`recovering:true`, même fencing) → rattachement
  au nouvel epoch (fencing/epoch = anti double-ownership + mutex single-instance).
- Journal `%USERPROFILE%\.orch-runner\recovery\<job_id>.json` atomique avant spawn (identité, fencing,
  runtime/mode/workspace, prompt+hash, session, PID+start, curseurs), quarantaine `.corrupt-*`, soldé
  au terminal ; `stale_fencing/unknown_job` → solde anti-fantôme.
- Reprise : session runtime (`opencode --session`, `claude --resume`) ou rebuild frais (`fake`/`read_only`)
  avec borne `resume_attempt` ; sinon parking explicite (bail entretenu). Plus de kill offline :
  processus conservé, sortie bufferisée/reflushée. Orphelin PID = diagnostique (anti-recyclage
  PID+create-time), jamais rattaché.

## Fichiers / migrations
- `src/orch_protocol/__init__.py` : `RECOVERY_GRACE_S`, `RECOVERING/SUSPENDED`, 4 events.
- `src/orch_mcp/store.py` : migration 3 colonnes, `hello`/`heartbeat`/`reap` recovery, `_park_suspended`,
  `transition` solde recovery au terminal, `_job_view` +3 champs, `_col` tolérant (rollback OK).
- `src/orch_runner/recovery.py` (new), `winproc.py` (+`process_create_time`/`is_same_process`),
  `adapters.py` (+`resume()` opencode/claude, None sinon), `runner.py` (journal, `_supervise`,
  flush différé, parked, réconciliation boot, guards stop, `VERSION=0.3.0`).
- Tests : `tests/test_recovery.py` (new, 8 tests), `test_store.py`/`test_supervision.py`/`test_runner_windows.py`
  mis à jour (park-then-lost, resume fake, offline-keeps-job).

## Tests + sorties
- `pytest tests/ --ignore=test_real_runtimes` : **107 passed, 1 skipped** (7 min, Windows réel fake agent).
- `ruff check src tests` : **All checks passed**.
- E2E recovery : restart runner → `completed` (même job, `attempt=1`, event `runner_recovering`) ;
  sans journal → `running`+`suspended`, jamais relancé ; offline 20 s (broker down) → `completed`.
- Scénario Windows réel sans tuer de job utilisateur : tests sur brokers/tmp isolés uniquement ;
  live `main-windows-pc` (epoch 41, job actif `cc38af32`) **jamais touché** (lecture `status.json` seule).

## Déploiement live / rollback — DÉPLOIEMENT DIFFÉRÉ (job actif)
- Backup : `%USERPROFILE%\.orch-runner\backup-pre-recovery-20260914\` (`runner.toml`, `status.json`).
  Vérifié : 1 job actif → **pas de restart du runner live** (mise à jour seulement à `active_jobs=[]`).
- Déploiement quand libre : VPS `src/`+`deploy/install.sh`+restart ; PC `install-runner.ps1`
  (journal `recovery/` préservé, rattachement auto). Rollback : ancien `src/` sur DB migrée = OK
  (colonnes ignorées) ; runner : `-Uninstall` + restaurer backup + ancien `app/`.
- Commit : voir SHA ci-dessous. Push après tests : oui (branche main).

## Limites restantes
- Sessions `codex`/`agy` non résumées v1 (parking) ; syntaxe `codex exec resume` non câblée (fallback sûr).
- Grâce 4 h : offline > 4 h → `lost` (décision humaine, comme avant mais différée et tracée).
- `claim`/`heartbeat` portent `recovering/suspended` en extra (ignorés par vieux broker : compatible).
- Deux PC partageant `runner_id`+jeton : flap d'epoch préexistant, inchangé (1 PC = 1 runner_id).
