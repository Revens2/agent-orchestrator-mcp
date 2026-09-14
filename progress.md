# Mission CRITICAL — BUG1 (wait) + BUG2 (télémétrie) : état d'avancement

Base : main `a4aa694` (observabilité Telegram unifiée). Repo local aligné avant écriture.
## Diagnostic (reproduit localement avant fix)

- **BUG1** (`Store.wait_for_change`) : job déjà terminal à l'appel → `woke_by='timeout'`
  après le délai complet (repro : completed + `wait(timeout 3)` → timeout, 3,0 s).
  Cause : aucune vérification immédiate ; la condition `terminal` exigeait un
  *changement* d'état pendant l'attente, et `seq > since_seq > base_seq` ne
  couvrait jamais le retard client.
- **BUG2** (télémétrie null) : transition `RUNNING` et `event()` ne persistaient
  jamais `proc_*` (seul le heartbeat, toutes les 5 s). Un job qui produit sa
  sortie avant le premier heartbeat (ex. fake : `output_tail='pid=4724'` mais
  `process_alive/pid/started_at/child_count=null`) restait à null. Conséquence :
  `execution_health='idle'`, aucune détection `suspected_stall`/`stalled`.
  (Jobs longs à null = vieux runner sans instrumentation : cas conservé et
  maintenant distinguable via `telemetry_at`.)

## Correctifs (atomiques, vérifiés après chaque étape)

1. `src/orch_mcp/store.py::wait_for_change` : retour immédiat `terminal` si déjà
   terminal, immédiat `event` si `last_seq > since_seq` ; boucle corrigée
   (`state`/`terminal` prioritaires, `change` sur sortie/activité, `event` sur
   seq > since_seq). `timeout` conservé quand rien ne bouge.
2. `src/orch_mcp/store.py` : `_telemetry_values()` (mapping UNIQUE) +
   `_store_telemetry()` ; `transition()` et `event()` acceptent et persistent
   `telemetry` (retry idempotent inclus) ; `get_job` expose `telemetry_at` +
   `telemetry_age_s` (additif, formes existantes intactes).
3. `src/orch_mcp/runner_api.py` : `/runner/v1/transition` et `/event` transmettent
   les champs optionnels (`pid, proc_alive, proc_started_at, child_procs, tool`).
4. `src/orch_runner/runner.py` : `JobWorker._telemetry()` (best-effort, clés
   absentes = non observé) joint aux transitions et à chaque flush d'event ;
   `held()` réutilise le même snapshot (comportement fil conservé).
5. `docs/OPERATIONS.md` : sémantique wait immédiat + télémétrie-sur-transition/event.
6. Tests : `test_wait_terminal_is_immediate`, `test_wait_unseen_events_is_immediate`,
   `test_wait_race_to_terminal`, `test_telemetry_on_transition_and_event`,
   `test_telemetry_flows_over_http` ; `test_wait_timeout_and_wakeup` adapté
   (client à jour pour le cas bloquant).

## Preuves

- `pytest` (hors runtimes réels/Windows) : **112 passed, 1 skipped** ; `ruff check` propre.
- Matrice E2E locale (broker `src/` patché sur `:8802` + runner simulé HTTP) :
  `e2e/e2e_matrix_local.py` → **36/36 PASS** (`MATRIX_RESULT.json`) : 19 outils,
  race wait→`terminal`, terminal→immédiat, télémétrie non-null + `healthy`,
  cancel→arbre, mission failed→retry→validated, alertes ETUDE+NEXUS + dédup
  (`occurrences=2`), question fixture synthetic (dédup, due, sender file,
  answer single-use, replay `question_closed`).
- BUG1/BUG2 repro-avant + preuve-après exécutés (voir scripts temporaires E2E).

## Déploiement prod (VPS + PC)

- Backup/rollback : migration DB additive (`IF NOT EXISTS` / `ADD COLUMN`, NULL =
  non observé) ; ancien `src/` redéployé reste fonctionnel (chemin de rollback).
- **VPS FAIT (2026-09-14 ~00:28 UTC+2)** : backup `/root/orch-src-bak-<ts>` +
  `/root/orch-db-bak-<ts>.sqlite`, `src/` du commit `12ea5a7` déployé
  (tarball md5 vérifié), `systemctl restart orch-mcp`, `/health ok`
  (reaper 2,7 s). Gateway non touchée (aucun changement).
- **Preuve BUG1 live prod** : `wait_for_change` (depuis le broker prod) sur job
  `completed` → `woke_by=terminal` en 0,0 s (`BUG1_LIVE_OK`).
- **Runner PC (main-windows-pc) EN ATTENTE** : 1 job opencode actif en `e2e`
  (`9dc5076e`, output récente — ne pas tuer). La MAJ runner
  (`install-runner.ps1`) exige aucune activité (sinon job `lost`). Dès le job
  terminé : MAJ runner, puis preuve BUG2 live (job fake → télémétrie non-null
  dès `running`, `healthy`). Sans MAJ : broker corrigé, mais vieux runner =
  télémétrie toujours null + `idle` (pas de faux stall — comportement voulu).

## Erreurs

- (aucune erreur bloquante restante côté code ; E2E timeouts bornés non déclenchés)

---

# Mission CRITICAL — Follow-through automatique (wait répétés jusqu'au terminal)

Base : main `8a0cbf9`. Le bug `wait_for_change` (job déjà terminal → attente
complète) restait corrigé et prouvé ; le problème restant était le contrat de
suivi : après `agent_job_start`/`agent_mission_create`, rien n'imposait au
caller de continuer jusqu'au terminal (repro : start → `{job_id, state,
created}` sans `must_follow`/`next_tool` ; wait timeout → `woke_by=timeout`
sans `should_continue`, indistinguable d'une fin).

## Correctif (additif, sans migration DB, runner inchangé)

1. `src/orch_mcp/store.py` : `follow_for_job()` / `follow_for_wait()` (bloc
   machine-lisible : `must_follow`, `terminal`, `should_continue`, `next_tool`
   (`agent_job_wait` / `agent_job_get`), `wait_timeout_s`, `until=terminal`,
   `since_seq`) ; `wait_for_change` enrichi (retour immédiat ET boucle) ;
   nouveau `wait_for_mission(mission_id, since_seq, timeout_s)` borné ≤ 60 s
   (état mission + job + `next_tool` = `agent_mission_wait` tant que
   `executing` non terminal, `agent_mission_validate` quand
   `needs_validation`/`incomplete`/`blocked`).
2. `src/orch_mcp/tools.py` : descriptions MCP renforcées (DOIT rester dans le
   même tour, rappeler après chaque non-terminal, timeout ≠ fin, arrêts
   autorisés uniquement : `waiting_for_user`, entrée réellement requise,
   `failed/timeout/cancelled/lost` remonté) ; `agent_job_start` /
   `agent_mission_create` retournent `follow_up` + `terminal` /
   `should_continue` / `next_tool` / `since_seq` / `until`, avec
   `fire_and_forget` explicite (opt-out) ; nouvel outil lecture
   `agent_mission_wait`.
3. `src/orch_gateway/politique.py` : `agent_mission_wait` en lecture (20 outils) ;
   `src/orch_mcp/server.py` : INSTRUCTIONS avec contrat de suivi.
4. `README.md` (20 outils), `docs/OPERATIONS.md` (flux + CONTRAT DE SUIVI).
5. Tests : `tests/test_followthrough.py` (7 tests : helpers, timeout→rappel,
   terminal immédiat, waits répétés→terminal, séquence job complète,
   séquence mission→needs_validation→validated, failed→validate) ;
   `tests/test_mcp_tools.py` (surface 20 outils + contrat via MCP +
   fire-and-forget).

## Preuves

- `pytest` (hors runtimes réels/Windows) : **120 passed, 1 skipped** ; `ruff check` propre.
- Matrice E2E locale (broker `src/` patché sur `:8802` + runner simulé) :
  `e2e/e2e_matrix_local.py` → **42/42 PASS** (36 antérieurs + 6 follow-through :
  start→follow, timeout→recall, waits répétés, mission_create→mission_wait,
  mission_wait sur validée, fire-and-forget).
