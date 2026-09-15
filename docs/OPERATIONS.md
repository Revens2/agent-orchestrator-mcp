# Exploitation — orchestrateur d'agents

## Installation VPS (vps-etude)

```bash
# depuis le dépôt : copier src/orch_* + deploy + requirements.lock vers /srv/orch
sudo python3 -m venv /srv/orch/venv
sudo /srv/orch/venv/bin/pip install --require-hashes -r /srv/orch/requirements.lock
sudo install -d -m 750 /srv/orch/secrets   # puis créer orch.env d'après deploy/orch.env.example
sudo bash /srv/orch/deploy/install.sh      # units + nginx (sauvegarde vhost, nginx -t, auto-restauration)
```

`ORCH_GW_CONSENT_HASH` reprend l'empreinte de consentement de tasks (même phrase de passe humaine).
Pour une phrase dédiée : générer une nouvelle empreinte PBKDF2 au format de `orch_gateway/consentement.py`.

## Installation PC (Windows)

1. `%USERPROFILE%\.orch-runner\runner.toml` (hors Git, **pas dans AppData** : Claude Desktop
   et les autres apps MSIX virtualisent `AppData\Local` pour leurs processus enfants) :
   `runner_id`, `broker_url = "http://10.200.114.203:8803"`, `max_parallel`, `[runtimes.*]`, `[workspaces.*]`.
2. Jeton : `python -m orch_runner gen-token` → jeton chiffré DPAPI sur le PC, affiche
   `runner_id:sha256`. Mettre **cette empreinte** dans `ORCH_RUNNER_TOKENS` sur le VPS puis
   `sudo systemctl restart orch-mcp`.
3. `powershell -ExecutionPolicy Bypass -File deploy\windows\install-runner.ps1`
   → venv + tâche planifiée `orch-runner` (au logon + watchdog 1 min, instance unique).

Limite assumée : le runner tourne dans la session de l'utilisateur (les runtimes utilisent
l'authentification du profil). Session fermée = runner `offline`, les jobs restent `queued`.

## Ajouter un workspace

```toml
[workspaces.mon-projet]          # id : [a-z0-9_-], 64 car. max
path = 'C:\Users\Juliann\Desktop\MonProjet'   # absolu, disque fixe, sans jonction/symlink
modes = ["read_only"]            # ajouter "workspace_write" si l'agent peut modifier
description = "…"
```

Puis relancer le runner (`Stop-ScheduledTask orch-runner; Start-ScheduledTask orch-runner`) :
les workspaces sont annoncés au `hello`. Un chemin invalide est ignoré (log `workspace_invalid`).

## Ajouter / activer un runtime

- `[runtimes.<id>] enabled = true`, `exe = '<chemin absolu .exe>'` (les shims `.cmd` sont refusés).
- Nouveau runtime : classe `Adapter` dans `src/orch_runner/adapters.py` (argv, transport du prompt,
  mapping des modes, parsing de fin), id ajouté à `orch_protocol.RUNTIMES` et au `Literal` de
  `orch_mcp/tools.py`, tests `ORCH_REAL=<id> pytest tests/test_real_runtimes.py`.

## Politique de permissions (locale au runner)

`default_permission_policy` dans `runner.toml` (défaut `unattended`). Jamais fournie par ChatGPT/broker :
le contrat MCP ne transporte ni flags, ni commande, ni argv. Les barrières structurelles (allowlists
runner/runtime/workspace/mode, cwd imposé, Job Object, leases/fencing, max_parallel, auth) restent actives.

- `unattended` : en `workspace_write` chaque runtime reçoit son mode sans aucune demande d'autorisation
  (l'agent peut exécuter du shell avec les droits de l'utilisateur Windows, y compris hors workspace).
- `guarded` : éditions autorisées, tout ce qui demanderait une autorisation est refusé sans attente ;
  opencode n'accepte alors que `read_only`.
- `read_only` reste lecture seule dans les deux politiques (vérifié par les tests réels `SHOULD_NOT_EXIST.txt`).

Désactiver temporairement : `default_permission_policy = "guarded"` puis relancer le runner
(`Stop-ScheduledTask orch-runner; Start-ScheduledTask orch-runner`). Retirer `workspace_write` d'un
workspace reste le verrou le plus fort.

| Runtime | Prompt | read_only | workspace_write `unattended` | workspace_write `guarded` | Limite connue |
|---|---|---|---|---|---|
| claude-code | stdin | `--permission-mode plan --permission-prompts none` | `--permission-mode bypassPermissions` | `--permission-mode acceptEdits --permission-prompts none` | charge settings/hooks/MCP utilisateur |
| codex | stdin | `-s read-only -c approval_policy="never"` | `-s danger-full-access -c approval_policy="never"` | `-s workspace-write -c approval_policy="never"` | sandbox + approbation toujours explicites (jamais `config.toml`) |
| agy | `--print=` | `--mode plan --sandbox` | `--mode accept-edits --dangerously-skip-permissions` | `--mode accept-edits --sandbox` | workspace via `--add-dir` + prompt : pas de confinement strict du dossier |
| opencode | argv après `--` | `--agent plan` | `--agent build --auto` | refusé | `-m` épinglé par `model` ; `--auto` n'honore que les `deny` explicites de la config opencode |
| claude-desktop | fichier `--prompt-file` | bridge UIA `claude_desktop_bridge.py` (profil vérifié, UI sérialisée) | refusé | refusé | read_only seul (confinement workspace non démontrable) ; vole le focus pendant le job |

### OpenCode

Modèle épinglé dans `runner.toml` (`model = "opencode/muse-spark-1.3-contributor-free"`, OpenCode Zen gratuit,
auth `auth.json` du profil) : le runner ne dépend pas du modèle par défaut global d'opencode.
`--version` ne suffit pas : au démarrage (et à chaque relance) le probe fait une génération minimale
`Reply OK.` en agent plan ; échec → `available=false`, `reason=runtime_not_ready…`. Résultat mis en cache
pour la session (pas d'appel LLM par heartbeat). `probe_generation = false` pour le désactiver.

## Rotation / révocation du jeton runner

```powershell
python -m orch_runner gen-token --force     # nouveau jeton DPAPI + nouvelle empreinte
```
VPS : remplacer l'empreinte dans `ORCH_RUNNER_TOKENS`, `sudo systemctl restart orch-mcp`.
Révocation immédiate : retirer l'entrée et redémarrer orch-mcp (le runner reçoit 401).

## Santé

```bash
curl -s http://127.0.0.1:8802/health          # broker (DB + reaper), VPS
curl -s http://127.0.0.1:8801/health          # gateway, VPS
curl -s http://10.200.114.203:8803/health     # depuis le PC (NetBird)
```
```powershell
%USERPROFILE%\.orch-runner\venv\Scripts\python.exe -m orch_runner status   # statut runner local
```

## Supervision d'une mission longue (depuis ChatGPT)

Flux recommandé : `agent_runner_list` → `agent_runner_inspect(runner_id)` →
`agent_mission_create(objective, acceptance_criteria, …)` → boucle
`agent_mission_wait(mission_id, timeout_s=25)` (ou `agent_job_wait(job_id,
timeout_s=25, since_seq=…)` sur la tentative courante) + `agent_job_get`
(champ `execution_health`, couches `broker_health`/`runner_health`/
`runtime_process_health`) → `agent_job_events(job_id, after_seq)` pour le
journal → à la fin du job : `agent_mission_validate(mission_id, verdict)`.

CONTRAT DE SUIVI (non négociable, sauf `fire_and_forget=true` explicite) :
chaque retour de `agent_job_start` / `agent_mission_create` / `agent_job_wait` /
`agent_mission_wait` porte un bloc machine-lisible (`must_follow`, `terminal`,
`should_continue`, `next_tool`, `wait_timeout_s`, `until=terminal`,
`since_seq`/`last_event_seq`). Tant que `terminal=false` — Y COMPRIS
`woke_by=timeout` — le caller DOIT rappeler `agent_job_wait` /
`agent_mission_wait` avec `since_seq=last_event_seq` dans le MÊME tour, sans
répondre à l'utilisateur. Un timeout pendant que le process continue n'est
jamais une fin. Répondre uniquement après résultat terminal / validation.
Seuls vrais arrêts : question `waiting_for_user` ouverte
(`agent_question_list`), entrée utilisateur réellement requise, ou job
`failed`/`timeout`/`cancelled`/`lost` à remonter explicitement.

- `completed` (exit 0) ≠ mission réussie : le job passe la mission en `needs_validation`,
  tout autre terminal (`failed`, `timeout`, `cancelled`, `lost`) en `incomplete`.
- `agent_mission_retry` crée une NOUVELLE tentative (nouveau job) de la même mission,
  dans la limite `max_attempts`, sur décision explicite après examen du journal.
  Le serveur ne relance jamais seul, surtout pas une mission `workspace_write`.
- `lost` = issue inconnue : `agent_job_get` + `agent_job_events` (`lease_expired`,
  `runner_disconnect`) montrent la cause observable (runner offline ? processus mort ?).
  Vérifier l'état réel du workspace avant tout retry.
- Stalls : événement `suspected_stall` (silence ≥ 10 min, processus vivant) puis
  `stalled` (≥ 30 min). Notification seule : décider humainement (notify/cancel/resume
  via `agent_job_cancel` ou `agent_mission_retry`). Pas de télémétrie (vieux runner) =
  pas de faux signal (`execution_health` reste `idle`/`healthy`, champs à null).
  Depuis la version télémétrie-sur-transition/event, le runner joint son snapshot
  (pid, vivant, enfants, outil) à la transition `running` et à chaque event de
  sortie : un job qui produit de la sortie a toujours sa télémétrie (`telemetry_age_s`
  donne l'âge de la dernière observation ; `telemetry_at=null` + champs null =
  runner sans instrumentation, jamais une panne).
- `agent_job_wait` évite le polling agressif pendant un tour actif (≤ 60 s). Réveil
  immédiat (sans attendre le timeout) si le job est déjà terminal (`woke_by=terminal`)
  ou si des événements non vus existent (`last_seq > since_seq` → `woke_by=event`).
  Quand le tour ChatGPT est fini, reprendre plus tard avec `agent_job_get` + `after_seq`.
- `agent_runner_inspect` : versions/capacités des runtimes, workspaces allowlistés,
  git par workspace (`branch`/`head`/`dirty`, null si non observé), jobs actifs enrichis.
  Jamais de secrets, jamais de dump d'environnement (`current_command_sanitized`
  est toujours null : les adapters n'exposent pas les commandes, par design).

## Alertes infra unifiées ([ETUDE]/[NEXUS])

Persistance normalisée des alertes sortantes dans le broker (`infra_alerts`),
l'historique Telegram n'étant jamais la source de vérité. Écriture réservée
aux ingesteurs locaux du VPS (jamais via MCP) ; lecture ChatGPT via
`infra_alert_list` / `infra_alert_get` (read-only, bornés, redactés).

```bash
# Enregistrer (compte orch-app, loopback) : dedup par empreinte (fenêtre 1 h,
# occurrences+1 + escalade de sévérité au lieu d'une nouvelle ligne)
sudo -u orch-app env PYTHONPATH=/srv/orch/src /srv/orch/venv/bin/python \
  -m orch_mcp.alert_cli --db /srv/orch/data/orch.db \
  record --source etude --service orch-mcp --severity critical \
  --title "broker hors ligne" --detail "exit 1 ..." [--fingerprint ...]
# Lister / détail / acquitter / résoudre :
.../alert_cli list --source etude --state active --limit 20
.../alert_cli get --id <alert_id>
.../alert_cli ack --id <alert_id> ; .../alert_cli resolve --id <alert_id>
```

Format Telegram canonique (même vue que le MCP) :
`[ETUDE] [critical] <titre>` + timestamp UTC + détail borné.
`alert_cli record --telegram-format` affiche le message à envoyer via
`/usr/local/bin/send_telegram.sh` (Étude). Rétention 90 j (purge horaire),
plafond 5 000 lignes. Rollback : table additive ignorée par l'ancien `src/`
(sauvegarde `/root/orch-src-bak-*` avant chaque déploiement).

### Pull Nexus (automatisé, toutes les 5 min)

Le spool `/var/log/nexus-alerts.jsonl` du VPS Nexus est aspiré par
`deploy/nexus_alert_pull.sh` (cron `orch-app`, curseur = dernier `ts` traité,
résistant à la rotation ; doublons absorbés par déduplication broker).
Transport SSH : clé dédiée `/srv/orch/secrets/nexus_pull` (600 orch-app,
sans passphrase) → `ubuntu@10.200.61.52` (NetBird), restreinte côté Nexus par
`command="tail -n 200 ..."` + `no-pty,...` dans `~ubuntu/.ssh/authorized_keys`,
et `AllowUsers ... ubuntu@10.200.114.203` dans `sshd_config` (backup
`/root/sshd_config.bak-*`, `sshd -t` + `reload`, jamais de restart aveugle).
Host key Nexus épinglée dans `/srv/orch/.ssh/known_hosts` (vérifiée contre une
connexion de confiance avant ajout). Rotation : régénérer la clé, remplacer la
ligne `nexus-pull` sur Nexus, tester `sudo -u orch-app
/srv/orch/deploy/nexus_alert_pull.sh` (attend `RECORDED: n`).
Côté Nexus, les scripts installés (`nexus_alert_spool.sh` 700,
`nexus_fim_alert.sh` 750, `nexus_nightly_sec.sh` 700, backups
`/root/nexus-alerts-bak-*`) correspondent à la branche NEXUS
`feat/alerts-unified-spool` (non mergée : le merge déclenche le full deploy
prod CI → relecture utilisateur requise avant merge).

## Titres de conversations

`display_title` stable (fonction pure du premier objectif, jamais renommé) :
`agent_job_get` / `agent_job_list` / `agent_mission_get` l'exposent (calculé à
la lecture, aucune migration, identités ConvIA intactes). Seul OpenCode pose un
titre natif (`opencode run --title`, via le runner). Claude/Codex/AGY n'offrent
aucun mécanisme headless : fallback `display_title` broker uniquement.

## Session OpenCode corrompue (reprise)

Ne jamais réutiliser une session reconnue corrompue. Chaîne :
`opencode` (adapter : signature exacte via `match_corruption`, jamais le mot
`error`) → job `failed` (`session_corrupted:<signature>`, handoff minimal en
`result_summary`) → événement structuré `session_corrupted` → `agent_mission_retry`
crée une NOUVELLE session (`session_recreated`, handoff : objectif + job
abandonné + prochaine action, jamais le transcript).
Anti-boucle : 2 corruptions consécutives sur la même mission ⇒ `retry` refuse
(`session_corruption_loop`, cause config/plugin/auth à corriger) ; `max_attempts` borne le reste.
Journal : `agent_job_events` (`session_corrupted`, `session_recreated`).

## Questions en attente (Photon/iMessage + inbox ChatGPT)

État explicite `waiting_for_user` (table `pending_questions`), jamais deviné
sur `?` : l'agent émet `[[QUESTION]]...[[/QUESTION]]` (+ `[[OPTIONS]]a|b...`)
— parsé par le runner opencode → route runner `/runner/v1/question`
(authentifiée, fencing) — ou enregistrement direct (`question_cli record`,
`origin=mission|chatgpt-web`).

```bash
sudo -u orch-app env PYTHONPATH=/srv/orch/src /srv/orch/venv/bin/python \
  -m orch_mcp.question_cli --db /srv/orch/data/orch.db \
  record --origin mission --session-ref <job_id> --runtime opencode \
  --title "..." --question "..." [--option "1: ..." --option "2: ..."] [--notify-after-s 300]
.../question_cli due | .../question_cli list --status open
.../question_cli answer --id <qid> --answer "2" --from imessage
```

Après `notify_after_s` (défaut 300 s) sans réponse, `deploy/photon_dispatch.sh`
envoie UN message corrélé dédupliqué (titre, runtime/session, question bornée,
choix numérotés, correlation_id). Senders : `none` (défaut honnête → deferred),
`file` (spool E2E), `hermes`/`photon` (à brancher : deferred en attendant —
jamais de promesse d'envoi ; pas d'injection ChatGPT Web, techniquement non
supportée : l'inbox MCP `agent_question_list/get/answer` est le fallback vrai,
`agent_question_answer` étant single-use + expiration + `answer_from` audité).
Aucune réponse ne devient une commande shell (texte ≤500 car. stocké, relu à
la prochaine activité de LA session émettrice). Allowlist Photon vérifiée à la
réception par Hermes/Photon ; purge 7 j des réponses.

## Runner Hermes (hermes-vps)

Le runtime `hermes` n'utilise pas `orch_runner` : il est servi sur vps-etude par
`deploy/hermes-poller/poller_v2.py` (unité `hermes-orch-poller.service`,
`User=hermes-ops`, source live `/var/lib/hermes-ops/orch-poller-v2/`, journal
`/var/lib/hermes-ops/orch-jobs/{poller.db,runs/,health.json}`). Invocation :
`docker exec -i hermes hermes chat --query-file - -Q --yolo --accept-hooks
--pass-session-id` (prompt sur stdin, stdout+stderr -> `runs/<job8>.log`,
publiés en events `output` ; télémétrie `pid`/`proc_alive` à plat).

```bash
# deploiement v2.2 (backup + remplacement atomique ; NE PAS oublier runtime_support.py,
# importé par poller_v2.py depuis le même WorkingDirectory, ni le .service si changé)
B=/var/backups/hermes-poller-$(date +%Y%m%d-%H%M%S); sudo mkdir -p $B
sudo cp -a /var/lib/hermes-ops/orch-poller-v2/poller_v2.py /var/lib/hermes-ops/orch-poller-v2/runtime_support.py $B/
sudo cp -a /etc/systemd/system/hermes-orch-poller.service $B/ 2>/dev/null || true
sudo install -o hermes-ops -g hermes-ops -m 644 poller_v2.py /var/lib/hermes-ops/orch-poller-v2/poller_v2.py.new
sudo install -o hermes-ops -g hermes-ops -m 644 runtime_support.py /var/lib/hermes-ops/orch-poller-v2/runtime_support.py.new
sudo mv /var/lib/hermes-ops/orch-poller-v2/poller_v2.py.new /var/lib/hermes-ops/orch-poller-v2/poller_v2.py
sudo mv /var/lib/hermes-ops/orch-poller-v2/runtime_support.py.new /var/lib/hermes-ops/orch-poller-v2/runtime_support.py
# si hermes-orch-poller.service modifié : copier + daemon-reload (KillMode=process requis
# pour que les launchers durables survivent au restart et écrivent leur receipt)
sudo systemctl restart hermes-orch-poller && journalctl -u hermes-orch-poller -n 3  # "hello ok"
# rollback
sudo cp -a $B/poller_v2.py $B/runtime_support.py /var/lib/hermes-ops/orch-poller-v2/ && sudo systemctl restart hermes-orch-poller
```

## Runtime Claude Desktop (claude-desktop)

Pilote l'application Claude Desktop Windows (MSIX, éditeur Anthropic — constaté
`Claude 1.52386.6.0`, profil `Caroline · Pro`) via `src/orch_runner/claude_desktop_bridge.py`,
distinct du runtime CLI `claude-code`. Cascade reprise d'`osauto` (projet local
`C:\Users\Juliann\Desktop\ui controle`) : `powershell` → `UIA`
(`.NET UIAutomationClient`, brique Module 2 / Option B) → sondes d'effet de bord
(stabilité de la conversation + `IsHungAppWindow`, Module 3) → vision exclue.
AUCUN clic à coordonnées fixes : entrée ciblée par AutomationId/Name
(ValuePattern.SetValue, repli presse-papiers + Ctrl+V), envoi par InvokePattern
du bouton Send (repli Entrée), fenêtre résolue par EnumWindows + GetWindowText.

Garanties du bridge (vérifiées avant tout pilotage, échec fermé sinon) :
- package MSIX (nom ~Claude, éditeur ~Anthropic) + processus `Claude.exe` issu
  de `WindowsApps` (la CLI `claude-code` est exclue) + fenêtre visible ;
- profil attendu (`profile`, défaut `$ORCH_CLAUDE_DESKTOP_PROFILE` sinon
  `"Caroline"`) présent dans l'arbre UIA — écart = `profile_mismatch`, le compte
  Desktop n'est JAMAIS modifié ni basculé ;
- sérialisation : verrou `%TEMP%\orch-claude-desktop.lock` (attente bornée
  `lock_timeout_s` 300 s → `ui_busy`), un seul pilote UI à la fois ;
- résultat texte : diff de conversation → `<tmpdir>/last_message.txt` + NDJSON
  `{"type": "result"}` consommé par l'adapter (résumé ≤8000 car. pour le broker).

Confinement : l'app partage profil/historique utilisateur, aucun confinement au
workspace démontrable → `read_only` UNIQUEMENT (`P.RUNTIME_MODES` refusé côté
broker `mode_denied`, `ValueError` côté adapter). Le prompt neutre E2E laisse
une trace dans l'historique Desktop (assumé, comme toute conversation).

Contrat MCP : `RuntimeT` += `claude-desktop` (`src/orch_mcp/tools.py`,
descriptions `agent_runner_list`/`agent_job_start`, `INSTRUCTIONS` de
`src/orch_mcp/server.py`) ; filtres `agent_job_list(runtime=…)` et missions
hérités. Le schéma MCP expose le nouveau Literal : **si le client MCP ChatGPT
met en cache le schéma, le reconnecter** (connector à ré-authentifier) pour voir
`claude-desktop` ; en attendant, l'E2E reste prouvable par le chemin
broker/MCP local (`python -m pytest`, `agent_job_start` direct).

Activation prudente (ne JAMAIS redémarrer le runner tant que des jobs actifs
tourneraient — tués par le restart) :
1. backup : `%USERPROFILE%\.orch-runner\runner.toml` → `runner.toml.bak-<date>` ;
2. ajouter `[runtimes.claude-desktop]` (voir `deploy/windows/runner.toml.example`,
   `enabled = true`, `exe` = python) ; valider à froid :
   `venv\Scripts\python.exe src\orch_runner\claude_desktop_bridge.py --verify-only` ;
3. si `status.json` montre des jobs actifs : STAGER seulement, puis activation
   différée sûre après leur fin (`Stop-ScheduledTask orch-runner;
   Start-ScheduledTask orch-runner`) ; sinon redémarrer la tâche ;
4. contrôle : `agent_runner_list` / `probe` annonce `claude-desktop`
   (`available=true`, `modes=["read_only"]`), puis E2E neutre
   (`agent_job_start` read_only, prompt « Reply with exactly: … »).
Rollback : `enabled = false` (ou restaurer le `.bak`) + redémarrage tâche.

Dépannage : `claude_desktop_not_installed` (package absent), `not_running`
(Desktop fermé — le lancer), `no_window` (fenêtre masquée), `profile_mismatch`
(mauvais compte : ne rien basculer, corriger `profile`), `ui_busy` (job
concurrent, attendre), `input_not_found` (UI remodelée : ré-inspecter),
`response_timeout` (réponse non stabilisée), `hung` (relancer l'app).
Limite assumée : le pilotage vole le focus de la session interactive.

Broker v2.2 (même déploiement `src/`, migration additive) : colonne
`jobs.recovery_since` (`ADD COLUMN` idempotent, NULL = pas de recovery) ;
`PROCESS_RECOVERY_S=60` — première observation négative (`proc_alive=0` ou
`supervisor_alive=false`) puis `lost` borné même si le heartbeat continue, seule
une preuve positive fraîche referme la fenêtre ; transitions `starting/running ->
lost` autorisées ; nouvel endpoint lecture fencée `/runner/v1/job-state` pour
réconciliation déterministe (aucun prompt/sortie exposé). Rollback : ancien
`src/` redéployé reste fonctionnel (colonne ignorée).

Symptôme v2.0 corrigé en 2.1 : job bloqué `running` jusqu'au timeout avec
`pid`/`runtime_session_id` null et `output_chars=0` alors que `runs/<job8>.log`
contient la réponse (enfant zombie vu vivant par `os.kill(pid, 0)`).

### v2.3 : multi-instance sûr (`max_parallel=10` démontré)

Un job = un profil Hermes dédié = un `HERMES_HOME` isolé. Pool `orch-slot-00` …
`orch-slot-09` (provision : `sudo bash deploy/hermes-poller/provision-profiles.sh` ;
`--clone` reprend config/`.env`/skills, `state.db`/sessions/mémoires séparés —
prouvé live le 2026-09-15 : `hermes -p <slot> chat` n'écrit que dans son profil).
Invocation : `docker exec -i hermes hermes -p <slot> chat … --source orch-<job>`.

- Affectation persistée en SQLite (table `slots`, migration auto) : restart/
  recovery réutilise la même identité (`slot-conserve`), jamais de double spawn
  (génération de lancement + receipt durable inchangés) ; libération seulement
  quand le job passe `done`/`abandoned`.
- Admission côté runner (`Store.gate`) : `workspace_write` identique sérialisé
  (premier réclamé premier servi, les suivants attendent en `claimed`, lease
  renouvelé par heartbeat) ; `read_only` concurrent. Pas de verrou broker :
  garanti pour le runner unique `hermes-vps`.
- `runtime_support.native_snapshot` lit le `state.db` du profil du slot
  (`HERMES_HOME` passé au `docker exec`) ; jobs pré-2.3 en recovery → `default`.
- `health.json` : `version`, `slots` ({job: slot}), `gated` (attente + motif).

Déploiement v2.3 (ne tue aucun job : launchers + receipts survivent, recovery
rattache) : backup 2 fichiers + DB, copie atomique, restart, santé `health.json`
(`version: hermes-poller/2.3`). Rollback : restaurer les 2 fichiers + restart
(colonnes/table `slots` ignorées par l'ancien code) ; profils conservés
(inertes sans la 2.3) ou supprimés un par un.

Limite assumée : sérialisation `workspace_write` locale au runner. Avec 2+
runners sur le même workspace, il faudrait un verrou côté broker (`store.claim`).

## Logs

- VPS : `journalctl -u orch-mcp -u orch-gateway` (transitions `job_transition job_id=…`, `runner_connected`,
  `reaper`), `/var/log/nginx/orch_runner_access.log`, `/var/log/nginx/mymcps_access.log`.
- PC : `%USERPROFILE%\.orch-runner\logs\runner.log` (rotation 5×5 Mo, redaction des secrets).
- Aucun prompt complet n'est journalisé ; chaque ligne de job porte `job_id`.

## Annuler un job

Depuis ChatGPT : `agent_job_cancel(job_id)`. Hors ChatGPT (VPS, loopback) :
`python scripts/mcp_call.py http://127.0.0.1:8802/mcp agent_job_cancel '{"job_id":"…"}'`.
L'arbre de processus est tué via le Job Object ; état final `cancelled`.

## Mise à jour

- VPS : recopier `src/` (et `deploy/` si changé) dans `/srv/orch`, `sudo bash /srv/orch/deploy/install.sh`
  (idempotent), `sudo systemctl restart orch-mcp orch-gateway`. Les jobs `running` survivent au
  redémarrage du broker (bail 60 s, retries runner).
- La migration DB est automatique et backward-compatible (`ALTER TABLE … ADD COLUMN` idempotent,
  nouvelles tables `IF NOT EXISTS`, NULL = non observé). Un ancien `src/` redéployé sur une DB
  migrée continue de fonctionner (colonnes ignorées) : c'est le chemin de rollback.
- PC : `deploy\windows\install-runner.ps1` (arrête le runner, recopie, relance). Un job en cours sur le PC
  pendant la mise à jour finit `lost` : mettre à jour hors activité (`agent_job_list state=running`).
  Un vieux runner (sans télémétrie) reste compatible : champs à null, pas de stall détecté.

## Rollback

```bash
sudo bash /srv/orch/deploy/rollback.sh          # stop units, retire include + listener, nginx -t, reload
sudo bash /srv/orch/deploy/rollback.sh --purge-data
```
```powershell
powershell -ExecutionPolicy Bypass -File deploy\windows\install-runner.ps1 -Uninstall
```
Sauvegardes du vhost partagé : `/root/mymcps.duckdns.org.conf.bak-orch-*`.
Les autres MCP (tasks, astra, calendar, github, vault) ne dépendent d'aucun composant orch.

## Dépannage

| Symptôme | Cause probable | Action |
|---|---|---|
| runner `offline` | session Windows fermée, NetBird coupé, jeton révoqué | `orch_runner status`, `runner.log`, `curl 10.200.114.203:8803/health` |
| `401` dans runner.log | empreinte absente/fausse côté VPS | vérifier `ORCH_RUNNER_TOKENS`, restart orch-mcp |
| `403` sur 8803 | origine hors 10.200.0.0/16 | passer par NetBird |
| tâche `Ready` sans processus, résultat `0x80070002` | runner installé dans AppData virtualisé | réinstaller dans `%USERPROFILE%\.orch-runner` |
| job `failed` `workspace_denied` | chemin changé, jonction, lecteur réseau | corriger `runner.toml`, relancer |
| job `lost` | runner tué/redémarré ou PC isolé > 60 s pendant l'exécution | vérifier l'état du workspace puis relancer volontairement |
| `runtime_unavailable` | runtime désactivé, `--version` en échec, ou opencode `runtime_not_ready` (modèle/fournisseur) | `python -m orch_runner probe` ; changer `model` puis relancer |
| `/orch/mcp` 401 dans ChatGPT | jeton OAuth expiré | reconnecter le connecteur |
