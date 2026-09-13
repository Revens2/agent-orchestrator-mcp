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
- PC : `deploy\windows\install-runner.ps1` (arrête le runner, recopie, relance). Un job en cours sur le PC
  pendant la mise à jour finit `lost` : mettre à jour hors activité (`agent_job_list state=running`).

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
