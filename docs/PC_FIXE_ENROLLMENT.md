# Enrôlement du second runner `pc-fixe` (PC fixe i5-14600KF / RTX 4070)

Bootstrap reproductible, à exécuter **quand pc-fixe sera joignable** sur NetBird/SSH.
État constaté le 2026-09-16 : `pc-fixe.netbird.selfhosted` ne résout pas, aucun peer
NetBird `pc-fixe` (pairs vus : `pc-travail-juliann`, `vps-ia`, `vps-nexus`, `nas-juliann`,
`vps-etude`). `pc-travail` (10.200.12.43, user `julia`) est une autre machine, pas la
cible. Le portable (`pc-portable-juliann`, runner `main-windows-pc`) tourne avec des
jobs actifs : **ne jamais le toucher** pendant cet enrôlement.

## Principes

- Le broker supporte déjà plusieurs jetons (`ORCH_RUNNER_TOKENS=runner_id:sha256[,…]`,
  cf. `src/orch_mcp/runner_api.py::parse_tokens`) : ajouter pc-fixe est additif, le
  portable garde son entrée et son jeton inchangés.
- Le jeton pc-fixe est généré **sur pc-fixe** (`python -m orch_runner gen-token`,
  chiffré DPAPI) : il ne quitte jamais la machine, **aucun secret dans Git**.
  Seule l'empreinte `pc-fixe:<sha256hex>` est copiée vers le VPS.
- `deploy/windows/pc-fixe/enroll-pc-fixe.ps1` refuse de s'exécuter contre une config
  d'un autre runner (ex. `main-windows-pc`) et contre des jobs actifs, sauf drapeaux
  explicites : une exécution par erreur sur le portable aborte au lieu de casser le live.

## Fichiers (cette branche)

| Fichier | Rôle |
|---|---|
| `deploy/windows/pc-fixe/runner.toml.pc-fixe.example` | template pc-fixe (`runner_id = "pc-fixe"`, `max_parallel = 4` pour le i5-14600KF, placeholders `<user>`, aucun secret) |
| `deploy/windows/pc-fixe/enroll-pc-fixe.ps1` | à lancer **sur pc-fixe** : rend la config, crée les dossiers, génère le jeton, affiche l'empreinte + la commande VPS ; `-Install` enchaîne `install-runner.ps1` |
| `deploy/add-runner-token.sh` | à lancer **sur vps-etude** : ajoute/remplace UNE empreinte dans `ORCH_RUNNER_TOKENS` (idempotent, backup, autres entrées préservées), redémarre `orch-mcp`, vérifie `/health` |
| `tests/test_pcfixe_bootstrap.py` | preuves locales en home temporaire (aucun accès réseau/machine réelle) |

## Procédure (quand pc-fixe est joignable)

1. **Vérifier la cible** (depuis le portable, lecture seule) :
   ```powershell
   netbird status --detail            # attendre un peer pc-fixe
   Resolve-DnsName pc-fixe.netbird.selfhosted   # doit résoudre en 10.200.x
   ssh <user>@pc-fixe.netbird.selfhosted 'hostname'  # doit répondre le fixe, PAS pc-travail
   ```
   Garde-fou : si `hostname` renvoie autre chose que le fixe, STOP (mauvaise machine).
2. **Sur pc-fixe** (clone du dépôt à jour sur cette branche) :
   ```powershell
   powershell -NoProfile -ExecutionPolicy Bypass -File deploy\windows\pc-fixe\enroll-pc-fixe.ps1
   # affiche : EMPRUNTE `pc-fixe:<sha256>` + commande VPS exacte
   ```
3. **Sur vps-etude** :
   ```bash
   sudo bash deploy/add-runner-token.sh 'pc-fixe:<sha256hex>'
   # idempotent : 'unchanged' si déjà déclaré ; backup $ENV_FILE.bak-<ts> ; restart orch-mcp + /health ok
   ```
4. **Sur pc-fixe**, démarrer le runner :
   ```powershell
   powershell -NoProfile -ExecutionPolicy Bypass -File deploy\windows\pc-fixe\enroll-pc-fixe.ps1 -Install
   %USERPROFILE%\.orch-runner\venv\Scripts\python.exe -m orch_runner status
   ```
5. **Vérifier côté broker** (depuis ChatGPT ou loopback, lecture seule) :
   `agent_runner_list` → `pc-fixe` online ; `agent_runner_inspect(runner_id="pc-fixe")`,
   puis un job `fake`/`read_only` neutre avant toute charge réelle.

## Rotation / révocation

- Rotation : sur pc-fixe `enroll-pc-fixe.ps1 -Rotate [-Install]`, puis VPS
  `add-runner-token.sh 'pc-fixe:<nouvelle_empreinte>'` (remplace l'entrée pc-fixe,
  `main-windows-pc` préservé).
- Révocation immédiate : retirer l'entrée `pc-fixe:…` de `ORCH_RUNNER_TOKENS` puis
  `sudo systemctl restart orch-mcp` (le runner pc-fixe reçoit 401, le portable reste connecté).
- Rollback config pc-fixe : restaurer `%USERPROFILE%\.orch-runner\runner.toml` depuis
  le backup, ou `install-runner.ps1 -Uninstall` (config/jeton/logs conservés).

## Ce qu'on ne fait jamais

- Aucun `restart`/`gen-token --force` sur le portable tant que ses jobs tournent
  (un job en cours finirait `lost`).
- Aucune empreinte réelle, aucun jeton, aucun chemin utilisateur réel dans Git
  (placeholders `<user>` / `<sha256hex>` uniquement — vérifié par test).
