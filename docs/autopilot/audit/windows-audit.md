# Audit Windows — runner Autopilot V1 (lecture seule, 2026-10-09)

Machine DESKTOP-V30KQ19, Windows 11 Home 10.0.26200. Aucune modification système. Seuls fichiers écrits : ce rapport + `windows/*.ps1` (non exécutés, syntaxe vérifiée par le parser PowerShell). Aucun secret lu : uniquement taille, date et ACL ; les clés de `runner.toml` ont été listées avec les valeurs masquées, puis seules les valeurs non secrètes ont été affichées.

Légende : **[F]** = fait observé (commande citée) ; **[H]** = hypothèse.

## 1. Identité et comptes
- [F] `whoami /all` : `desktop-v30kq19\juliann`, SID `S-1-5-21-3782191491-1246939613-2125819924-1001`. Le groupe `BUILTIN\Administrateurs` porte l'attribut « utilisé pour les refus uniquement » : c'est un jeton filtré par UAC. Le niveau d'intégrité est **Moyen** (S-1-16-8192). Juliann fait aussi partie de `docker-users`.
- [F] `Get-LocalUser` : comptes activés = Juliann, CodexSandboxOffline (-1006), CodexSandboxOnline (-1007). Administrateur, Invité, DefaultAccount et WDAGUtilityAccount sont désactivés.
- [F] `Get-LocalGroupMember` :
  - Administrateurs = Administrateur, Juliann.
  - CodexSandboxUsers (SID -1003) = CodexSandboxOffline, CodexSandboxOnline.
  - Les groupes « CodexSandboxOffline » et « CodexSandboxOnline » n'existent pas : ce sont des **comptes**, pas des groupes.

## 2. .orch-runner et ACL
- [F] Le home du runner est `C:\Users\Juliann\.orch-runner` (`policy.py` : `DEFAULT_HOME = ORCH_RUNNER_HOME` ou `~/.orch-runner`). Le code est dans `.orch-runner\app\orch_runner` (runner.py, policy.py, winproc.py, adapters.py…). Le venv est lié au Python du profil (`pyvenv.cfg`), injecté via `orch_runner_app.pth`.
- [F] `icacls .orch-runner` : `CodexSandboxUsers:(OI)(CI)(RX)` est une ACE **EXPLICITE posée sur le dossier racine**, sans le préfixe (I). Le parent `C:\Users\Juliann` n'a pas d'ACE CodexSandbox.
- [F] `Get-Acl` fichier par fichier : runner.token (294 o, 15/09), runner.token.bak-rotate-20260915-031351 (294 o), runner.toml et ses 9 .bak, les scripts .ps1 et status.json ont tous `CodexSandboxUsers ReadAndExecute inherited=True`. Aucune autre ACE explicite ni ACL protégée dans l'arbre (profondeur 2).
- [F] Des ACE explicites CodexSandboxUsers existent aussi sur **environ 110 dossiers du profil**. Avec Modify : `.secrets`, `.claude`, `.mcp-auth`, `.oci`, `.local`, `.cache`, `.cargo`, Documents, Desktop, Downloads, OneDrive… Avec RX : `.orch-runner`, `orch-workspaces`, `.broker`, `.opencode`, `.profiles`…
- [F] `.local` porte en plus une ACE Modify pour le SID non résolu `S-1-5-21-1166338081-…-2482207823`, et `.cache` / `.codex` pour `S-1-5-21-2659286349-…-1541692272`.
- [H] Ces ACE ont été posées par l'installation du sandbox Windows de Codex (accès lecture/écriture accordé aux comptes sandbox). Les deux SID non résolus seraient des SID synthétiques du sandbox Codex.

## 3. Tâches planifiées (`Get-ScheduledTask` + `Get-ScheduledTaskInfo`)
- [F] **orch-runner** : Juliann, Interactive, RunLevel **Limited**.
  - Action : `.orch-runner\venv\Scripts\pythonw.exe -m orch_runner run` (wd = .orch-runner).
  - Déclencheurs : ouverture de session + toutes les 1 min ; MultipleInstances=IgnoreNew.
  - LastResult `0x800710E0`. [H] Code normal pour IgnoreNew quand l'instance tourne déjà.
- [F] **orch-rotate-token** : Juliann, Limited.
  - Action : `C:\Tools\silent-run.exe powershell … rotate-token-deferred.ps1`, déclencheur unique le 2026-09-15 02:37.
  - LastResult=1 (échec), état Ready.
- [F] **opencode-rotate-watch** : Juliann, Limited, ouverture de session avec répétition toutes les 2 min. Action : `powershell -ExecutionPolicy Bypass -File C:\projet\opencode-rotate\Watch-Rotate.ps1`.

## 4. Processus (`Win32_Process` + GetOwner)
- [F] Lanceur + enfant **confirmés** :
  - PID 31752 `venv\Scripts\pythonw.exe -m orch_runner run` (PPID 2212).
  - Il lance le PID 32172 `Python311\pythonw.exe -m orch_runner run` (redirecteur du venv).
  - Celui-ci lance le PID 33520 `opencode.exe run --agent build --auto -m opencode/muse-spark-1.3-contributor-free --dir …\orch-workspaces\e2e`.
  - Propriétaire : Juliann pour tous.
- [F] Trois autres `opencode.exe --auto` interactifs (Juliann) lancent des MCP via `C:\Tools\mcp-hidden-run.pyw`.

## 5. OpenCode
- [F] Binaire : `…\WinGet\Packages\SST.opencode_…\opencode.exe`, v1.18.29, 179 Mo. ACL héritées du profil : Juliann, SYSTEM et Administrateurs uniquement, donc **inaccessible aux autres comptes**.
- [F] `opencode debug paths` :
  - data = `~\.local\share\opencode` (contient auth.json 228 o et mcp-auth.json 799 o, datés du 08/10, ainsi que opencode.db de 2,2 Go) ;
  - config = `~\.config\opencode` ;
  - cache = `~\.cache\opencode` ;
  - state = `~\.local\state\opencode` ;
  - tmp = `%TEMP%\opencode`.
- [F] **auth.json et mcp-auth.json : `CodexSandboxUsers:(I)(M)` et SID étranger `(I)(M)`, hérités de l'ACE explicite sur `C:\Users\Juliann\.local`.** `.config\opencode` n'a pas d'ACE sandbox.
- [F] Isolation possible :
  - `XDG_DATA_HOME`, `XDG_CONFIG_HOME`, `XDG_CACHE_HOME` et `XDG_STATE_HOME` sont honorés : test `debug paths` avec des XDG pointant vers un dossier temporaire de l'audit, mis ensuite à la corbeille.
  - Le binaire référence aussi `OPENCODE_CONFIG`, `OPENCODE_CONFIG_DIR`, `OPENCODE_CONFIG_CONTENT` et `OPENCODE_AUTH_CONTENT`.
- [F] Mais `policy.child_env` ne transmet que `ENV_ALLOW` (USERPROFILE, APPDATA…) : **XDG_\* et OPENCODE_\* sont retirés** de l'environnement des jobs. L'isolation passe donc par un compte Windows distinct (USERPROFILE différent) ; sinon il faut modifier l'adaptateur.
- [F] `OPENCODE_ZEN_API_KEY` est défini dans l'environnement utilisateur de Juliann (valeur non lue). Il est filtré pour les jobs.

## 6. Jeton, enrôlement, coexistence
- [F] Jeton :
  - `gen-token` crée le jeton (`secrets.token_urlsafe(48)`) et le chiffre avec **DPAPI utilisateur** (`CryptProtectData`, flag 0x1, sans LOCAL_MACHINE). Il n'affiche que `runner_id:sha256`.
  - Au démarrage, le runner déchiffre le jeton (`load_token`) puis envoie `Authorization: Bearer` vers `{broker_url}/runner/v1/{hello,heartbeat,claim,event,job-state,question,transition}`.
- [F] `runner_id` = `main-windows-pc` et `broker_url` = `http://10.200.114.203:8803`, lus dans runner.toml. Autopilot cible ce runner via `AUTOPILOT_RUNNER_ID`, par défaut `main-windows-pc` (`orch_autopilot/policy.py:98`).
- [F] Broker (`agent-orchestrator-mcp/src/orch_mcp/runner_api.py`) :
  - `ORCH_RUNNER_TOKENS=runner_id:sha256[,…]` (plusieurs runners possibles) ;
  - le `runner_id` est **dérivé du jeton**, et un `runner_id` différent dans le corps de requête est refusé ;
  - filtre par CIDR overlay via X-Real-IP.
- [F] La rotation `rotate-token-deferred.ps1` enchaîne : `gen-token --force`, puis SSH vers `vps-etude` qui sauvegarde `/srv/orch/secrets/orch.env`, remplace **uniquement l'entrée de ce runner_id** et redémarre `orch-mcp`, puis relance la tâche.
- [F] Coexistence : **possible.** Aucun port entrant (connexion sortante uniquement). Le mutex `Local\orch-runner-<runner_id>` est propre à chaque runner_id et à chaque session. home, token_file, logs et status.json sont paramétrables (`--config`, `home`, `token_file`). Le broker accepte une liste de jetons.
- [H] La protection DPAPI du jeton empêche les comptes sandbox de le déchiffrer : ils lisent le blob, mais pas `Protect\` (ACL protégée : SYSTEM, Administrateurs, Juliann). Un processus tournant sous Juliann peut, lui, le déchiffrer.

## Risques classés
1. **CRITIQUE [F]** — Hérité de `C:\` (défaut Windows), « Utilisateurs authentifiés » a Modify sur `C:\Tools\silent-run.exe`, `C:\Tools\mcp-hidden-run.pyw` et `C:\projet\opencode-rotate\Watch-Rotate.ps1`. Ces trois fichiers sont exécutés sous Juliann (toutes les 2 min pour Watch-Rotate). **Tout nouveau compte standard pourrait obtenir une exécution de code sous Juliann.** Il faut corriger ces ACL avant de créer autopilot-win-01 ; le script (a) bloque tant que ce n'est pas fait.
2. **ÉLEVÉ [F]** — CodexSandboxUsers a Modify sur les fichiers d'auth OpenCode (auth.json, mcp-auth.json), `.secrets`, `.mcp-auth`, `.claude` et `.oci`. Un job Codex sandboxé peut lire ou altérer les identifiants de Juliann.
3. **MOYEN [F]+[H]** — CodexSandboxUsers a RX sur `.orch-runner` : le jeton (blob DPAPI, [H] non déchiffrable), runner.toml (chemins, broker) et les scripts de rotation sont lisibles.
4. **MOYEN [F]** — Les jobs tournent sous Juliann, admin filtré par UAC à intégrité moyenne. opencode `build --auto` hérite donc de tous les droits de Juliann : profil, SSH `vps-etude`, auth.
5. **FAIBLE [F]** — La tâche orch-rotate-token (déclencheur unique) est en échec (résultat 1) et reste enregistrée.

## Corrections proposées (scripts NON exécutés)
Tous les scripts sont dans `C:\Users\Juliann\orch-workspaces\e2e\autopilot-v1\docs\autopilot\audit\windows\`.
- `a-create-autopilot-win-01.ps1` :
  - preflight bloquant (admin + ACL de C:\Tools et C:\projet) ;
  - création du compte standard, dans Utilisateurs seulement ;
  - `C:\autopilot` avec l'héritage de C:\ coupé : runner/bin en RX, workspaces/state/tmp en M ;
  - copies dédiées d'opencode.exe et de l'app runner, venv sur un Python machine ;
  - runner.toml avec `runner_id=autopilot-win-01` et `home`/`token_file` explicites ;
  - étapes manuelles sous le compte : `gen-token` et `opencode auth login` ;
  - tâche planifiée en LogonType Password, RunLevel Limited.
- `b-fix-orch-runner-acl.ps1` :
  - `icacls /save` puis `/remove:g *S-…-1003` sur la racine `.orch-runner`, et vérification de l'arbre ;
  - rollback avec `-Restore` (`icacls C:\Users\Juliann /restore`) ;
  - option `-IncludeOpencodeAuth` pour couper l'héritage sur auth.json et mcp-auth.json.
- `c-negative-tests-autopilot-win-01.ps1` : 26 tests à lancer sous le compte dédié. Résultat attendu DENIED pour :
  - le profil Juliann, runner.token, runner.toml, auth.json, .secrets, .ssh, Protect, HKU ;
  - l'écriture dans orch-workspaces, C:\Tools, C:\projet, C:\Windows, C:\autopilot\runner et C:\autopilot\bin ;
  - schtasks /change sur les tâches de Juliann.

  Contrôles positifs (ALLOWED) : workspace, state, `opencode --version`. Code de sortie = nombre d'échecs.

Points à vérifier :
- [H] le droit « Ouvrir une session en tant que tâche » est attribué automatiquement au compte par Register-ScheduledTask avec mot de passe ;
- [H] le sandbox Codex pourrait reposer ses ACE au prochain run ;
- les dépendances pip exactes du runner (le script installe seulement httpx).
