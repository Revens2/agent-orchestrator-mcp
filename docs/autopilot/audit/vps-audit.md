# Audit VPS (lecture seule) : broker orch + staging Autopilot

- Date de l'observation : 2026-10-08 entre 22:17 et 22:30 UTC (horloge VPS)
- Cible : alias SSH `vps-etude` (hostname `oui`, aarch64), utilisateur `juliann`, `sudo -n` utilisé uniquement pour lire
- Référence locale : `C:\Users\Juliann\orch-workspaces\e2e\autopilot-v1`, branche `feat/autopilot-v1`, HEAD `21774736`, arbre de travail propre ; `origin/main` = `883133dd`
- Méthode : lecture seule stricte. Aucun service n'a été modifié, redémarré ou rechargé, aucun fichier n'a été supprimé et aucun nettoyage n'a été fait. Les secrets ne sont pas affichés : seuls les noms de clés et leur présence le sont.
- Seuls effets secondaires : lectures SQLite concurrentes via `sqlite3 -readonly` sous l'utilisateur `orch-app`, et requêtes HTTP sans effet (`GET /health`, `POST /mcp` avec un corps JSON invalide qui renvoie une erreur de parsing, donc sans appel d'outil). Trois fichiers temporaires `/tmp/.r*` sont restés sur le VPS, en `/tmp` de l'utilisateur juliann.
- Légende : **[OBS]** = fait observé (commande et extrait) ; **[HYP]** = hypothèse ou inférence non prouvée.

---

## 1. Staging Autopilot `/srv/autopilot`

### Version déployée
- [OBS] Le dossier ne contient pas de `.git`. `cat /srv/autopilot/VERSION` donne `b31093e`.
- [OBS] Le SHA-256 des 17 fichiers déployés (`src/orch_autopilot/*.py`, `src/orch_protocol/*.py`, `deploy/autopilot/*`) est **identique** à `git show 2177473:<fichier> | sha256sum` pour les 17 fichiers. Exemples :
  - `cockpit.py` : `4fd2b47b…cb5b9` des deux côtés
  - `policy.py` : `5dba2bd4…038a` des deux côtés
  - `supervisor.py` : `0ba09421…7b862` des deux côtés
- [OBS] `git diff --stat b31093e 2177473` ne touche que `docs/autopilot/RUNBOOK.md` et `docs/autopilot/progress.md`. Le code déployé est donc fonctionnellement celui de `2177473` ; le fichier VERSION dit `b31093e`, ce qui est cohérent.
- [OBS] Un backup du déploiement précédent existe dans `/srv/autopilot/backup-0223cac/` (VERSION, autopilot.db, autopilot.env, src, deploy ; dossier en 0700 root).

### systemd
- [OBS] `autopilot.service` : `active (running)`, `enabled`, MainPID 2963445, démarré le 2026-10-08 à 20:55:15 UTC, `NRestarts=0`, aucun drop-in.
- [OBS] `autopilot-gateway.service` : **non installé** (`No files found`, `UnitFileState` vide). Le fichier unit existe seulement dans `/srv/autopilot/deploy/autopilot/`.

### Port
- [OBS] `ss -tlnp` : `LISTEN 127.0.0.1:8812 users:(("python",pid=2963445))`. Aucun bind externe.

### Kill switch
- [OBS] Il n'y a pas de variable d'environnement : le kill switch est stocké en base, dans `meta.kill_switch`. Dans le code : `supervisor.py:145-155` ; il se désactive uniquement par la CLI (`python -m orch_autopilot.service kill-switch on|off`), car le cockpit refuse (`cockpit.py:85`).
- [OBS] `curl 127.0.0.1:8812/health` renvoie `{"status":"ok","tick_age_s":3.59,"kill_switch":true}`.
- [OBS] `sqlite3 -readonly … "select key,value from meta"` donne `schema_version|2` et `kill_switch|1`. **Le kill switch est bien ON.**

### Base de données et état
- [OBS] La base est `/srv/autopilot/data/autopilot.db` (orch-app, 0600, mode `wal`). Fichiers : `-wal` de 585 Ko, `-shm`. Le dossier `data/` est en 0700 orch-app.
- [OBS] Contenu de la base :
  - objectives : `ACTIVE=1`
  - steps : `DONE=2`, `PENDING_GATE=1`
  - outbox : `acked=3`
  - consents : 0
  - dossier `data/approvals/` : vide
- [OBS] L'environnement (`/srv/autopilot/secrets/autopilot.env`, root:orch-app 0640) contient seulement des clés non secrètes :
  - `AUTOPILOT_PORT=8812`
  - `AUTOPILOT_DATA_DIR=/srv/autopilot/data`
  - `AUTOPILOT_BROKER_URL=http://127.0.0.1:8802/mcp`
  - `AUTOPILOT_RUNNER_ID=main-windows-pc`
  - `AUTOPILOT_WORKSPACES=e2e`
  - `AUTOPILOT_DAILY_MAX_JOBS=50`
  - `AUTOPILOT_TICK_S=5`
  - `AUTOPILOT_NOTIFY_DIR=/srv/autopilot/data/approvals`
  - `AUTOPILOT_ALLOWED_KINDS=read,decision`
- [OBS] `AUTOPILOT_OPUS_URL` est absente. `service.py:52` applique donc `UnavailableGate` : **le gate Opus n'est pas câblé**, et l'étape `PENDING_GATE` ne peut pas avancer sans approbation humaine.
- [OBS] `AUTOPILOT_RUNNER_DEDICATED_ACCOUNT` est absente et `local_write` ne figure pas dans `ALLOWED_KINDS` : l'écriture locale est désactivée.
- [OBS] Le runtime Python est partagé : `ExecStart=/srv/orch/venv/bin/python` (venv du broker, `mcp` 2.2.0).

---

## 2. Broker `/srv/orch` : écart avec `origin/main` (883133d)

- [OBS] Pas de `.git` dans `/srv/orch`. J'ai comparé le SHA-256 de `src/`, `deploy/` et `requirements.lock` déployés avec `git show 883133d:<fichier>`.
- [OBS] Bilan : **32 fichiers identiques, 10 différents, 7 présents seulement sur le VPS.** Les 10 fichiers différents :

| Fichier | Lignes propres à main / au déployé | Correspondance dans l'historique Git (sha256) |
|---|---|---|
| src/orch_mcp/runner_api.py | 63 / 4 | = `986394b` (15/09) : il manque `ORCH_RUNNER_SOURCE_BINDINGS` (remap de recovery) |
| src/orch_mcp/store.py | 1 / 15 | aucune : hotfix local (« runtime_unavailable sans substitution de runtime ») |
| src/orch_mcp/tools.py | 1 / 8 | aucune : hotfix local (même consigne « SANS SUBSTITUTION DE RUNTIME ») |
| src/orch_runner/adapters.py | 10 / 18 | aucune : probe lazy (`probe_generation=False`), sans le fix 883133d (verdict avant rmtree), plus des options conversation/new_chat |
| src/orch_runner/runner.py | 29 / 1 | = `50c7e88` (14/09) |
| src/orch_runner/claude_desktop_bridge.py | 2 / 239 | aucune : +239 lignes non versionnées |
| src/orch_runner/desktop_patch.py | 5 / 6 | = `ce9fc65` / `7b5329b` (16/09) |
| deploy/orch.env.example | — | = `bb5834d` (branche feat/pc-fixe-bootstrap, **absente de main**) |
| deploy/windows/install-runner.ps1 | — | aucune |
| deploy/windows/runner.toml.example | — | aucune |

- [OBS] Fichiers présents seulement sur le VPS :
  - `deploy/add-runner-token.sh`
  - `deploy/windows/pc-fixe/{enroll-pc-fixe.ps1,runner.toml.pc-fixe.example}`
  - `src/orch_gateway/oauth.py.bak-norotate-20260923-153945`
  - `src/orch_mcp/tools.py.bak-20260914T083126Z`
  - `src/orch_runner/adapters.py.bak-lazy`
  - `src/orch_runner/policy.py.bak.20260914212535`
- [OBS] `src/orch_gateway/*` est identique à 883133d. Les modifications de la branche feature (`orch_gateway/app.py`, `politique.py`, soit la politique de la gateway autopilot) **ne sont pas déployées**.
- [HYP] Le déployé est un mélange : commits de mi-septembre, plus des hotfix locaux non versionnés (store/tools/adapters/claude_desktop_bridge), sans les commits main récents. Un redéploiement naïf de main **ferait perdre** les hotfix « sans substitution de runtime » et les +239 lignes du bridge Desktop, et ajouterait le probe de génération par défaut (`probe_generation=True`, qui consomme des tokens).
- [OBS] Services du broker : `orch-mcp` (127.0.0.1:8802), `orch-gateway` (127.0.0.1:8801) et `orch-gateway-rs` (127.0.0.1:18981), tous actifs depuis le 2026-10-07 à 02:01:47 UTC.

---

## 3. systemd, réseau et joignabilité de l'Opus MCP

### `autopilot.service` (extraits, sans secret)
```
User=orch-app  Type=notify  WatchdogSec=120  Restart=always
EnvironmentFile=/srv/autopilot/secrets/autopilot.env
After=network-online.target orch-mcp.service   (pas de Requires/BindsTo)
NoNewPrivileges, PrivateTmp, PrivateDevices, ProtectSystem=strict, ProtectHome, ProtectProc=invisible,
CapabilityBoundingSet= (vide), SystemCallFilter=@system-service, UMask=0077
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
IPAddressAllow=localhost
IPAddressDeny=any
ReadWritePaths=/srv/autopilot/data
```

### `autopilot-gateway.service`
- [OBS] Non installé.

### Emplacement de l'Opus MCP
- [OBS] Il tourne **sur le même VPS**, en local :
  - `opus-mcp.service` : utilisateur astra-app, `/srv/opus/venv/bin/python -m opus_mcp.mcp_server`, écoute sur **127.0.0.1:8794**, sans IPAddressDeny (il a besoin d'un egress Internet vers le portail).
  - `claude-opus-mcp.service` : binaire Rust `/opt/claude-opus-mcp`, écoute sur **127.0.0.1:8796**, `OPUS_GW_RS_UPSTREAM=http://127.0.0.1:8794`. C'est une gateway avec bearer/OAuth : `POST /mcp` sans jeton renvoie **401**, `GET /health` renvoie 200.
  - `claude-opus-oauth.service` : `opus_gateway.server`, sur 127.0.0.1:8795.
  - Exposition publique par nginx (`mymcps.duckdns.org.conf`) : `/claude-opus/mcp` vers 8796.

### Pare-feu du loopback
- [OBS] La table nft `inet localport_guard`, persistée dans `/etc/nftables.d/localport-guard.nft`, filtre le loopback par UID :
  - `tcp dport 8802 meta skuid { 0, 33, 982 } accept` : root, www-data et orch-app
  - `tcp dport 8794 meta skuid { 0, 985 } accept` : root et astra-app
  - les ports 8794 et 8802 font partie de la liste rejetée pour les autres UID
  - test fait en tant que juliann : `curl 127.0.0.1:8802` et `curl 127.0.0.1:8794` échouent tous deux avec `curl: (7) Couldn't connect`
  - 8796 et 8812 **ne sont pas** dans la garde

### Conclusion
- [OBS/inférence] `autopilot.service` (orch-app, uid 982) :
  - **ne peut pas** joindre 8794 : refusé par nft ;
  - **peut** joindre 8796 : systemd autorise localhost et nft ne filtre pas ce port.
  - La note du Vault (« Opus distant nécessite une ouverture d'egress ») est donc **inexacte** : l'Opus MCP est local.
- Connectivité minimale nécessaire :
  - `127.0.0.1:8796` sur TCP uniquement ;
  - un jeton bearer dédié au périmètre minimal (le gateway-rs gère des jetons avec scope, par exemple `claude-opus:ecriture`), stocké dans un fichier 0640 root:orch-app sous `/srv/autopilot/secrets/` ;
  - `AUTOPILOT_OPUS_URL=http://127.0.0.1:8796/mcp` et `AUTOPILOT_OPUS_TOKEN_FILE=…`.
  - **Aucune modification d'IPAddressAllow ni de nft n'est nécessaire.**
  - Ne pas ajouter orch-app à la garde de 8794 : ce serait un accès direct sans authentification.
- [HYP] À vérifier avant de câbler le jeton :
  - est-ce que la sortie `opus_health` (`served_model_last`) est exposée par 8796 ?
  - le gate exige `AUTOPILOT_EXPECTED_SERVED_MODEL` (Vault : `served_last null`).

### Constat sécurité hors périmètre Autopilot
- [OBS] `/etc/systemd/system/claude-opus-mcp.service` est en mode 644 et contient `Environment=OPUS_GW_RS_TOKEN=<valeur en clair>` (valeur non affichée).
- [HYP] Ce jeton est lisible par tout utilisateur local (`systemctl show` / `cat`). Il faudrait le déplacer dans un `EnvironmentFile` en 0600.

---

## 4. Disque (aucune suppression)

- [OBS] `df -h /` : `/dev/sda1 193G, 178G utilisés, 16G libres, 92 %`. Inodes : 8 %.
- [OBS] Mémoire : 23 Gi au total, 12 Gi disponibles. Load average : 2.8 à 3.3. Uptime : 6 jours.
- [OBS] `du -xh --max-depth=1 /` :
  - /var : 94G
  - /home : 30G
  - /srv : 20G
  - /tmp : 16G
  - /opt : 7.2G
  - /usr : 6.6G
  - /root : 1.1G
- [OBS] Plus gros consommateurs :
  - /var/lib/rag-v2 : 28G
  - /var/lib/containerd : 25G
  - /var/backups : 14G (dont vault 8.3G, 38 archives `coffre-*.tar.gz.gpg` depuis le 08/09, et rag-rebuild 4.9G)
  - /var/lib/docker : 11G
  - /var/lib/rag-v2-opt : 7.6G
  - /home/juliann : 14G (dont build 6.4G)
  - /home/ghrunner : 7.8G
  - /home/ludo/.ollama : 3.6G
  - /srv/vault-mirror-trash : 13G (écrit le 08/10 à 21:57, donc encore actif)
  - /tmp/geniso/generations : 15G (ragv2, 05/10)
  - /tmp/e6-rerank-venv : 1.1G et /tmp/e6-models : 558M (ragv2, 04-05/10)
  - /opt/vault-mcp-backup-20260925T153122Z : 2.2G
  - /var/log : 2.5G (journal 1.7G, syslog 0.3G, nginx 165M)
- [OBS] `journalctl --disk-usage` : 1.6G.
- [OBS] `docker system df` :
  - Images : 23.6GB, dont 3.73GB récupérables
  - Volumes locaux : 10.59GB, dont 6.12GB récupérables (124 volumes inactifs sur 148)
  - Build cache : 2.38GB
- [OBS] Unités en échec liées au disque :
  - `logrotate.service` et `logrotate-check.service` : `error creating output file /var/log/warp_watchdog.log-20260926.gz: Permission denied`. La rotation est bloquée depuis le 26/09 et `/var/log/warp_watchdog.log` grossit (15 Mo).
  - `tmp-cleanup.service` (`find /tmp /var/tmp -mtime +14 -delete`) échoue sur `/var/tmp/trash-20260927/…: Directory not empty`.

### Ce qui pourrait être libéré (propositions, à faire avec accord et backup)

| Cible | Gain estimé | Risque |
|---|---|---|
| /tmp/geniso/generations | ~15G | Faible à moyen : artefacts de génération RAG v2 du 05/10. Vérifier avec le propriétaire ragv2 qu'aucun job ne les consomme. |
| /srv/vault-mirror-trash | jusqu'à 13G | Moyen : corbeille de la synchro du Vault, encore alimentée. Il faut une politique de rétention (par exemple plus de 14 jours) et non une purge totale. |
| Volumes Docker orphelins (`docker volume prune` après inventaire) | ~6G | Moyen : un volume « inactif » peut appartenir à une stack arrêtée volontairement. Lister avant. |
| Images Docker inutilisées | ~3.7G | Faible |
| Build cache | ~2.3G | Faible |
| /opt/vault-mcp-backup-20260925T153122Z | 2.2G | Faible si la version courante de vault-mcp est validée depuis le 25/09 |
| /var/backups/vault (38 archives gpg) | quelques G | Moyen : appliquer une rétention (par exemple 14 quotidiennes) **après** vérification d'une copie hors site |
| /var/backups/rag-rebuild | 4.9G | Moyen |
| `journalctl --vacuum-size=500M` | ~1.1G | Faible |
| Réparer logrotate (permissions ou directive `su` pour warp_watchdog) | préventif | Faible |

---

## 5. Services

- [OBS] Services critiques actifs :
  - orch-mcp, orch-gateway, orch-gateway-rs
  - autopilot
  - opus-mcp, claude-opus-mcp, claude-opus-oauth
  - nginx, netbird, unbound, dnsproxy, fail2ban, docker, containerd, earlyoom
  - hermes-orch-poller
  - opencode-serve, opencode-bridge
  - browser-gateway(-rs), chatgpt-web-browser
  - vault-mcp(-rs), vault-context-mcp(-v2), vault-ngrok
  - tasks-*, calendar-*, github-*, excalidraw-*, activity-*, sandbox-gateway-rs
  - rag-v2-shadow, rag-v2-shadow-pu5, rag-v2-qwen-backfill
  - pr-reviewer-*
  - 3 runners GitHub Actions `Revens2-Watchy` (utilisateur ghrunner)
  - convia-decide, codex-asr, media-ingest-*
- [OBS] Unités en échec (`systemctl --failed`) :
  - `logrotate.service`, `logrotate-check.service` (voir section 4)
  - `snap.certbot.renew.service` : échec le 2026-10-08 à 10:21 UTC. Le log montre les certificats « skipped » (pas encore à renouveler), mais l'unité sort en exit 1. Certificat le plus proche de l'expiration : `netbird-etude.duckdns.org` le **2026-11-20**, puis tasks-mcp et vaultwarden-vps le 2026-12-04, et mymcps le 2026-12-06.
  - `tmp-cleanup.service` (voir section 4)
  - `notify-failure@vault-edge-probe.service.service`
  - `run-u18530.service` (unité transitoire)

---

## 6. Gateway OAuth et cockpit

### Exposition
- [OBS] `grep -rn "8812|autopilot" /etc/nginx/` ne trouve rien. Le cockpit **n'est pas exposé** par nginx : il est lié à 127.0.0.1:8812 (`service.py:123` force `host="127.0.0.1"`). `autopilot-gateway.service` est absent.
- [OBS] Exposition publique actuelle du broker (`/etc/nginx/snippets/orch-public.conf`, inclus dans mymcps.duckdns.org) :
  - `/orch/mcp` vers 127.0.0.1:18981 (orch-gateway-rs)
  - les routes OAuth `/oauth/orch/*` vers 127.0.0.1:8801
  - le runner Windows passe par `orch-runner.conf` : 10.200.114.203:8803 (NetBird), `allow 10.200.0.0/16; deny all`, POST limité à `/runner/v1/(hello|heartbeat|job-state|claim|event|transition)`, puis vers 8802
- [OBS] La politique MCP de la gateway prévue pour autopilot (`orch_gateway/politique.py` dans la branche) **n'est pas déployée** : `/srv/orch/src/orch_gateway` est identique à main.

### Authentification du cockpit
- [OBS] Le cockpit répond **sans authentification** sur le loopback : `POST 127.0.0.1:8812/mcp` (corps invalide, en tant que juliann) renvoie `400 {"jsonrpc":"2.0",…"Parse error"}`, sans 401.
- [OBS] Aucun middleware d'authentification dans `service.py` ni `cockpit.py`. Le port 8812 n'est pas dans la garde nft.
- [OBS] Les runners GitHub Actions (`ghrunner`) n'ont ni `IPAddressDeny` ni `PrivateNetwork`.
- [HYP] Tout processus local, y compris un workflow CI Watchy, pourrait appeler `autopilot_submit`, `pause`, `cancel` ou `approve` en phase 1. Ce qui borne le risque :
  - la phase 2 d'approbation exige le code hors bande ;
  - le kill switch OFF n'est accessible que par la CLI ;
  - ON reste possible : déni de service par pause globale.

### Les 8 outils du cockpit (`cockpit.py`)
| Outil | Rôle |
|---|---|
| `autopilot_submit` | soumettre un objectif |
| `autopilot_status` | état d'un objectif |
| `autopilot_list` | lister les objectifs |
| `autopilot_pause` | pause ; avec `'*'`, met le kill switch à ON |
| `autopilot_resume` | reprendre ; refuse le kill switch OFF |
| `autopilot_cancel` | annuler un objectif |
| `autopilot_approve` | phase 1 `step_id+resolution`, phase 2 `action_hash+code` |
| `autopilot_handoff` | reprise sans transcript |

Les outils en écriture (`TOOLS_WRITE`) sont submit, pause, resume, cancel et approve.

### Code d'approbation
- [OBS] Le cockpit ne renvoie jamais le code d'approbation (code lu dans le déployé, dont le sha est identique à 2177473) :
  - `request_approval` (`supervisor.py:160-184`) génère le code (8 caractères, `secrets.choice`). Il stocke seulement `sha256(action_hash:code)` dans `consents.code_hash`, envoie le code par `notifier.send` (FileNotifier, fichier 0600 dans `data/approvals/`), et renvoie au cockpit uniquement `{action_hash, summary, expires_at, next}` ;
  - l'événement `approval_requested` ne journalise que `action_hash` et `resolution` ;
  - `objective()`, `list_objectives()` et `handoff()` ne lisent pas la table `consents`.
- [HYP] Faiblesse résiduelle : `code_hash` est un sha256 non salé d'un code d'environ 39 bits (8 caractères sur un alphabet de 31). En cas de fuite de `autopilot.db`, une attaque par force brute hors ligne est réaliste. Il serait préférable d'utiliser un HMAC avec un secret serveur. Par ailleurs, `CommandNotifier` (Telegram) ne pourrait pas sortir vers Internet sous `IPAddressDeny=any`.

---

## 7. Risques d'interruption en cas de redéploiement de `/srv/autopilot`

- [OBS] Couplages :
  - autopilot utilise le **venv de `/srv/orch`** : toute installation pip faite pour autopilot dans ce venv modifie le runtime d'orch-mcp et d'orch-gateway ;
  - `After=orch-mcp.service`, sans `Requires` : un redémarrage d'autopilot ne touche pas le broker ;
  - autopilot écrit uniquement dans `/srv/autopilot/data` (orch.db n'est jamais montée en écriture).
- [OBS] `install.sh` fait `install unit`, `daemon-reload`, `enable --now` et `curl /health`. Le `daemon-reload` est global mais sans effet sur les unités inchangées.
- [HYP] `enable --now` **ne redémarre pas** un service déjà actif : après une mise à jour du code, il faut un `systemctl restart autopilot` explicite, sinon l'ancien code reste chargé et `/health` passe quand même.
- [OBS] État à préserver :
  - `autopilot.db` (WAL : copier db, wal et shm ensemble, ou faire `sqlite3 .backup`) ;
  - `meta.kill_switch=1` ;
  - 1 étape `PENDING_GATE`.
  Le backup de 0223cac existe.
- [HYP] Risques pour les services existants si le redéploiement se limite à `/srv/autopilot` et `autopilot.service` : **faibles** (port 8812 dédié, utilisateur orch-app partagé mais dossier séparé).
- Risques réels :
  1. modification du venv partagé ;
  2. installation d'`autopilot-gateway.service` ou d'une route nginx, qui touche nginx (rechargement global de mymcps, avec toutes les routes MCP publiques) ;
  3. tout redéploiement de `/srv/orch` depuis main ou la branche : perte des hotfix non versionnés (section 2) et changement du comportement du probe runner ;
  4. disque à 92 % : une copie de backup ou un pip install peut aggraver la situation.

---

## Écarts entre le Vault et l'état réel
- Le Vault (note P0) dit « Opus distant nécessite un design réseau ». En réalité, l'Opus MCP est local (8796 avec authentification, 8794 sans) et 8796 est joignable depuis autopilot.service sans changement réseau.
- Le Vault (note P0) dit que le constat « broker live divergent » et « disque ~92 % » n'a pas été revérifié. Les deux sont maintenant **confirmés** (10 fichiers différents et 7 en plus ; 92 %).
- RUNBOOK/progress dit « staging 2177473 ». Le fichier VERSION indique `b31093e`, mais le code est identique (seule la doc diffère).

## Mises à jour proposées pour le Vault (non effectuées, Vault en lecture seule)
- Note ChatGPT-Autopilot-Security-P0 :
  - Opus MCP local sur 127.0.0.1:8796 (gateway-rs avec bearer) et 8794 (garde nft sur astra-app) ;
  - divergence du broker confirmée avec la liste des fichiers ;
  - cockpit 8812 sans authentification sur le loopback ;
  - gate Opus non configuré.
