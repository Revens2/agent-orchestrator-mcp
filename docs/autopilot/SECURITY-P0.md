# Autopilot V1 — rapport sécurité P0 avant / après (2026-10-09)

Sources : audits `audit/windows-audit.md`, `audit/vps-audit.md`, `audit/arch-audit.md` (lecture seule, 08–09/10),
revue Opus session `BzGGPrx51TyIUUefSvfPTj4s` (consultative : modèle servi non attesté).

| # | Constat (avant) | Preuve | Après (code PR) | Reste à faire (hors code) |
|---|---|---|---|---|
| 1 | DONE = marqueurs/JSON écrits par l'agent ; 6 falsifications passent | arch-audit P0-1 | DONE `local_write` exige enveloppe Ed25519 du runner (diff manifeste, attestation OS, lien job_id/runner/mode/exit/âge) ; read exige diff vide si clé runner | runner isolé installé (J3) |
| 2 | local_write activable par `AUTOPILOT_RUNNER_DEDICATED_ACCOUNT=1` | policy.py@2177473 | variable refusée au démarrage ; clé publique + SID agent requis ; `main-windows-pc` jamais en écriture | — |
| 3 | Runner = compte perso admin, OpenCode même identité | windows-audit §1/§4 | runner A + agent B standards ; OpenCode via CreateProcessWithLogonW, Job Object, sondes OS sous B par job | exécution installateur (autorisation) |
| 4 | `C:\Tools\*`, `Watch-Rotate.ps1` modifiables par Utilisateurs authentifiés, exécutés sous Juliann | windows-audit risque 1 | `fix-tools-acl.ps1` (sauvegarde + rollback) ; installateur refuse tant que non corrigé | exécution (autorisation) |
| 5 | CodexSandboxUsers lit `.orch-runner` (token DPAPI) ; Modify sur auth OpenCode | windows-audit §2/§5 | `audit/windows/b-fix-orch-runner-acl.ps1` (sauvegarde/restauration) | exécution (autorisation) ; rotation token conseillée |
| 6 | Gate : NO-GO d'un modèle attesté => DONE | arch-audit P0-2 | verdict 1re ligne ; NO-GO/ambigu => PENDING_GATE | — |
| 7 | « 429 » écrit par l'agent : cooldown au lieu d'audit, breaker global | arch-audit P1 | 429 lu seulement dans l'erreur runner d'un job failed `read` | — |
| 8 | Cockpit 8812 sans auth sur loopback (autres comptes VPS) | vps-audit risques | Bearer obligatoire (refus de démarrer sans) ; gateway relaie un jeton dédié | déploiement staging |
| 9 | Code d'approbation : sha256 non salé (~39 bits) | vps-audit | HMAC à clé serveur `consent.key` 0600 | déploiement |
| 10 | Disque 92 %, aucune garde | vps-audit §4 | refus missions/jobs sous 2 Go ou > 95 %, alerte, `/health` | nettoyage (autorisation) |
| 11 | Watchdog 120 s < appel Opus 900 s | arch-audit P1 | thread watchdog séparé (tick bloqué > 1000 s => restart) | — |
| 12 | Balise `<donnees_non_fiables>` fermable par l'agent | arch-audit P0-1 | neutralisée | — |
| 13 | Opus : identité servie non attestée | opus_health `served_model_last=null` | inchangé : PENDING_GATE (fail-closed) | attestation fournisseur |
| 14 | `claude-opus-mcp.service` 644 avec jeton en clair | vps-audit | signalé | EnvironmentFile 0600 (autorisation) |

## Risques résiduels acceptés V1 (documentés)
- L'agent B peut exfiltrer via le réseau et utiliser l'auth OpenCode de B (pas de filtrage egress Windows).
- Persistance dans le profil de B (HKCU Run, tâches utilisateur) non couverte par le manifeste.
- `runner_observed` = observé et signé par A ; ne prouve pas la qualité du contenu produit.
- Un attaquant contrôlant A (code runner) peut signer : A est standard, code en lecture seule pour A.
