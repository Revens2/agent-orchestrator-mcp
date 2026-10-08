# Autopilot V1 — plan (CRITICAL)

Source mission : `ChatGPT_Autopilot_Claude_OneShot_v2.md` (2026-10-08). Branche `feat/autopilot-v1`, base main `883133d`.
Runtime produit : **OpenCode exclusivement**. Aucun fallback Claude/Codex/AGY/Hermes.

## Architecture retenue (revue Opus session `45lmRkmrXFuA5CFQr1dx0guD`, 2026-10-08)

1. `src/orch_autopilot/` — service séparé `autopilot.service` (systemd, `orch-app`, `ReadWritePaths=/srv/orch/autopilot`).
2. `autopilot.db` SQLite WAL séparée : objectives, steps, events (append-only), outbox (payload figé), effects, gate_decisions,
   consents (action_hash + code hors bande, usage unique, expiration), budgets, kill switch, handoff (etag).
3. Superviseur = tick déterministe. Broker joint via MCP client `127.0.0.1:8802/mcp` (outils existants seulement).
   Jamais d'écriture dans `orch.db`. Allowlist produit locale `{"opencode"}` + outils broker autorisés.
4. Cockpit = MCP **séparé** servi par le superviseur (localhost, port configurable — vérifier libre sur VPS ; 8803 est
   déjà le port runner nginx), exposé via une 2e instance `orch-gateway` avec politique `autopilot` (env `ORCH_GW_POLICY`).
5. Gate Opus : entrée outbox `kind=gate`. Modèle servi non attesté => `PENDING_GATE`. Opus consultatif, ne crée
   jamais de consent. Repli : approbation humaine avec code hors bande.
6. `approve` exige `action_hash` + code hors bande (jamais visible du LLM/agent). Texte agent/web ne crée jamais de consent.

## Invariants P0

- `completed` broker => `NEEDS_VALIDATION` ; `DONE` seulement après validateur déterministe (artefacts déclarés).
- `lost`/`timeout` : read => nouvelle tentative après confirmation fin (lease/terminal) ; local_write => `NEEDS_AUDIT` ;
  external => `NEEDS_EFFECT_AUDIT`. Job introuvable (purge) => `NEEDS_AUDIT`, jamais resoumission.
- Outbox : payload sérialisé figé avant 1er envoi ; clé = sha(objective, step, attempt) ; réponse perdue => rejouer mêmes octets.
- Kill switch lu dans la transaction qui passe l'outbox à `sending`. Consent consommé dans la même transaction.
- 429 : Retry-After, backoff+jitter, cooldown, circuit breaker ; plafonds durée + nombre totaux.
- Un seul job actif par workspace (verrou + époque).
- `kind=external` refusé en V1. Brave désactivé (profil personnel, pas de profil séparé). Desktop GUI absent.
- Sortie d'agent jamais injectée dans champs privilégiés (workspace, mode, kind, runtime, action_hash).

## Jalons

- J0 preflight + clone + baseline tests (225 pass / 8 skip).
- J1 audits A/B/C + revue Opus + contrat gate.
- J2 db/migrations, FSM, outbox, réconciliation, budgets, kill switch, consents ; tests unitaires + négatifs P0.
- J3 client broker MCP (opencode exclusif, BLOCKED_WORKER), cockpit MCP, politique gateway, gate Opus, handoff.
- J4 tests chaos/sécurité, smoke e2e local (fake broker + vrai MCP in-process), unités systemd staging, rollback, PR.

## Acceptance non satisfiable sans action utilisateur (=> pas de prod)

- « Accès hors sandbox refusé par l'OS » : runner tourne sous le compte personnel, aucune ACL/AppContainer. Requiert compte
  Windows dédié non admin (décision + action utilisateur).
- Modèle Opus servi non attesté (`served_model_last=null`).
- Exposition publique cockpit (nginx + OAuth) = changement infra à autoriser.
