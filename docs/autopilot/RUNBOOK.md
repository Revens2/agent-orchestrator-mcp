# Autopilot V1 — runbook

## Ce que fait le service

`autopilot.service` (VPS, `orch-app`) tourne seul, sans conversation ouverte : à chaque tick (5 s) il
promeut les étapes prêtes, écrit l'intention dans l'outbox, appelle le broker (`127.0.0.1:8802/mcp`),
réconcilie les jobs et valide les sorties. Le runtime est **OpenCode uniquement** ; si OpenCode est
indisponible sur le runner, l'étape passe `BLOCKED_WORKER` et repart seule quand il revient.

## Depuis ChatGPT Web (cockpit)

1. `autopilot_submit(spec, client_key)` : `client_key` = identifiant stable choisi par la conversation ;
   resoumettre la même clé renvoie le même `objective_id`.
2. Nouveau chat : `autopilot_handoff(objective_id)` suffit (faits, hypothèses, blocages, prochaine action).
3. Blocage humain (`PENDING_GATE`, `NEEDS_AUDIT`, `NEEDS_EFFECT_AUDIT`) :
   `autopilot_approve(step_id, resolution)` puis `autopilot_approve(action_hash, code)` avec le code
   reçu **hors bande** (fichier 0600 dans `/srv/autopilot/data/approvals/` ou commande configurée).
   Le code n'est jamais renvoyé au cockpit.
4. Arrêt d'urgence : `autopilot_pause("*")` (kill switch). Désactivation : CLI VPS uniquement :
   `cd /tmp && sudo -u orch-app env $(sudo cat /srv/autopilot/secrets/autopilot.env | xargs) PYTHONPATH=/srv/autopilot/src /srv/orch/venv/bin/python -P -m orch_autopilot.service kill-switch off`

## Sémantique des états

| broker | read | local_write |
|---|---|---|
| completed + contrat OK | DONE (`agent_reported`) | DONE (`agent_reported`) |
| completed sans contrat | nouvelle tentative | NEEDS_AUDIT |
| failed / timeout | nouvelle tentative (backoff) | NEEDS_AUDIT |
| lost | nouvelle tentative après 300 s | NEEDS_AUDIT |
| 429 / rate limit | COOLDOWN (non compté, Retry-After, breaker) | idem |
| job introuvable (purgé) | NEEDS_AUDIT, jamais resoumis | NEEDS_AUDIT |

`kind=external` (publication, DM, paiement, push) est refusé à la soumission en V1.
Décision structurante : gate Opus ; modèle servi non attesté => `PENDING_GATE`.

## Staging

```
# code copié dans /srv/autopilot (src/ + deploy/), env renseigné depuis deploy/autopilot/autopilot.env.example
sudo bash /srv/autopilot/deploy/autopilot/install.sh
curl -s 127.0.0.1:$AUTOPILOT_PORT/health
```

Rollback : `sudo bash /srv/autopilot/deploy/autopilot/rollback.sh` (rien n'est effacé ; stack orch, nginx,
orch.db intacts). L'install staging ne modifie ni `/srv/orch` ni nginx.

## Conditions P0 avant production (non remplies au 2026-10-08)

- Runner Windows sous compte dédié non admin + ACL limitant l'écriture au workspace sandbox
  (aujourd'hui : compte personnel, confinement par code seulement).
- Identité du modèle Opus attestable (`served_model_last` non nul) ou acceptation explicite du repli humain.
- Exposition publique du cockpit (2e instance orch-gateway `ORCH_GW_POLICY=autopilot` + route nginx + OAuth) : décision explicite.
- Canal hors bande opérateur (commande d'alerte existante) choisi.
