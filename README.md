# agent-orchestrator-mcp

MCP distant qui permet à ChatGPT Web de lancer, suivre et annuler de **vrais agents IA existants**
(Claude Code, Codex, Antigravity, OpenCode) sur un PC Windows personnel, sans exposer de shell distant.

```text
ChatGPT Web ──HTTPS + OAuth 2.1 (DCR, PKCE, consentement phrase de passe)──┐
                                                                            ▼
                         nginx mymcps.duckdns.org:443   /orch/mcp, /oauth/orch/*, .well-known
                                                                            ▼
                         orch-gateway  127.0.0.1:8801   OAuth + politique outil par outil
                                                                            ▼
                          orch-mcp      127.0.0.1:8802   MCP (20 outils) + broker SQLite (autorité)
                                                                            ▲
                         nginx 10.200.114.203:8803      /runner/v1/* — IP NetBird uniquement
                                                                            │ long-poll SORTANT
                         PC Windows : orch-runner (tâche planifiée, session utilisateur)
                           └─ Job Object ─▶ claude.exe / codex.exe / agy.exe / opencode.exe
                                             dans un workspace allowlisté
```

## Principes

- **Pas de shell** : les outils MCP n'acceptent que `runner_id`, `runtime` (enum), `workspace_id`
  (identifiant d'allowlist), `mode` (`read_only` | `workspace_write`) et un `prompt` traité comme donnée.
- **Le runner construit l'invocation** via un adapter par runtime (argv figé, `.exe` absolu, jamais
  `cmd.exe`/PowerShell), revalide workspace/runtime/mode localement, lance le processus **suspendu**
  dans un **Job Object** (`KILL_ON_JOB_CLOSE`) puis le reprend.
- **Agents autonomes, MCP fermé** : `default_permission_policy` (runner.toml, défaut `unattended`) donne
  à chaque runtime son mode sans demande d'autorisation en `workspace_write` ; `read_only` reste lecture
  seule ; `guarded` pour revenir aux éditions seules. Détail par runtime : `docs/OPERATIONS.md`.
  L'autonomie est interne au runtime : le MCP n'expose toujours ni shell, ni flags, ni argv.
- **Le broker est la source d'autorité** : transitions compare-and-set avec *fencing token* et epoch de
  session runner ; un job perdu après lancement devient `lost`, **jamais relancé ni déclaré `completed`**.
- **Aucun port entrant sur le PC** : le runner se connecte en sortie au VPS via NetBird.
- **Pas de duplication ConvIA** : le broker ne garde que métadonnées, sortie bornée (2 Mo/job) et
  résumé ; prompts purgés à 7 j, sorties à 7 j, métadonnées à 90 j.

## Outils MCP

| Outil | Portée | Rôle |
|---|---|---|
| `agent_runner_list` | lecture | runners, présence réelle (heartbeat < 30 s), runtimes, jobs actifs |
| `agent_workspace_list` | lecture | workspaces autorisés (ids + modes), jamais de chemin |
| `agent_job_start` | écriture | crée un job `queued`, retour immédiat ; `idempotency_key` optionnelle |
| `agent_job_get` | lecture | état structuré d'exécution + `output_tail` borné : heartbeat runner, processus (pid, vivant, enfants), progression, `execution_health`, couches `broker/runner/process` |
| `agent_job_output` | lecture | sortie paginée (`cursor`, `limit` ≤ 20 000) |
| `agent_job_events` | lecture | journal structuré borné et paginé (`after_seq`, `limit` ≤ 200), pas de transcript |
| `agent_runner_inspect` | lecture | snapshot runner : versions, capacités, workspaces, git (branch/HEAD/dirty), jobs actifs |
| `agent_job_wait` | lecture | long-poll borné (≤ 60 s) ; retour machine-lisible (`terminal`, `should_continue`/`must_follow`, `next_tool`, curseur `since_seq`) : un timeout non terminal impose de rappeler dans le même tour, jamais de répondre |
| `agent_mission_wait` | lecture | attente bornée (≤ 60 s) sur la tentative courante d'une mission (`mission_state`, `terminal`, `should_continue`, `next_tool` = `agent_mission_wait` ou `agent_mission_validate`) |
| `agent_job_cancel` | écriture | `cancelled` / `cancel_requested` / `already_finished` / `unknown_job` |
| `agent_job_list` | lecture | liste filtrable |
| `agent_mission_create` | écriture | mission (objectif + critères) + 1re tentative ; jamais de retry auto |
| `agent_mission_get` | lecture | objectif, critères, tentatives, job courant, validation |
| `agent_mission_retry` | écriture | nouvelle tentative explicite de la même mission (≤ `max_attempts`) |
| `agent_mission_validate` | écriture | `validated` \| `incomplete` \| `blocked` \| `failed` (seule preuve de succès) |
| `infra_alert_list` | lecture | alertes infra [ETUDE]/[NEXUS] filtrables (source, sévérité, état, période), vue compacte |
| `infra_alert_get` | lecture | détail borné d'une alerte (erreur, contexte, empreinte, occurrences) |
| `agent_question_list` | lecture | questions en attente (waiting_for_user explicite), inbox corrélée |
| `agent_question_get` | lecture | détail question + réponse éventuelle |
| `agent_question_answer` | écriture | réponse single-use routée à la session émettrice (jamais un shell) |

États : `queued → claimed → starting → running → completed | failed | timeout | cancelled | lost`.

> **Sémantique** : `completed` = processus terminé avec exit 0, **PAS mission validée**.
> Un job `completed` passe sa mission en `needs_validation` ; seul `agent_mission_validate`
> (humain/ChatGPT après examen) fait passer à `validated`. Un `lost` garde « issue inconnue » :
> `agent_job_get` expose alors les couches `broker_health` / `runner_health` /
> `runtime_process_health` pour diagnostiquer la cause observable, et `agent_job_events`
> le journal (`lease_expired`, `runner_disconnect`, …).
>
> **Stalls** : processus vivant + silence d'activité/output ≥ 10 min → événement
> `suspected_stall`, ≥ 30 min → `stalled`. Notification seule : jamais de relance ni
> d'annulation automatique (surtout pas pour une mission d'écriture).

## Arborescence

```text
src/orch_protocol/   contrat partagé (états, transitions, bornes, redaction)
src/orch_mcp/        broker SQLite, API runner, outils MCP, serveur
src/orch_gateway/    passerelle OAuth (dérivée de tasks_gateway)
src/orch_runner/     runner Windows (Job Object, politique, adapters, démon)
deploy/              systemd, nginx, install.sh, rollback.sh, windows/install-runner.ps1
docs/OPERATIONS.md   exploitation
tests/               unitaires, E2E runner↔broker HTTP, runtimes réels (ORCH_REAL=…)
```

Exploitation : voir [docs/OPERATIONS.md](docs/OPERATIONS.md).
