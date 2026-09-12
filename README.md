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
                         orch-mcp      127.0.0.1:8802   MCP (7 outils) + broker SQLite (autorité)
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
| `agent_job_get` | lecture | vue compacte + `output_tail` borné |
| `agent_job_output` | lecture | sortie paginée (`cursor`, `limit` ≤ 20 000) |
| `agent_job_cancel` | écriture | `cancelled` / `cancel_requested` / `already_finished` / `unknown_job` |
| `agent_job_list` | lecture | liste filtrable |

États : `queued → claimed → starting → running → completed | failed | timeout | cancelled | lost`.

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
