# Consignes agents — agent-orchestrator-mcp

Orchestrateur d'agents personnel (broker MCP + runners). Sécurité d'abord.

## Règles

- Ne jamais accepter commande, chemin ou exécutable depuis le réseau : uniquement
  des identifiants d'allowlist (runner, runtime, workspace), mode borné, prompt = donnée.
- Ne jamais substituer un runtime à la volée : `runtime_unavailable` se remonte
  tel quel (« agent indisponible + raison »), sauf demande explicite.
- Chaque runtime a son propre quota : aucun fallback auto inter-runtimes.
- Aucun secret dans Git. Vérifier `git status` / `git diff` avant tout commit.
  Ne jamais committer/pusher sans demande explicite.
- `completed` (exit 0) ne valide jamais une mission : seule `agent_mission_validate` atteste.

## Vérification

- `python -m pytest tests/ -q` (sauf `test_real_runtimes.py` : consomme du quota,
  activation explicite uniquement ; `test_runner_windows.py` : processus réels, long).
- `ruff check src/ tests/`
