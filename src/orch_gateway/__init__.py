"""Passerelle d'authentification de l'orchestrateur d'agents IA.

Copie-adaptation du pattern `vault-mcp` / `calendar-mcp-gateway` (adr/0015, adr/0016) :
serveur d'autorisation OAuth 2.1 colocalisé (SDK python `mcp`), page de consentement,
jeton Bearer statique pour les CLI, et proxy transparent vers l'upstream orch-mcp
(127.0.0.1:8802) sur `/mcp`. La passerelle n'est PAS un serveur MCP : elle valide le
jeton puis relaie le trafic Streamable HTTP tel quel.
"""

__version__ = "1.0.0"
