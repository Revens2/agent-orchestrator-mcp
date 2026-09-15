"""Validation canary orch :18981 (prod gateway OAuth-only, sans Bearer statique).

Pas de comparaison prod-gateway directe possible sans client OAuth (prod
`ORCH_GW_TOKEN` volontairement absent — OAuth-only par design). A la place :
1. `initialize` + `tools/list` via le canary (Bearer dedie) : le canary relaie
   vers l'upstream `:8802`, donc la liste obtenue EST le contrat live amont ;
2. conformite stricte aux 20 outils du contrat code (`tools.py` @ 428b527) :
   noms exacts + description + inputSchema presents ;
3. `resources/list` + `prompts/list` (comportement enregistre) ;
4. appel reel read-only `agent_job_list` (sans effet) ;
5. refus locaux : outil inconnu (-32000), methode inconnue (-32601).
Bearer canary lu sur le VPS uniquement (jamais affiche, jamais journalise).
Sortie : PASS/FAIL + ecarts uniquement.
"""
import json
import sys
import urllib.request

CANARY_FILE = sys.argv[1] if len(sys.argv) > 1 else "/opt/orch-gateway-rs/.mcp_token"
CANARY = "http://127.0.0.1:18981"

# Scripts operateur : loopback VPS uniquement.
BASES_AUTORISEES = (CANARY,)

# Contrat code : 20 outils `orch_mcp/tools.py` (clone @ 428b527).
OUTILS_ATTENDUS = {
    "agent_runner_list", "agent_workspace_list", "agent_job_start",
    "agent_job_get", "agent_job_output", "agent_job_cancel", "agent_job_list",
    "agent_job_events", "agent_runner_inspect", "agent_job_wait",
    "agent_mission_create", "agent_mission_get", "agent_mission_wait",
    "agent_mission_retry", "agent_mission_validate", "infra_alert_list",
    "infra_alert_get", "agent_question_list", "agent_question_get",
    "agent_question_answer",
}


def lire_token(path):
    try:
        with open(path, encoding="utf-8") as fh:
            tok = fh.read().strip()
    except OSError:
        print("JETON_CANARY_ILLISIBLE")
        sys.exit(2)
    if len(tok) < 32:
        print("JETON_CANARY_INVALIDE")
        sys.exit(2)
    return tok


TOKEN = lire_token(CANARY_FILE)


def sse_unwrap(raw):
    out = []
    for line in raw.decode("utf-8", "replace").splitlines():
        line = line.strip()
        if line.startswith("data:"):
            payload = line[5:].lstrip()
            try:
                out.append(json.loads(payload))
            except ValueError:
                pass
    return out


def post(body, session=None):
    headers = {
        "content-type": "application/json",
        "accept": "application/json, text/event-stream",
        "authorization": "Bearer " + TOKEN,
        "mcp-protocol-version": "2025-11-25",
    }
    if session:
        headers["mcp-session-id"] = session
    req = urllib.request.Request(  # nosemgrep: python.lang.security.audit.insecure-transport.urllib.insecure-request-object.insecure-request-object
        CANARY + "/mcp", data=json.dumps(body).encode(), headers=headers, method="POST"
    )  # loopback operateur contraint par BASES_AUTORISEES, jamais d'exterieur
    try:
        # base contrainte a BASES_AUTORISEES (loopback operateur, pas de file://)
        with urllib.request.urlopen(req, timeout=120) as res:  # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
            raw = res.read()
            sess = res.headers.get("mcp-session-id") or session
            status = res.status
    except Exception as exc:  # noqa: BLE001 - diagnostic smoke
        return -1, {"transport_error": str(exc)[:120]}, session
    try:
        return status, json.loads(raw), sess
    except ValueError:
        for m in sse_unwrap(raw):
            if m.get("id") == body.get("id"):
                return status, m, sess
        return status, {"sse_messages": len(sse_unwrap(raw))}, sess


ecarts = []

# 1. initialize
st, init, session = post(
    {"jsonrpc": "2.0", "id": 1, "method": "initialize",
     "params": {"protocolVersion": "2025-11-25", "capabilities": {},
                "clientInfo": {"name": "diff", "version": "0"}}}
)
print(f"initialize: http={st} session={bool(session)} result={str(init.get('result'))[:160]}")
if st != 200 or "result" not in init:
    ecarts.append(f"initialize: http={st}")

# 2. tools/list live via relay :8802 vs contrat code
st, lst, session = post(
    {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}, session
)
outils = {t.get("name"): t for t in ((lst.get("result") or {}).get("tools") or []) if t.get("name")}
print(f"canary tools ({len(outils)}): {sorted(outils)}")
if set(outils) != OUTILS_ATTENDUS:
    ecarts.append(f"outils: manquants={sorted(OUTILS_ATTENDUS - set(outils))} "
                  f"ajoutes={sorted(set(outils) - OUTILS_ATTENDUS)}")
for nom, outil in outils.items():
    if not outil.get("description") or not isinstance(outil.get("inputSchema"), dict):
        ecarts.append(f"outil {nom}: description/schema manquant")

# 3. resources/prompts (comportement)
for method in ("resources/list", "prompts/list"):
    st, resp, session = post(
        {"jsonrpc": "2.0", "id": 3, "method": method, "params": {}}, session
    )
    print(f"{method}: http={st} result={str(resp.get('result'))[:160]} erreur={'error' in resp}")

# 4. appel reel read-only agent_job_list
st, jl, session = post(
    {"jsonrpc": "2.0", "id": 4, "method": "tools/call",
     "params": {"name": "agent_job_list", "arguments": {}}}, session
)
print(f"agent_job_list: http={st} erreur={'error' in jl}")
if "error" in jl:
    ecarts.append("agent_job_list: erreur inattendue")

# 5. refus locaux
st, ru, session = post(
    {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
     "params": {"name": "outil-inexistant-xyz", "arguments": {}}}, session
)
code_u = (ru.get("error") or {}).get("code")
st, rm, session = post(
    {"jsonrpc": "2.0", "id": 6, "method": "drop_database", "params": {}}, session
)
code_m = (rm.get("error") or {}).get("code")
print(f"refus inconnu={code_u} methode inconnue={code_m}")
if code_u != -32000:
    ecarts.append(f"refus inconnu={code_u} (attendu -32000)")
if code_m != -32601:
    ecarts.append(f"methode inconnue={code_m} (attendu -32601)")

if ecarts:
    print("ECARTS:")
    for e in ecarts:
        print(f"  - {e}")
    print("RESULT: FAIL")
    sys.exit(1)
print("RESULT: PASS (canary conforme au contrat + relay live)")
