"""Diff contrat zero live : prod Python :8801 vs canary Rust :18981.

Secrets lus depuis leurs fichiers 600 sur le VPS uniquement (jamais affiches,
jamais journalises). Echec de parsing = message statique, exit 2.
Compare : initialize, tools/list (16 noms + descriptions + inputSchema),
resources/list + prompts/list (parite de comportement), tools/call reel
read-only `agent_job_list`, refus local outil inconnu (-32000 des deux cotes).
Sortie : PASS/FAIL + diffs uniquement.
"""
import json
import re
import sys
import urllib.request

PROD_FILE = sys.argv[1] if len(sys.argv) > 1 else "/srv/orch/secrets/orch.env"
CANARY_FILE = sys.argv[2] if len(sys.argv) > 2 else "/opt/orch-gateway-rs/.mcp_token"
PROD = "http://127.0.0.1:8801"
CANARY = "http://127.0.0.1:18981"

# Scripts operateur : loopback VPS uniquement.
BASES_AUTORISEES = (PROD, CANARY)


def lire_prod_env(path):
    try:
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
    except OSError:
        print("ENV_PROD_ILLISIBLE")
        sys.exit(2)
    m = re.search(
        r"(?m)^\s*(?:export\s+)?ORCH_GW_TOKEN\s*=\s*['\"]?([^'\"\r\n]+)['\"]?\s*$",
        content,
    )
    if not m:
        print("ENV_PROD_SANS_JETON")
        sys.exit(2)
    tok = m.group(1).strip()
    if len(tok) < 32 or "\n" in tok:
        print("ENV_PROD_JETON_INVALIDE")
        sys.exit(2)
    return tok


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


TOKEN_PROD = lire_prod_env(PROD_FILE)
TOKEN_CANARY = lire_token(CANARY_FILE)


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


def post(base, token, body, session=None):
    assert base in BASES_AUTORISEES, "loopback VPS uniquement"
    headers = {
        "content-type": "application/json",
        "accept": "application/json, text/event-stream",
        "authorization": "Bearer " + token,
        "mcp-protocol-version": "2025-11-25",
    }
    if session:
        headers["mcp-session-id"] = session
    req = urllib.request.Request(
        base + "/mcp", data=json.dumps(body).encode(), headers=headers, method="POST"
    )
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


def tools_map(resp):
    tools = ((resp.get("result") or {}).get("tools") or [])
    return {t.get("name"): t for t in tools if t.get("name")}


def norm_tool(t):
    return {
        "name": t.get("name"),
        "description": t.get("description"),
        "inputSchema": t.get("inputSchema"),
    }


diffs = []
sessions = {}

# 1. initialize
for tag, base, token in (("prod", PROD, TOKEN_PROD), ("canary", CANARY, TOKEN_CANARY)):
    _, payload, sess = post(
        base, token,
        {"jsonrpc": "2.0", "id": 1, "method": "initialize",
         "params": {"protocolVersion": "2025-11-25", "capabilities": {},
                    "clientInfo": {"name": "diff", "version": "0"}}},
    )
    sessions[tag] = sess
    globals()[f"init_{tag}"] = payload

pi, ci = init_prod, init_canary
if (pi.get("result") or {}).get("protocolVersion") != (ci.get("result") or {}).get("protocolVersion"):
    diffs.append("initialize.protocolVersion")
print(f"initialize: prod={str(pi.get('result'))[:120]}")
print(f"initialize: canary={str(ci.get('result'))[:120]}")
print(f"sessions: prod={bool(sessions['prod'])} canary={bool(sessions['canary'])}")

# 2. tools/list : noms + descriptions + schemas
_, pl, _ = post(PROD, TOKEN_PROD,
                {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
                sessions["prod"])
_, cl, _ = post(CANARY, TOKEN_CANARY,
                {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
                sessions["canary"])
pm, cm = tools_map(pl), tools_map(cl)
for name in sorted(set(pm) | set(cm)):
    if name not in pm:
        diffs.append(f"outil ajoute (canary seul) : {name}")
    elif name not in cm:
        diffs.append(f"outil perdu : {name}")
    elif norm_tool(pm[name]) != norm_tool(cm[name]):
        diffs.append(f"outil modifie : {name}")
print(f"prod tools ({len(pm)}): {sorted(pm)}")
print(f"canary tools ({len(cm)}): {sorted(cm)}")

# 3. resources/list + prompts/list : parite de comportement
for method in ("resources/list", "prompts/list"):
    _, pr, _ = post(PROD, TOKEN_PROD,
                    {"jsonrpc": "2.0", "id": 3, "method": method, "params": {}},
                    sessions["prod"])
    _, cr, _ = post(CANARY, TOKEN_CANARY,
                    {"jsonrpc": "2.0", "id": 3, "method": method, "params": {}},
                    sessions["canary"])
    jp = json.dumps(pr.get("result"), sort_keys=True)
    jc = json.dumps(cr.get("result"), sort_keys=True)
    print(f"{method}: prod={jp[:200]} canary={jc[:200]}")
    if ("error" in pr) != ("error" in cr) or jp != jc:
        diffs.append(f"{method}: divergence")

# 4. tools/call reel read-only agent_job_list
_, pt, _ = post(PROD, TOKEN_PROD,
                {"jsonrpc": "2.0", "id": 4, "method": "tools/call",
                 "params": {"name": "agent_job_list", "arguments": {}}}, sessions["prod"])
_, ct, _ = post(CANARY, TOKEN_CANARY,
                {"jsonrpc": "2.0", "id": 4, "method": "tools/call",
                 "params": {"name": "agent_job_list", "arguments": {}}}, sessions["canary"])
pe, ce = "error" in pt, "error" in ct
print(f"agent_job_list: prod_erreur={pe} canary_erreur={ce}")
if pe != ce:
    diffs.append("agent_job_list: erreur d'un seul cote")

# 5. refus local outil inconnu : -32000 des deux cotes
_, pu, _ = post(PROD, TOKEN_PROD,
                {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
                 "params": {"name": "outil-inexistant-xyz", "arguments": {}}},
                sessions["prod"])
_, cu, _ = post(CANARY, TOKEN_CANARY,
                {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
                 "params": {"name": "outil-inexistant-xyz", "arguments": {}}},
                sessions["canary"])
code_p = (pu.get("error") or {}).get("code")
code_c = (cu.get("error") or {}).get("code")
print(f"refus inconnu: prod={code_p} canary={code_c}")
if code_p != -32000 or code_c != -32000:
    diffs.append(f"refus inconnu: prod={code_p} canary={code_c} (attendu -32000)")

if diffs:
    print("DIFFS:")
    for d in diffs:
        print(f"  - {d}")
    print("RESULT: FAIL")
    sys.exit(1)
print("RESULT: PASS (diff contrat zero)")
