"""Client MCP de test (JSON-RPC streamable-http stateless) : démarre un job et le suit jusqu'à la fin.

Usage (sur le VPS, contre l'upstream loopback, sans OAuth) :
  python mcp_smoke.py http://127.0.0.1:8802/mcp fake e2e "print 3" [mode]
Affiche chaque changement d'état et la vue finale compacte.
"""

import json
import sys
import time
import uuid

import httpx

URL, RUNTIME, WS, PROMPT = sys.argv[1:5]
MODE = sys.argv[5] if len(sys.argv) > 5 else "workspace_write"
H = {"accept": "application/json, text/event-stream", "content-type": "application/json", "mcp-protocol-version": "2025-06-18"}


def call(name, args):
    r = httpx.post(URL, headers=H, timeout=60, json={"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": args}})
    body = r.text
    if "data:" in body:
        body = "\n".join(line[5:].strip() for line in body.splitlines() if line.startswith("data:"))
    res = json.loads(body)["result"]
    return res.get("structuredContent") or json.loads(res["content"][0]["text"])


runners = call("agent_runner_list", {})["runners"]
print("runners:", [(r["id"], r["status"], [x["id"] for x in r["runtimes"] if x.get("available")]) for r in runners])
print("workspaces:", call("agent_workspace_list", {})["workspaces"])
prompt = PROMPT.replace("\\n", "\n").replace("{NONCE}", "E2E-" + uuid.uuid4().hex[:12])
started = call("agent_job_start", {"runner_id": "main-windows-pc", "runtime": RUNTIME, "workspace_id": WS, "prompt": prompt, "mode": MODE, "idempotency_key": uuid.uuid4().hex})
print("start:", started)
job_id, last = started["job_id"], None
t0 = time.time()
while True:
    view = call("agent_job_get", {"job_id": job_id, "tail_chars": 300})
    if view["state"] != last:
        print(f"+{time.time() - t0:5.1f}s state={view['state']} activity={view.get('last_activity')!r}")
        last = view["state"]
    if view["state"] in ("completed", "failed", "timeout", "cancelled", "lost"):
        break
    time.sleep(1)
print(json.dumps({k: view.get(k) for k in ("job_id", "state", "exit_code", "duration_s", "result_summary", "error", "runtime_session_id", "output_chars")}, ensure_ascii=False, indent=1))
print("PROMPT_USED:", prompt)
