"""Appel unitaire d'un outil MCP orch (JSON-RPC stateless). Sortie JSON sur stdout.

  python mcp_call.py URL TOOL '{"json": "args"}'
"""

import json
import sys

import httpx

URL, TOOL = sys.argv[1], sys.argv[2]
ARGS = json.loads(sys.argv[3]) if len(sys.argv) > 3 else {}
H = {"accept": "application/json, text/event-stream", "content-type": "application/json", "mcp-protocol-version": "2025-06-18"}
r = httpx.post(URL, headers=H, timeout=60, json={"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": TOOL, "arguments": ARGS}})
body = r.text
if "data:" in body:
    body = "\n".join(line[5:].strip() for line in body.splitlines() if line.startswith("data:"))
msg = json.loads(body)
if "error" in msg:
    print(json.dumps(msg["error"]))
    sys.exit(1)
res = msg["result"]
text = res["content"][0]["text"] if res.get("content") else ""
try:
    out = res.get("structuredContent") or json.loads(text)
except ValueError:
    out = {"text": text[:300]}
if res.get("isError"):
    out = {"isError": True, **(out if isinstance(out, dict) else {"content": out})}
print(json.dumps(out, ensure_ascii=False))
