"""Agent factice pour tests d'orchestration. Lit le prompt sur stdin.

Directives (une par ligne) : `sleep S`, `print N`, `flood BYTES`, `spawn S`
(lance un petit-enfant qui dort S secondes), `write NAME TEXT` (dans le cwd),
`echo-prompt-sha`, `fail CODE`. Toute autre ligne est ignorée.
"""

import hashlib
import os
import subprocess
import sys
import time

prompt = sys.stdin.buffer.read().decode("utf-8")
print(f"fake agent started mode={sys.argv[1] if len(sys.argv) > 1 else '?'} pid={os.getpid()}", flush=True)
code = 0
for line in prompt.splitlines():
    parts = line.strip().split(" ", 2)
    cmd = parts[0] if parts else ""
    if cmd == "sleep":
        time.sleep(float(parts[1]))
    elif cmd == "print":
        for i in range(int(parts[1])):
            print(f"line {i}", flush=True)
    elif cmd == "flood":
        chunk = "x" * 1023 + "\n"
        for _ in range(int(parts[1]) // 1024):
            sys.stdout.write(chunk)
        sys.stdout.flush()
    elif cmd == "spawn":
        child = subprocess.Popen([sys.executable, "-c", f"import time; time.sleep({float(parts[1])})"])
        print(f"CHILD_PID:{child.pid}", flush=True)
    elif cmd == "write":
        with open(parts[1], "w", encoding="utf-8") as fh:
            fh.write(parts[2])
    elif cmd == "echo-prompt-sha":
        print("PROMPT_SHA256:" + hashlib.sha256(prompt.encode("utf-8")).hexdigest(), flush=True)
    elif cmd == "fail":
        code = int(parts[1])
print(f"SUMMARY: fake done code={code}", flush=True)
sys.exit(code)
