"""Linux runtime evidence and durable launcher. No broker credentials here.

The docker CLI is a transport, not the Hermes process. Resolve the engine exec
identity, observe its host PID, and read native session activity in read-only
mode. Missing evidence stays unknown. Never infer success from a session title
or an assistant response.
"""
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time


def atomic_json(path, value):
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def process_info(pid):
    if not isinstance(pid, int) or pid <= 0:
        return None
    try:
        stat = Path("/proc/%d/stat" % pid).read_text().rsplit(")", 1)[1].split()
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        btime = next(int(x.split()[1]) for x in Path("/proc/stat").read_text().splitlines() if x.startswith("btime "))
        return {"pid": pid, "identity": boot + ":" + stat[19],
                "alive": stat[0] not in ("Z", "X"), "ppid": int(stat[1]),
                "started_at": btime + int(stat[19]) / os.sysconf("SC_CLK_TCK")}
    except (OSError, ValueError, IndexError, StopIteration):
        return None


def process_children(pid):
    """Count descendants, including tools outside the docker client tree."""
    parents = {}
    try:
        for p in Path("/proc").iterdir():
            if p.name.isdigit():
                try:
                    fields = p.joinpath("stat").read_text().rsplit(")", 1)[1].split()
                    if fields[0] not in ("Z", "X"):
                        parents[int(p.name)] = int(fields[1])
                except (OSError, ValueError, IndexError):
                    pass
    except OSError:
        return None
    found = {pid}
    while True:
        children = {p for p, parent in parents.items() if parent in found}
        new = children - found
        if not new:
            return len(found) - 1
        found.update(new)


class UnixHTTP(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect("/var/run/docker.sock")


def docker_get(path):
    conn = UnixHTTP("localhost", timeout=3)
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        if response.status == 404:
            return None
        if response.status != 200:
            raise OSError("docker inspect status %d" % response.status)
        return json.load(response)
    finally:
        conn.close()


def find_exec(job, source):
    candidates = []
    for eid in (docker_get("/containers/hermes/json") or {}).get("ExecIDs") or []:
        ex = docker_get("/exec/" + eid + "/json")
        if not ex:
            continue
        cfg = ex.get("ProcessConfig") or {}
        args = cfg.get("arguments") or []
        if cfg.get("entrypoint") != "hermes" or "chat" not in args:
            continue
        if source in args:
            candidates.append(ex)
        elif "--source" not in args:
            # Legacy migration: an exact start-time match, never "first exec".
            proc = process_info(ex.get("Pid"))
            started = job.get("proc_started_at")
            if proc and started and abs(proc["started_at"] - started) < 10:
                candidates.append(ex)
    return candidates[0] if len(candidates) == 1 else None


# Executed inside the container with JSON stdin. SQL is read-only/parameterized;
# no credentials, command arguments, tool results or prompts are returned.
NATIVE_QUERY = r'''
import json, os, sqlite3, sys
q = json.load(sys.stdin)
path = os.path.join(os.environ.get("HERMES_HOME", "/opt/data"), "state.db")
db = sqlite3.connect("file:" + path + "?mode=ro", uri=True, timeout=2)
db.row_factory = sqlite3.Row
sid = q.get("session_id")
if not sid:
    rows = db.execute("SELECT id FROM sessions WHERE source=?", (q["source"],)).fetchall()
    if not rows:
        rows = db.execute("SELECT DISTINCT s.id FROM sessions s JOIN messages m ON s.id=m.session_id "
                          "WHERE m.role='user' AND m.content=? AND abs(s.started_at-?) < 120",
                          (q.get("prompt", ""), q["started_at"])).fetchall()
    if len(rows) != 1:
        print("null"); sys.exit(0)
    sid = rows[0]["id"]
session = db.execute("SELECT id,ended_at,end_reason FROM sessions WHERE id=?", (sid,)).fetchone()
if session is None:
    print("null"); sys.exit(0)
rows = db.execute("SELECT id,role,tool_name,substr(content,1,4000) AS content,timestamp "
                  "FROM messages WHERE session_id=? AND id>? ORDER BY id LIMIT 20",
                  (sid, q.get("cursor", 0))).fetchall()
messages = [{"id": r["id"], "role": r["role"], "tool": r["tool_name"], "at": r["timestamp"],
             "text": r["content"] if r["role"] == "assistant" else None} for r in rows]
print(json.dumps({"session_id": sid, "ended_at": session["ended_at"], "messages": messages}))
'''


def native_snapshot(job, source, cursor, hermes_home="/opt/data"):
    prompt = ""
    if not job.get("session_id") and job.get("prompt_path"):
        prompt = Path(job["prompt_path"]).read_text()
    query = {"session_id": job.get("session_id"), "source": source,
             "started_at": job.get("proc_started_at") or job["created_at"],
             "prompt": prompt, "cursor": cursor}
    result = subprocess.run(
        ["docker", "exec", "-i", "-e", "HERMES_HOME=" + hermes_home,
         "hermes", "python3", "-c", NATIVE_QUERY],
        input=json.dumps(query), capture_output=True, text=True, timeout=6,
    )
    if result.returncode:
        raise OSError("native session read failed")
    return json.loads(result.stdout)


def launch(descriptor):
    """Survives poller restart, reaps the child, and fsyncs its exit receipt.

    Descriptor/receipts belong to one fenced launch generation. The prompt is
    an input file, avoiding an unbounded stdin.write before PID persistence.
    """
    spec = json.loads(Path(descriptor).read_text())
    receipt = spec["receipt"]
    try:
        with open(spec["prompt"], "rb") as prompt, open(spec["log"], "ab", buffering=0) as log:
            proc = subprocess.Popen(spec["argv"], stdin=prompt, stdout=log,
                                    stderr=subprocess.STDOUT, start_new_session=True)
            info = process_info(proc.pid)
            atomic_json(receipt, {"pid": proc.pid, "identity": info["identity"] if info else None,
                                  "started_at": time.time(), "exit_code": None})
            code = proc.wait()
            atomic_json(receipt, {"pid": proc.pid, "identity": info["identity"] if info else None,
                                  "exit_code": code, "finished_at": time.time()})
    except Exception as exc:
        atomic_json(receipt, {"exit_code": None, "error": type(exc).__name__, "finished_at": time.time()})


if __name__ == "__main__":
    launch(sys.argv[1])
