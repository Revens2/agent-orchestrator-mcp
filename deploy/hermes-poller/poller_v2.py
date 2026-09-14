"""Poller Hermes v2 pour l'orchestrateur (runner hermes-vps) — durable.

Cause racine v1 (82 lignes : claim + JSON non-atomique + held memoire seule
+ heartbeat aveugle + aucune execution supervisee + cancel jete) :
- journal local SQLite (WAL) : jobs, outbox, meta. Transactions + fsync,
  corruption -> quarantaine (.corrupt-TS) + rebuild, jamais silencieuse.
- import spool v1 (*.json) au boot : continuite du held, pas de requeue.
- held reconstruit depuis le journal a chaque hello : restart -> lease
  repris, fencing conserve.
- supervision Hermes : `docker exec hermes hermes chat` en groupe detache,
  log fichier, transitions claimed->starting->running->terminal fencees.
- recovery explicite : proc vivant -> reattache ; proc mort + session
  reprenable -> --resume meme session ; sinon attente operateur, jamais
  double spawn aveugle.
- outbox idempotente : event_id stables ; stale_fencing/superseded ->
  abandon local, stop publication (fencing strict).
- broker down : tout reste local, flush au retour si fencing valide.
- cancel broker -> kill gracieux puis transition CANCELLED (v1: suppression).
- health.json atomique. Aucun secret dans ce fichier ni en logs.
"""
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.request
import urllib.error

import re

BASE = os.environ.get("ORCH_BROKER_BASE", "http://127.0.0.1:8803")
if not BASE.startswith(("http://", "https://")):
    raise SystemExit("ORCH_BROKER_BASE doit etre une URL http(s)")
TOKEN_FILE = os.environ.get("HERMES_ORCH_TOKEN_FILE",
                            "/var/lib/hermes-ops/.orch-hermes-runner-token")
SPOOL = os.environ.get("HERMES_ORCH_SPOOL", "/var/lib/hermes-ops/orch-jobs")
DB_PATH = os.path.join(SPOOL, "poller.db")
EPOCH_FILE = os.path.join(SPOOL, ".epoch")
HEALTH_FILE = os.path.join(SPOOL, "health.json")
PROMPT_DIR = os.path.join(SPOOL, "prompts")
LOG_DIR = os.path.join(SPOOL, "runs")

RUNNER_ID = "hermes-vps"
VERSION = "hermes-poller/2.1"
MAX_PARALLEL = 10
INFO = {"version": VERSION, "max_parallel": MAX_PARALLEL,
        "runtimes": [{"id": "hermes", "available": True,
                      "modes": ["read_only", "workspace_write"],
                      "version": "intervention-directe-vps-hermes"}],
        "workspaces": [{"id": "vps-etude", "modes": ["read_only", "workspace_write"],
                        "description": "Intervention directe Hermes sur vps-etude"}]}

HB_INTERVAL = 5
CLAIM_WAIT = 25
OUTBOX_FLUSH_EVERY = 5
PROC_POLL_S = 2
OUTPUT_CHUNK = 16000
# `--pass-session-id` : Hermes ecrit `session_id: <id>` sur la sortie.
SESSION_RE = re.compile(r"^session_id:\s*([A-Za-z0-9_.:-]{4,128})\s*$", re.M)
# Invocation non interactive (stdin non-TTY + -Q => oneshot). Surchargeable en test.
HERMES_ARGV = ["docker", "exec", "-i", "hermes", "hermes", "chat",
               "--query-file", "-", "-Q", "--yolo", "--accept-hooks",
               "--pass-session-id"]
# Certains modeles (ex. muse-spark via opencode-free) finissent parfois un long
# tour par un appel outil SERIALISE EN TEXTE (`<atem:function_calls>...`,
# finish_reason=stop) : Hermes le prend pour la reponse finale, exit 0, rien
# n'est execute. Detection ancree en fin de sortie -> relance bornee de la
# meme session, puis echec explicite (jamais un faux `completed`).
LEAKED_TOOL_CALL_RE = re.compile(
    r"</(?:[A-Za-z0-9_.-]+:)?(?:function_calls|tool_calls?|invoke)>\s*$", re.I)
LEAK_RETRIES = 2
LEAK_NUDGE = (
    "Ton message precedent contenait un appel d'outil ecrit en texte "
    "(balises <function_calls>/<invoke>) : il n'a PAS ete execute. "
    "Reprends exactement ou tu en etais en utilisant les VRAIS appels d'outils "
    "natifs, jamais de balises XML dans ta reponse, puis termine la mission.")


def LOG(*a):
    print(*a, flush=True)


def atomic_write(path, data, mode="w"):
    tmp = path + ".tmp"
    with open(tmp, mode) as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class Store:
    """Journal local durable."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS local_jobs(
      job_id TEXT PRIMARY KEY, fencing INTEGER NOT NULL, epoch INTEGER,
      local_state TEXT NOT NULL, session_id TEXT, pid INTEGER,
      proc_started_at REAL, prompt_path TEXT, created_at REAL, updated_at REAL);
    CREATE TABLE IF NOT EXISTS outbox(
      seq INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
      kind TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
    """

    def __init__(self, path=DB_PATH):
        self.path = path
        self.lock = threading.Lock()
        self._open()

    def _open(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        try:
            self.db = sqlite3.connect(self.path, timeout=30,
                                      check_same_thread=False)
            self.db.execute("PRAGMA journal_mode=WAL;")
            self.db.executescript(self.SCHEMA)
            self.db.commit()
            self.db.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('init','1')")
            self.db.commit()
        except sqlite3.DatabaseError:
            q = self.path + ".corrupt-%d" % int(time.time())
            try:
                try:
                    self.db.close()
                except Exception:
                    pass
                shutil.move(self.path, q)
                for sfx in ("-wal", "-shm", "-journal"):
                    try:
                        os.remove(self.path + sfx)
                    except OSError:
                        pass
                LOG("db corrompue quarantine:", q)
            except OSError as e:
                LOG("quarantaine impossible:", type(e).__name__)
                raise
            self.db = sqlite3.connect(self.path, timeout=30,
                                      check_same_thread=False)
            self.db.execute("PRAGMA journal_mode=WAL;")
            self.db.executescript(self.SCHEMA)
            self.db.commit()
            self.set_meta("quarantined", q)
            self.set_meta("recovery_count",
                          str(int(self.get_meta("recovery_count") or 0) + 1))

    def set_meta(self, k, v):
        with self.lock:
            self.db.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            (k, v))
            self.db.commit()

    def get_meta(self, k):
        with self.lock:
            r = self.db.execute("SELECT value FROM meta WHERE key=?", (k,)).fetchone()
            return r[0] if r else None

    def upsert_job(self, job_id, fencing, epoch, local_state, **kw):
        with self.lock:
            now = time.time()
            self.db.execute(
                "INSERT INTO local_jobs(job_id,fencing,epoch,local_state,session_id,"
                "pid,proc_started_at,prompt_path,created_at,updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET"
                " fencing=excluded.fencing, epoch=COALESCE(excluded.epoch,epoch),"
                " local_state=excluded.local_state,"
                " session_id=COALESCE(excluded.session_id,session_id),"
                " pid=COALESCE(excluded.pid,pid),"
                " proc_started_at=COALESCE(excluded.proc_started_at,proc_started_at),"
                " prompt_path=COALESCE(excluded.prompt_path,prompt_path),"
                " updated_at=excluded.updated_at",
                (job_id, fencing, epoch, local_state, kw.get("session_id"),
                 kw.get("pid"), kw.get("proc_started_at"), kw.get("prompt_path"),
                 now, now))
            self.db.commit()

    JOB_COLUMNS = ("fencing", "epoch", "local_state", "session_id", "pid",
                   "proc_started_at", "prompt_path")

    def set_job(self, job_id, **kw):
        bad = set(kw) - set(self.JOB_COLUMNS)
        if bad or not kw:
            raise ValueError("colonnes local_jobs invalides: %s" % sorted(bad))
        cols = [c for c in self.JOB_COLUMNS if c in kw]  # identifiants whitelistes
        sets = ", ".join(c + "=?" for c in cols) + ", updated_at=?"
        with self.lock:
            # colonnes whitelistees ci-dessus, valeurs parametrees
            self.db.execute("UPDATE local_jobs SET " + sets + " WHERE job_id=?",  # nosemgrep
                            (*(kw[c] for c in cols), time.time(), job_id))
            self.db.commit()

    def get_job(self, job_id):
        with self.lock:
            self.db.row_factory = sqlite3.Row
            r = self.db.execute("SELECT * FROM local_jobs WHERE job_id=?",
                                (job_id,)).fetchone()
            self.db.row_factory = None
            return dict(r) if r else None

    def active_jobs(self):
        with self.lock:
            self.db.row_factory = sqlite3.Row
            rows = self.db.execute(
                "SELECT * FROM local_jobs WHERE local_state NOT IN "
                "('done','abandoned') ORDER BY created_at").fetchall()
            self.db.row_factory = None
            return [dict(r) for r in rows]

    def enqueue(self, job_id, kind, event_id, payload):
        with self.lock:
            self.db.execute(
                "INSERT OR IGNORE INTO outbox(job_id,kind,event_id,payload,created_at)"
                " VALUES(?,?,?,?,?)",
                (job_id, kind, event_id, json.dumps(payload), time.time()))
            self.db.commit()

    def outbox_peek(self, limit=20):
        with self.lock:
            self.db.row_factory = sqlite3.Row
            rows = self.db.execute(
                "SELECT * FROM outbox ORDER BY seq LIMIT ?", (limit,)).fetchall()
            self.db.row_factory = None
            return [dict(r) for r in rows]

    def outbox_del(self, seq):
        with self.lock:
            self.db.execute("DELETE FROM outbox WHERE seq=?", (seq,))
            self.db.commit()

    def outbox_bump(self, seq):
        with self.lock:
            self.db.execute("UPDATE outbox SET attempts=attempts+1 WHERE seq=?",
                            (seq,))
            self.db.commit()

    def outbox_depth(self):
        with self.lock:
            return self.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]

    def import_spool_v1(self):
        """Reprend les *.json v1 (claims parques jamais executes)."""
        n = 0
        for name in sorted(os.listdir(SPOOL)):
            if not name.endswith(".json") or name.startswith("."):
                continue
            if name == "health.json":
                continue
            p = os.path.join(SPOOL, name)
            try:
                with open(p) as f:
                    job = json.load(f)
                jid = job.get("job_id")
                if not jid or self.get_job(jid):
                    continue
                prompt_path = self.store_prompt(jid, job.get("prompt") or "")
                self.upsert_job(jid, int(job.get("fencing", 0)), None,
                                "claimed", prompt_path=prompt_path)
                n += 1
            except (OSError, ValueError) as e:
                q = p + ".quarantine-%d" % int(time.time())
                try:
                    os.replace(p, q)
                except OSError:
                    pass
                LOG("spool v1 illisible quarantine:", name, type(e).__name__)
        return n

    def store_prompt(self, job_id, prompt):
        os.makedirs(PROMPT_DIR, exist_ok=True)
        safe = "".join(c for c in job_id if c.isalnum() or c in "-_")[:64]
        path = os.path.join(PROMPT_DIR, safe + ".md")
        atomic_write(path, prompt)
        return path


class Broker:
    def __init__(self, token):
        self.token = token

    def post(self, path, body):
        req = urllib.request.Request(
            BASE + "/runner/v1/" + path,
            data=json.dumps({"protocol_version": 1, **body}).encode(),
            headers={"Content-Type": "application/json",
                     "Authorization": "Bearer " + self.token,
                     "User-Agent": VERSION})
        # BASE valide http(s) au chargement du module
        with urllib.request.urlopen(req, timeout=45) as r:  # nosemgrep
            return json.load(r)

    def hello(self, info, held):
        return self.post("hello", {"info": info, "held": held})

    def heartbeat(self, epoch, held):
        return self.post("heartbeat", {"epoch": epoch, "held": held})

    def claim(self, epoch, slots, wait_s=CLAIM_WAIT):
        return self.post("claim", {"epoch": epoch, "free_slots": slots,
                                   "wait_s": wait_s})

    def event(self, epoch, job_id, fencing, event_id, **kw):
        return self.post("event", {"epoch": epoch, "job_id": job_id,
                                   "fencing": fencing, "event_id": event_id,
                                   **kw})

    def transition(self, epoch, job_id, fencing, src, dst, **kw):
        return self.post("transition", {"epoch": epoch, "job_id": job_id,
                                        "fencing": fencing, "from": src,
                                        "to": dst, **kw})


def proc_alive(pid):
    """Vivant = existe ET n'est pas zombie. v2.0 : os.kill(pid, 0) reussit sur
    un enfant termine non reaped -> boucle de suivi infinie jusqu'au timeout."""
    try:
        os.kill(pid, 0)
    except (OSError, OverflowError):
        return False
    try:
        with open("/proc/%d/stat" % pid) as f:
            return f.read().rsplit(")", 1)[1].split()[0] not in ("Z", "X")
    except (OSError, IndexError):
        return True


class Supervisor(threading.Thread):
    """Execute un job Hermes, publie via outbox (jamais d'appel direct fragile)."""

    def __init__(self, poller, job_id):
        super().__init__(name="sup-%s" % job_id[:8], daemon=True)
        self.poller = poller
        self.job_id = job_id
        self.stop_ev = threading.Event()
        self.cancel_ev = threading.Event()

    def run(self):
        st = self.poller.store
        job = st.get_job(self.job_id)
        if not job:
            return
        # Cas 1: processus local encore vivant -> reattache
        if job.get("pid") and proc_alive(job["pid"]):
            st.set_job(self.job_id, local_state="running")
            st.enqueue(self.job_id, "transition",
                       "tr-%s-running" % self.job_id,
                       {"src": "starting", "to": "running",
                        "runtime_session_id": job.get("session_id"),
                        "pid": job["pid"], "proc_alive": True})
            self._follow_proc(job["pid"], job.get("session_id"), st)
            return
        # Cas 2: session_id connue -> resume meme session Hermes
        resume = job.get("session_id")
        if resume:
            LOG("resume session:", self.job_id, resume[:20])
            self._spawn(st, resume=resume)
            return
        # Cas 3: etat running localement mais pas de session_id (premier echec avant capture)
        # -> ne pas re-spawn betement, marquer recovery, attendre operateur
        if job.get("local_state") == "running":
            LOG("running local sans session_id, recovery:", self.job_id)
            st.set_job(self.job_id, local_state="recovery")
            return
        # Cas 4: recovery explicite sans session -> attente operateur
        if job.get("local_state") == "recovery" and not resume:
            LOG("recovery sans session, attente operateur:", self.job_id)
            return
        # Cas 5: claimed (jamais execute) -> spawn normal
        if job.get("local_state") == "claimed":
            self._spawn(st, resume=None)
            return
        # Default: ne rien faire
        LOG("etat local inattendu, ignore:", self.job_id, job.get("local_state"))

    def _spawn(self, st, resume=None, prompt_text=None):
        job = st.get_job(self.job_id)
        prompt_path = job.get("prompt_path") or ""
        os.makedirs(LOG_DIR, exist_ok=True)
        log_path = os.path.join(LOG_DIR, self.job_id[:8] + ".log")
        if not resume:
            # En resume le broker est deja running : claimed->starting serait
            # un state_conflict -> abandon local du job repris.
            st.enqueue(self.job_id, "transition", "tr-%s-starting" % self.job_id,
                       {"src": "claimed", "to": "starting"})
            st.set_job(self.job_id, local_state="starting")
        # Lire le prompt depuis le fichier local
        if prompt_text is None:
            try:
                with open(prompt_path, "r") as pf:
                    prompt_text = pf.read()
            except OSError:
                prompt_text = ""
        # Passer via stdin (--query-file -) : pas de montage necessaire
        argv = list(HERMES_ARGV)
        if resume:
            argv += ["--resume", resume]
        try:
            lf = open(log_path, "ab")
        except OSError:
            lf = subprocess.DEVNULL
        try:
            p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=lf,
                                 stderr=subprocess.STDOUT, start_new_session=True)
            if prompt_text:
                p.stdin.write(prompt_text.encode("utf-8"))
            p.stdin.close()
        except OSError as e:
            st.enqueue(self.job_id, "transition", "tr-%s-failed" % self.job_id,
                       {"src": "claimed", "to": "failed",
                        "error": "spawn impossible: %s" % type(e).__name__})
            st.set_job(self.job_id, local_state="recovery")
            return
        started = time.time()
        st.set_job(self.job_id, pid=p.pid, proc_started_at=started,
                   local_state="running")
        # Telemetrie A PLAT : le broker ne lit que pid/proc_alive/... au
        # premier niveau (v2.0 l'imbriquait sous "telemetry" -> ignoree).
        st.enqueue(self.job_id, "transition", "tr-%s-running" % self.job_id,
                   {"src": "starting", "to": "running", "pid": p.pid,
                    "proc_alive": True, "proc_started_at": started})
        self._follow_proc(p.pid, None, st, popen=p)

    def _log_path(self):
        return os.path.join(LOG_DIR, self.job_id[:8] + ".log")

    def _pump(self, st, pid, sid, alive):
        """Publie la sortie nouvelle du log comme events `output` (offset
        durable en meta -> event_id stables, pas de doublon apres restart).
        Retourne (session_id, octets publies)."""
        key = "off:" + self.job_id
        off = int(st.get_meta(key) or 0)
        try:
            with open(self._log_path(), "rb") as f:
                f.seek(off)
                data = f.read(OUTPUT_CHUNK)
        except OSError:
            data = b""
        if data and alive:
            nl = data.rfind(b"\n")
            if nl >= 0:
                data = data[:nl + 1]
            elif len(data) < OUTPUT_CHUNK:
                data = b""  # ligne incomplete : attendre la suite
        if not data:
            return sid, 0
        text = data.decode("utf-8", "replace")
        m = None
        for m in SESSION_RE.finditer(text):
            pass
        if m:
            sid = m.group(1)
            st.set_job(self.job_id, session_id=sid)
        st.enqueue(self.job_id, "event", "out-%s-%d" % (self.job_id[:8], off),
                   {"output": text, "runtime_session_id": sid, "pid": pid,
                    "proc_alive": alive})
        st.set_meta(key, str(off + len(data)))
        return sid, len(data)

    def _drain(self, st, pid, sid):
        while True:
            sid, n = self._pump(st, pid, sid, False)
            if not n:
                return sid

    def _summary(self):
        try:
            with open(self._log_path(), "rb") as f:
                f.seek(max(0, os.fstat(f.fileno()).st_size - 4000))
                tail = f.read().decode("utf-8", "replace")
        except OSError:
            return ""
        return SESSION_RE.sub("", tail).strip()[-2000:]

    def _follow_proc(self, pid, session_id, st, popen=None):
        sid = session_id
        while True:
            if self.stop_ev.is_set():
                return  # abandon : le broker ne veut plus de nos transitions
            alive = (popen.poll() is None) if popen is not None else proc_alive(pid)
            if not alive:
                break
            sid, _ = self._pump(st, pid, sid, True)
            if self.cancel_ev.is_set():
                try:
                    os.killpg(pid, signal.SIGTERM)
                except OSError:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except OSError:
                        pass
                for _ in range(10):
                    if (popen.poll() is not None) if popen is not None else not proc_alive(pid):
                        break
                    time.sleep(0.5)
                try:
                    os.killpg(pid, signal.SIGKILL)
                except OSError:
                    pass
                if popen is not None:
                    try:
                        popen.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        pass
                sid = self._drain(st, pid, sid)
                st.enqueue(self.job_id, "transition",
                           "tr-%s-cancelled" % self.job_id,
                           {"src": "running", "to": "cancelled",
                            "runtime_session_id": sid, "pid": pid,
                            "proc_alive": False,
                            "result_summary": self._summary() or None,
                            "error": "annulation demandee par le broker"})
                st.set_job(self.job_id, local_state="done", session_id=sid)
                return
            self.stop_ev.wait(PROC_POLL_S)
        exit_code = None
        if popen is not None:
            try:
                exit_code = popen.wait(timeout=30)
            except subprocess.TimeoutExpired:
                exit_code = None
        sid = self._drain(st, pid, sid)
        summary = self._summary()
        tele = {"runtime_session_id": sid, "pid": pid, "proc_alive": False}
        if exit_code == 0 and LEAKED_TOOL_CALL_RE.search(summary):
            key = "leak:" + self.job_id
            n = int(st.get_meta(key) or 0)
            if sid and n < LEAK_RETRIES:
                st.set_meta(key, str(n + 1))
                LOG("appel outil en texte, relance session:", self.job_id, n + 1)
                self._spawn(st, resume=sid, prompt_text=LEAK_NUDGE)
                return
            st.enqueue(self.job_id, "transition",
                       "tr-%s-failed" % self.job_id,
                       {"src": "running", "to": "failed", "exit_code": 0,
                        "result_summary": summary,
                        "error": "appel outil emis en texte, non execute "
                                 "(%d relance(s))" % n, **tele})
            st.set_job(self.job_id, local_state="done", session_id=sid)
            return
        if exit_code == 0:
            st.enqueue(self.job_id, "transition",
                       "tr-%s-completed" % self.job_id,
                       {"src": "running", "to": "completed", "exit_code": 0,
                        "result_summary": summary, **tele})
        else:
            st.enqueue(self.job_id, "transition",
                       "tr-%s-failed" % self.job_id,
                       {"src": "running", "to": "failed",
                        "exit_code": exit_code if exit_code is not None else 1,
                        "result_summary": summary or None,
                        "error": "processus Hermes termine code=%s" % exit_code,
                        **tele})
        st.set_job(self.job_id, local_state="done", session_id=sid)


class Poller:
    def __init__(self):
        self.store = Store()
        token = open(TOKEN_FILE).read().strip()
        self.broker = Broker(token)
        self.epoch = None
        self.supervisors = {}
        self.last_broker_ok = None

    def held(self):
        out = []
        for j in self.store.active_jobs():
            out.append({"job_id": j["job_id"], "fencing": j["fencing"],
                        "pid": j.get("pid"),
                        "proc_alive": bool(j.get("pid") and proc_alive(j["pid"]))})
        return out

    def held_payload(self):
        """held du heartbeat avec telemetrie a plat quand le pid est connu."""
        out = []
        for h in self.held():
            item = {"job_id": h["job_id"], "fencing": h["fencing"]}
            if h.get("pid"):
                item.update(pid=h["pid"], proc_alive=h["proc_alive"])
            out.append(item)
        return out

    def write_health(self):
        active = self.store.active_jobs()
        try:
            atomic_write(HEALTH_FILE, json.dumps({
                "runner": RUNNER_ID, "epoch": self.epoch,
                "held": [j["job_id"] for j in active],
                "local_active_job": active[0]["job_id"] if active else None,
                "local_session": (active[0].get("session_id")
                                  if active else None),
                "outbox_depth": self.store.outbox_depth(),
                "recovery_count": int(self.store.get_meta("recovery_count") or 0),
                "last_broker_success": self.last_broker_ok,
                "ts": time.time()}, indent=1))
        except OSError as e:
            LOG("health:", type(e).__name__)

    def boot(self):
        n = self.store.import_spool_v1()
        if n:
            LOG("spool v1 importe:", n)
        held = self.held()
        r = self.broker.hello(
            INFO, [{"job_id": h["job_id"], "fencing": h["fencing"]}
                   for h in held])
        self.epoch = r["epoch"]
        atomic_write(EPOCH_FILE, str(self.epoch))
        self.store.set_meta("epoch", str(self.epoch))
        self.last_broker_ok = time.time()
        LOG("hello ok epoch=%s held=%d" % (self.epoch, len(held)))
        for j in self.store.active_jobs():
            self.ensure_supervisor(j["job_id"])

    def ensure_supervisor(self, job_id):
        s = self.supervisors.get(job_id)
        if s and s.is_alive():
            return
        s = Supervisor(self, job_id)
        self.supervisors[job_id] = s
        s.start()

    def flush_outbox(self):
        for item in self.store.outbox_peek(20):
            job = self.store.get_job(item["job_id"])
            if not job or job.get("local_state") == "abandoned":
                self.store.outbox_del(item["seq"])
                continue
            fencing = job["fencing"]
            payload = json.loads(item["payload"])
            try:
                if item["kind"] == "event":
                    self.broker.event(self.epoch, item["job_id"], fencing,
                                      item["event_id"], **payload)
                else:
                    rest = {k: v for k, v in payload.items()
                            if k not in ("src", "to")}
                    self.broker.transition(self.epoch, item["job_id"], fencing,
                                           payload.get("src", "claimed"),
                                           payload["to"], **rest)
                self.store.outbox_del(item["seq"])
                self.last_broker_ok = time.time()
            except urllib.error.HTTPError as e:
                body = ""
                try:
                    body = e.read().decode()[:200]
                except Exception:
                    pass
                if e.code in (409, 404) and any(
                        k in body for k in ("stale_fencing", "superseded",
                                            "unknown_job", "state_conflict",
                                            "invalid_transition")):
                    LOG("fencing refuse, abandon local:", item["job_id"],
                        body[:80])
                    self.store.set_job(item["job_id"], local_state="abandoned")
                    sup = self.supervisors.get(item["job_id"])
                    if sup:
                        sup.stop_ev.set()
                    self.store.outbox_del(item["seq"])
                else:
                    self.store.outbox_bump(item["seq"])
                    raise
            except Exception:
                self.store.outbox_bump(item["seq"])
                raise

    def loop(self):
        self.boot()
        last_flush = 0
        while True:
            try:
                hb = self.broker.heartbeat(self.epoch, self.held_payload())
                self.last_broker_ok = time.time()
                atomic_write(EPOCH_FILE, str(self.epoch))
                for jid in hb.get("cancel", []):
                    job = self.store.get_job(jid)
                    if job and job.get("local_state") not in ("done", "abandoned"):
                        sup = self.supervisors.get(jid)
                        if sup:
                            sup.cancel_ev.set()
                        else:
                            self.store.enqueue(
                                jid, "transition", "tr-%s-cancelled" % jid,
                                {"src": "claimed", "to": "cancelled",
                                 "error": "annulation demandee par le broker"})
                            self.store.set_job(jid, local_state="done")
                        LOG("cancel demande:", jid)
                for jid in hb.get("abandon", []):
                    job = self.store.get_job(jid)
                    if job:
                        self.store.set_job(jid, local_state="abandoned")
                        sup = self.supervisors.get(jid)
                        if sup:
                            sup.stop_ev.set()
                        LOG("abandon:", jid)
                # flush AVANT le claim long-poll : un terminal n'attend pas 25 s
                if time.time() - last_flush > OUTBOX_FLUSH_EVERY:
                    try:
                        self.flush_outbox()
                    except Exception as e:
                        LOG("flush differe:", type(e).__name__)
                    last_flush = time.time()
                free_slots = MAX_PARALLEL - len(self.store.active_jobs())
                if free_slots > 0:
                    for job in self.broker.claim(
                            self.epoch, free_slots).get("jobs", []):
                        jid = job["job_id"]
                        prompt_path = self.store.store_prompt(
                            jid, job.get("prompt") or "")
                        self.store.upsert_job(jid, job["fencing"], self.epoch,
                                              "claimed", prompt_path=prompt_path)
                        self.store.enqueue(
                            jid, "event", "hermes-park-%s" % jid,
                            {"activity": "Job pris en compte par Hermes, "
                                         "execution supervisee demarree."})
                        LOG("job pris:", jid)
                        self.ensure_supervisor(jid)
                self.write_health()
                time.sleep(HB_INTERVAL)
            except Exception as e:
                LOG("boucle:", type(e).__name__, "re-hello 10s")
                self.write_health()
                time.sleep(10)
                try:
                    held = [{"job_id": h["job_id"], "fencing": h["fencing"]}
                            for h in self.held()]
                    self.epoch = self.broker.hello(INFO, held)["epoch"]
                    atomic_write(EPOCH_FILE, str(self.epoch))
                    self.store.set_meta("epoch", str(self.epoch))
                    self.last_broker_ok = time.time()
                except Exception as e2:
                    LOG("re-hello echec:", type(e2).__name__)


if __name__ == "__main__":
    Poller().loop()
