"""Poller Hermes v2 pour l'orchestrateur (runner hermes-vps) — durable.

v2.3 : multi-instance sur. Pool de profils Hermes dedies (un slot = un
profil = un HERMES_HOME isole), affectation persistee en SQLite, porte
d'admission (workspace_write serialise par workspace, read_only concurrent).
Voir PROFILE_POOL/gate(). Garanties v2.x inchangees ci-dessous.

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
import http.client
import os
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import sys
from pathlib import Path
import urllib.request
import urllib.error

import re

try:
    import runtime_support as runtime
except ImportError:  # tests (spec_from_file_location) ou CWD inattendu
    import importlib.util as _ilu
    _rs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "runtime_support.py")
    _rs_spec = _ilu.spec_from_file_location("runtime_support", _rs_path)
    runtime = _ilu.module_from_spec(_rs_spec)
    _rs_spec.loader.exec_module(runtime)

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
VERSION = "hermes-poller/2.3"
MAX_PARALLEL = 10
# Isolation multi-job (2.3) : un profil Hermes dedie par job simultane.
# Chaque slot du pool correspond a un profil `orch-slot-NN` (provisionne
# par `hermes profile create <slot> --clone`) donc a un HERMES_HOME
# distinct (/opt/data/profiles/<slot>) : sessions, memoires et checkpoints
# ne sont jamais partages entre deux jobs simultanes. L'affectation est
# persistee en SQLite (table slots) : restart/recovery reutilise la meme
# identite, jamais de double spawn. Mecanisme verifie localement le
# 2026-09-15 : `hermes -p <slot> chat` isole state.db/sessions ; --clone
# reprend .env/SOUL/skills (auth OK, preuve PROOF_OK).
POOL_SIZE = 10  # == MAX_PARALLEL : un slot par job simultane possible
PROFILE_POOL = ["orch-slot-%02d" % i for i in range(POOL_SIZE)]
PROFILE_HOME = "/opt/data/profiles/%s"  # chemin VU DU CONTENEUR hermes
DEFAULT_HOME = "/opt/data"  # profil default (jobs pre-2.3 en recovery)
INFO = {"version": VERSION, "max_parallel": MAX_PARALLEL,
        "runtimes": [{"id": "hermes", "available": True,
                      "modes": ["read_only", "workspace_write"],
                      "version": "intervention-directe-vps-hermes"}],
        "workspaces": [{"id": "vps-etude", "modes": ["read_only", "workspace_write"],
                        "description": "Intervention directe Hermes sur vps-etude"}]}

HB_INTERVAL = 5
CLAIM_WAIT = 0  # heartbeat/outbox must not wait behind a claim long-poll
OUTBOX_FLUSH_EVERY = 5
PROC_POLL_S = 2
OUTPUT_CHUNK = 16000
RECOVERY_S = 30
SUPERVISOR_FRESH_S = 20
NATIVE_POLL_S = 5
DURABLE_LAUNCH = os.name == "posix"
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


def profile_home_for_slot(slot):
    """HERMES_HOME (vu du conteneur) isole pour un slot, default sinon."""
    if slot and slot in PROFILE_POOL:
        return PROFILE_HOME % slot
    return DEFAULT_HOME


def hermes_argv_for_slot(slot):
    """HERMES_ARGV ancre sur le profil dedie du slot (`-p <slot>`).

    Hors docker (tests FAKE) : argv inchange, l'isolation est portee par
    le slot persiste + les logs/session separes."""
    argv = list(HERMES_ARGV)
    if argv[0] == "docker" and slot and slot in PROFILE_POOL:
        try:
            chat_at = argv.index("chat")
        except ValueError:
            chat_at = len(argv)
        argv[chat_at:chat_at] = ["-p", slot]
    return argv


_PROFILE_OK = {}  # slot -> True (cache process-local, profils quasi-statiques)


def slot_profile_ready(slot):
    """Le profil du slot existe-t-il cote conteneur ?

    Hors docker (tests) : toujours vrai. Verifie via `docker exec ... test -d`
    (le spool hote /srv/hermes/data n'est pas lisible par hermes-ops, un
    isdir() hote serait un faux negatif permanent). Resultat mis en cache ;
    un echec de verification differe le spawn (sens sur : retry, jamais de
    job tue) au lieu de faire echouer le job."""
    if HERMES_ARGV[0] != "docker":
        return True
    if not slot or slot not in PROFILE_POOL:
        return True
    if _PROFILE_OK.get(slot):
        return True
    try:
        r = subprocess.run(["docker", "exec", "hermes", "test", "-d",
                            "/opt/data/profiles/" + slot],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return False
    if r.returncode == 0:
        _PROFILE_OK[slot] = True
        return True
    return False


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
      attempts INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, fencing INTEGER);
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE IF NOT EXISTS slots(
      slot TEXT PRIMARY KEY, job_id TEXT UNIQUE, workspace_id TEXT,
      mode TEXT, updated_at REAL);
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
            cols = {r[1] for r in self.db.execute("PRAGMA table_info(outbox)")}
            if "fencing" not in cols:
                self.db.execute("ALTER TABLE outbox ADD COLUMN fencing INTEGER")
                self.db.execute("UPDATE outbox SET fencing=(SELECT fencing FROM local_jobs WHERE local_jobs.job_id=outbox.job_id)")
            lj = {r[1] for r in self.db.execute("PRAGMA table_info(local_jobs)")}
            # Colonnes en dur (jamais d'entree externe) : semgrep-safe.
            if "workspace_id" not in lj:
                self.db.execute("ALTER TABLE local_jobs ADD COLUMN workspace_id TEXT")
            if "mode" not in lj:
                self.db.execute("ALTER TABLE local_jobs ADD COLUMN mode TEXT")
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
                "pid,proc_started_at,prompt_path,workspace_id,mode,created_at,updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET"
                " fencing=excluded.fencing, epoch=COALESCE(excluded.epoch,epoch),"
                " local_state=excluded.local_state,"
                " session_id=COALESCE(excluded.session_id,session_id),"
                " pid=COALESCE(excluded.pid,pid),"
                " proc_started_at=COALESCE(excluded.proc_started_at,proc_started_at),"
                " prompt_path=COALESCE(excluded.prompt_path,prompt_path),"
                " workspace_id=COALESCE(excluded.workspace_id,workspace_id),"
                " mode=COALESCE(excluded.mode,mode),"
                " updated_at=excluded.updated_at",
                (job_id, fencing, epoch, local_state, kw.get("session_id"),
                 kw.get("pid"), kw.get("proc_started_at"), kw.get("prompt_path"),
                 kw.get("workspace_id"), kw.get("mode"),
                 now, now))
            self.db.commit()

    JOB_COLUMNS = ("fencing", "epoch", "local_state", "session_id", "pid",
                   "proc_started_at", "prompt_path", "workspace_id", "mode")

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
            if kw.get("local_state") in ("done", "abandoned"):
                # Identite liberee SEULEMENT quand le job est reellement
                # termine/abandonne : aucun spawn futur ne reutilisera ce
                # slot par erreur, et un job actif ne le perd jamais.
                self.db.execute("DELETE FROM slots WHERE job_id=?", (job_id,))
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

    def held_jobs(self):
        """Execution OR pending durable publication (including terminal outbox)."""
        with self.lock:
            self.db.row_factory = sqlite3.Row
            rows = self.db.execute(
                "SELECT * FROM local_jobs WHERE local_state!='abandoned' AND "
                "(local_state!='done' OR EXISTS(SELECT 1 FROM outbox WHERE outbox.job_id=local_jobs.job_id))"
            ).fetchall()
            self.db.row_factory = None
            return [dict(r) for r in rows]

    def enqueue(self, job_id, kind, event_id, payload):
        with self.lock:
            self.db.execute(
                "INSERT OR IGNORE INTO outbox(job_id,kind,event_id,payload,created_at,fencing)"
                " VALUES(?,?,?,?,?,(SELECT fencing FROM local_jobs WHERE job_id=?))",
                (job_id, kind, event_id, json.dumps(payload), time.time(), job_id))
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

    def outbox_payload(self, seq, payload):
        with self.lock:
            self.db.execute("UPDATE outbox SET payload=? WHERE seq=?", (json.dumps(payload), seq))
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

    # --- Isolation multi-instance (2.3) : pool de profils + admission ---
    TERMINAL_LOCAL = ("done", "abandoned")

    def get_slot(self, job_id):
        with self.lock:
            r = self.db.execute("SELECT slot FROM slots WHERE job_id=?",
                                (job_id,)).fetchone()
            return r[0] if r else None

    def slot_map(self):
        """Affectations actuelles {job_id: slot}, lignes orphelines purgees."""
        with self.lock:
            act = {j["job_id"] for j in self._active_locked()}
            rows = self.db.execute("SELECT slot, job_id FROM slots").fetchall()
            out = {}
            for slot, jid in rows:
                if jid in act:
                    out[jid] = slot
                else:
                    self.db.execute("DELETE FROM slots WHERE slot=?", (slot,))
            self.db.commit()
            return out

    def _active_locked(self):
        self.db.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in self.db.execute(
                "SELECT * FROM local_jobs WHERE local_state NOT IN "
                "('done','abandoned') ORDER BY created_at").fetchall()]
        finally:
            self.db.row_factory = None

    def free_slots(self):
        """Slots du pool non tenus par un job actif (liste, [] = sature)."""
        with self.lock:
            act = {j["job_id"] for j in self._active_locked()}
            used = {row[0] for row in
                    self.db.execute("SELECT slot, job_id FROM slots").fetchall()
                    if row[1] in act}
            return [s for s in PROFILE_POOL if s not in used]

    def alloc_slot(self, job_id, workspace_id=None, mode=None):
        """Affecte un slot libre au job (idempotent, persiste). None si plein."""
        with self.lock:
            r = self.db.execute("SELECT slot FROM slots WHERE job_id=?",
                                (job_id,)).fetchone()
            if r:
                return r[0]
            act = {j["job_id"] for j in self._active_locked()}
            used = set()
            for (slot, jid) in self.db.execute("SELECT slot, job_id FROM slots").fetchall():
                if jid in act:
                    used.add(slot)
                else:
                    self.db.execute("DELETE FROM slots WHERE slot=?", (slot,))
            for slot in PROFILE_POOL:
                if slot not in used:
                    self.db.execute(
                        "INSERT INTO slots(slot,job_id,workspace_id,mode,updated_at)"
                        " VALUES(?,?,?,?,?)",
                        (slot, job_id, workspace_id, mode, time.time()))
                    self.db.commit()
                    return slot
            return None

    @staticmethod
    def is_write(job):
        return (job.get("mode") or "read_only") == "workspace_write"

    def gate(self, job_id):
        """Admissible au spawn ? (verrou workspace_write + slot disponible).

        - `workspace_write` sur un meme workspace : serialise (premier
          reclame premier servi, les suivants attendent en `claimed`, lease
          renouvele par heartbeat, jamais de double ecriture simultanee).
        - `read_only` : toujours concurrent (si slot libre).
        - Le job qui detient deja un slot reste admissible (recovery).
        Retourne (admissible: bool, motif: str)."""
        job = self.get_job(job_id)
        if not job or job["local_state"] in self.TERMINAL_LOCAL:
            return False, "terminal"
        if self.get_slot(job_id):
            return True, "slot-conserve"
        if self.is_write(job):
            ws = job.get("workspace_id") or ""
            with self.lock:
                act = self._active_locked()
            rivals = [j for j in act
                      if j["job_id"] != job_id and self.is_write(j)
                      and (j.get("workspace_id") or "") == ws]
            if rivals:
                first = min(rivals + [job],
                            key=lambda j: (j.get("created_at") or 0, j["job_id"]))
                if first["job_id"] != job_id:
                    return False, "workspace_write-verrouille-par-%s" % first["job_id"][:8]
        if not self.free_slots():
            return False, "pool-sature"
        return True, "ok"


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
        with urllib.request.urlopen(req, timeout=5) as r:  # nosemgrep
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
    if not isinstance(pid, int) or pid <= 0:
        return False
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
        self.last_tick = time.monotonic()
        self.snapshot = {}
        self.last_native = 0
        self.native_ended = False
        self.exit_code = None

    def _source(self):
        return "orch-" + self.job_id

    def _native(self, st, force=False):
        if HERMES_ARGV[0] != "docker" or (not force and time.monotonic() - self.last_native < NATIVE_POLL_S):
            return
        self.last_native = time.monotonic()
        key = "native:" + self.job_id
        cursor = int(st.get_meta(key) or 0)
        try:
            snap = runtime.native_snapshot(
                st.get_job(self.job_id), self._source(), cursor,
                hermes_home=profile_home_for_slot(st.get_slot(self.job_id)))
        except (OSError, ValueError, subprocess.TimeoutExpired):
            return  # absence of native evidence is not evidence of progress
        if not snap:
            return
        st.set_job(self.job_id, session_id=snap["session_id"])
        self.native_ended = snap.get("ended_at") is not None
        for message in snap["messages"]:
            mid = message["id"]
            role = message["role"]
            tool = re.sub(r"[^A-Za-z0-9_.-]", "", message.get("tool") or "")[:80]
            payload = {"runtime_session_id": snap["session_id"],
                       "activity": "Hermes %s%s (message %s)" % (role, ": " + tool if tool else "", mid),
                       **self.snapshot}
            if message.get("text"):
                payload["output"] = message["text"] + "\n"
                st.set_meta("summary:" + self.job_id, message["text"][-2000:])
            if tool:
                payload["tool"] = tool
            st.enqueue(self.job_id, "event", "native-%s-%s" % (self.job_id, mid), payload)
            st.set_meta(key, str(mid))  # enqueue before cursor: replay is idempotent

    def _observe(self, st, pid, popen=None):
        """Snapshot from the supervised runtime, never a fresh blind PID ping."""
        job = st.get_job(self.job_id)
        if HERMES_ARGV[0] != "docker":
            alive = popen.poll() is None if popen is not None else proc_alive(pid)
            self.snapshot = {"pid": pid, "proc_alive": alive,
                             "proc_started_at": job.get("proc_started_at"),
                             "child_procs": runtime.process_children(pid) if os.name == "posix" else None}
        else:
            key = "exec:" + self.job_id
            eid = st.get_meta(key)
            try:
                ex = runtime.docker_get("/exec/" + eid + "/json") if eid else runtime.find_exec(job, self._source())
                if ex:
                    st.set_meta(key, ex["ID"])
                    actual_pid = ex.get("Pid")
                    if ex.get("Running"):
                        info = runtime.process_info(actual_pid)
                        if info:
                            st.set_meta("runtime:" + self.job_id, json.dumps(info))
                        self.snapshot = {"pid": actual_pid, "proc_alive": bool(info and info["alive"]),
                                         "proc_started_at": info["started_at"] if info else None,
                                         "child_procs": runtime.process_children(actual_pid)}
                    else:
                        self.exit_code = ex.get("ExitCode")
                        self.snapshot = {**self.snapshot, "proc_alive": False}
                else:
                    saved = json.loads(st.get_meta("runtime:" + self.job_id) or "null")
                    info = runtime.process_info(saved["pid"]) if saved else None
                    alive = bool(info and info["identity"] == saved["identity"] and info["alive"])
                    # During startup the CLI may not have created its exec yet.
                    if not saved and proc_alive(pid):
                        alive = None
                    self.snapshot = {"pid": saved["pid"] if saved else None, "proc_alive": alive,
                                     "child_procs": runtime.process_children(saved["pid"]) if alive else None}
            except (OSError, ValueError, http.client.HTTPException):
                self.snapshot = {**self.snapshot, "proc_alive": None}
        self.last_tick = time.monotonic()
        return self.snapshot.get("proc_alive")

    def run(self):
        try:
            self._run()
        except Exception as exc:
            LOG("supervision interrompue:", self.job_id, type(exc).__name__)
            job = self.poller.store.get_job(self.job_id)
            if job and job["local_state"] not in ("done", "abandoned"):
                self.poller.store.set_job(self.job_id, local_state="recovery")

    def _run(self):
        st = self.poller.store
        job = st.get_job(self.job_id)
        if not job or job["local_state"] in ("done", "abandoned"):
            return
        if job["local_state"] == "claimed" and not job.get("pid"):
            self._spawn(st)
            return
        # Recover durable launcher metadata before deciding whether anything died.
        receipt = self._receipt(st)
        if receipt.get("pid"):
            st.set_job(self.job_id, pid=receipt["pid"])
            job = st.get_job(self.job_id)
        alive = self._observe(st, job.get("pid"))
        self._native(st, force=True)
        if alive is True or (alive is None and proc_alive(job.get("pid"))):
            st.set_job(self.job_id, local_state="running")
            self._follow_proc(job["pid"], job.get("session_id"), st)
            return
        # Session ID alone never authorizes replaying a possibly completed action.
        # Recovery bornee (986394b) : sans preuve positive, sortie terminale
        # explicite, jamais de relance aveugle. La reprise --resume reste
        # possible uniquement dans un run supervise actif (relance bornee
        # appel-outil-en-texte, meme slot/profil).
        self._finish(st, job.get("pid"), job.get("session_id"), receipt.get("exit_code"))

    def _receipt(self, st):
        path = st.get_meta("receipt:" + self.job_id)
        try:
            return json.loads(Path(path).read_text()) if path else {}
        except (OSError, ValueError):
            return {}

    def _spawn(self, st, resume=None, prompt_text=None):
        job = st.get_job(self.job_id)
        # Porte d'admission 2.3 : verrou workspace_write + slot du pool.
        # Le slot conserve (recovery/resume) passe toujours ; sinon gate puis
        # allocation persistee. Job non admissible -> reste `claimed`, lease
        # renouvele par heartbeat, la boucle du poller reessaie. Jamais de
        # spawn partage : un Hermes = un profil = un job.
        slot = st.get_slot(self.job_id)
        if not slot:
            ok, why = st.gate(self.job_id)
            if not ok:
                LOG("spawn differe:", self.job_id[:8], why)
                return
            slot = st.alloc_slot(self.job_id, job.get("workspace_id"),
                                 job.get("mode"))
            if not slot:
                LOG("spawn differe:", self.job_id[:8], "pool-sature")
                return
        if not slot_profile_ready(slot):
            LOG("spawn differe:", self.job_id[:8], "profil manquant:", slot)
            return
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
        # Passer via stdin (--query-file -) : pas de montage necessaire.
        # Identite isolee 2.3 : le profil dedie du slot (`-p`), donc un
        # HERMES_HOME (sessions/memoires) jamais partage entre jobs.
        argv = hermes_argv_for_slot(slot)
        if argv[0] == "docker":
            argv += ["--source", self._source()]
        if resume:
            argv += ["--resume", resume]
        p = None
        try:
            # Persist a unique launch intent BEFORE process creation. Restart in
            # this window reconciles the receipt; it never issues another spawn.
            generation = int(st.get_meta("launch:" + self.job_id) or 0) + 1
            st.set_meta("launch:" + self.job_id, str(generation))
            stem = os.path.join(LOG_DIR, "%s-%s-%s" % (self.job_id, job["fencing"], generation))
            input_path = stem + ".input"
            atomic_write(input_path, prompt_text)
            if DURABLE_LAUNCH:
                receipt_path = stem + ".receipt.json"
                descriptor = stem + ".launch.json"
                runtime.atomic_json(descriptor, {"argv": argv, "prompt": input_path,
                                                "log": log_path, "receipt": receipt_path})
                st.set_meta("receipt:" + self.job_id, receipt_path)
                launcher = subprocess.Popen([sys.executable, runtime.__file__, descriptor],
                                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                            stderr=subprocess.DEVNULL, start_new_session=True)
                st.set_meta("launcher:" + self.job_id, str(launcher.pid))
                until = time.monotonic() + 10
                while time.monotonic() < until:
                    receipt = self._receipt(st)
                    if receipt.get("pid"):
                        pid = receipt["pid"]
                        break
                    if receipt.get("error") or launcher.poll() is not None:
                        raise OSError("launcher failed")
                    self.last_tick = time.monotonic()
                    time.sleep(0.05)
                else:
                    raise OSError("launcher receipt timeout")
                # Keep the handle for reaping; receipt survives a poller restart.
                self.launcher = launcher
            else:
                with open(input_path, "rb") as pf, open(log_path, "ab") as lf:
                    p = subprocess.Popen(argv, stdin=pf, stdout=lf,
                                         stderr=subprocess.STDOUT, start_new_session=True)
                pid = p.pid
        except OSError as e:
            st.enqueue(self.job_id, "transition", "tr-%s-failed" % self.job_id,
                       {"src": "running" if resume else "starting", "to": "failed",
                        "error": "spawn impossible: %s" % type(e).__name__})
            st.set_job(self.job_id, local_state="done")
            return
        started = time.time()
        st.set_job(self.job_id, pid=pid, proc_started_at=started,
                   local_state="running")
        st.set_meta("client_identity:" + self.job_id, (runtime.process_info(pid) or {}).get("identity", ""))
        # Telemetrie A PLAT : le broker ne lit que pid/proc_alive/... au
        # premier niveau (v2.0 l'imbriquait sous "telemetry" -> ignoree).
        st.enqueue(self.job_id, "transition", "tr-%s-running" % self.job_id,
                   {"src": "starting", "to": "running", "pid": pid,
                    "proc_alive": True, "proc_started_at": started})
        self._follow_proc(pid, resume, st, popen=p)

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
        unknown_since = None
        while True:
            if self.stop_ev.is_set():
                self._stop_process(st, pid, popen)
                return  # stale fencing: stop actual runtime, publish nothing
            alive = self._observe(st, pid, popen)
            self._native(st)
            sid = st.get_job(self.job_id).get("session_id") or sid
            if alive is False:
                break
            if alive is None:
                unknown_since = unknown_since or time.monotonic()
                if time.monotonic() - unknown_since >= RECOVERY_S:
                    self._finish(st, pid, sid, None)
                    return
            else:
                unknown_since = None
            sid, _ = self._pump(st, pid, sid, True)
            if self.cancel_ev.is_set():
                stopped = self._stop_process(st, pid, popen)
                sid = self._drain(st, pid, sid)
                st.enqueue(self.job_id, "transition",
                           "tr-%s-cancelled" % self.job_id,
                           {"src": "running", "to": "cancelled" if stopped else "lost",
                            "runtime_session_id": sid, "pid": pid,
                            "proc_alive": False if stopped else None,
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
        else:
            # Give the durable launcher a bounded opportunity to fsync/reap.
            until = time.monotonic() + 2
            while time.monotonic() < until:
                receipt = self._receipt(st)
                if receipt.get("finished_at"):
                    exit_code = receipt.get("exit_code")
                    break
                time.sleep(0.05)
            launcher = getattr(self, "launcher", None)
            if launcher:
                launcher.poll()
        if self.exit_code is not None:
            exit_code = self.exit_code
        self._finish(st, pid, sid, exit_code)

    def _stop_process(self, st, pid, popen=None):
        """Stop only a positively identified process group, including Docker's
        host runtime PID (killing the docker client alone leaves Hermes alive)."""
        saved = json.loads(st.get_meta("runtime:" + self.job_id) or "null")
        target = saved["pid"] if saved else pid
        identity = saved["identity"] if saved else st.get_meta("client_identity:" + self.job_id)
        current = runtime.process_info(target)
        if current and identity and current["identity"] == identity:
            try:
                if os.getpgid(target) != target:
                    return False
                os.killpg(target, signal.SIGTERM)
                for _ in range(10):
                    if not proc_alive(target):
                        break
                    self.last_tick = time.monotonic()
                    time.sleep(0.5)
                if proc_alive(target):
                    os.killpg(target, signal.SIGKILL)
            except OSError:
                pass
        elif proc_alive(target):
            return False  # never signal a reused/unverified PID
        if popen is not None:
            try:
                popen.wait(timeout=5)
            except subprocess.TimeoutExpired:
                return False
        return not proc_alive(target)

    def _finish(self, st, pid, sid, exit_code):
        # Drain bounded native pages before the terminal, including legacy quiet
        # jobs whose client died while the container kept doing useful work.
        for _ in range(20):
            before = st.get_meta("native:" + self.job_id)
            self._native(st, force=True)
            if st.get_meta("native:" + self.job_id) == before:
                break
        sid = st.get_job(self.job_id).get("session_id") or sid
        sid = self._drain(st, pid, sid)
        summary = self._summary() or st.get_meta("summary:" + self.job_id) or ""
        tele = {"runtime_session_id": sid, "pid": self.snapshot.get("pid", pid),
                "proc_alive": self.snapshot.get("proc_alive", False) if exit_code is None else False}
        if exit_code is None:
            job = st.get_job(self.job_id)
            src = "starting" if job["local_state"] == "starting" else "running"
            st.enqueue(self.job_id, "transition", "tr-%s-lost" % self.job_id,
                       {"src": src, "to": "lost", "result_summary": summary or None,
                        "error": "processus orphelin : sortie recuperee si disponible, code de sortie inconnu; aucune relance automatique",
                        **tele})
            st.set_job(self.job_id, local_state="done", session_id=sid)
            return
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
        for j in self.store.held_jobs():
            sup = getattr(self, "supervisors", {}).get(j["job_id"])
            item = {"job_id": j["job_id"], "fencing": j["fencing"]}
            if j["local_state"] == "done":
                item.update(proc_alive=False, supervisor_alive=True)  # bounded terminal delivery
            elif sup:
                item.update(sup.snapshot)
                item["supervisor_alive"] = sup.is_alive() and time.monotonic() - sup.last_tick < SUPERVISOR_FRESH_S
            elif j.get("pid") and not proc_alive(j["pid"]):
                item.update(pid=j["pid"], proc_alive=False)
            # A live docker client on boot proves neither runtime liveness nor
            # runtime death. The supervisor resolves the engine identity next.
            out.append(item)
        return out

    def held_payload(self):
        """held du heartbeat avec telemetrie a plat quand le pid est connu."""
        return self.held()

    def write_health(self):
        active = self.store.active_jobs()
        try:
            gated = []
            for j in active:
                if not j.get("pid"):
                    ok, why = self.store.gate(j["job_id"])
                    if not ok:
                        gated.append({"job_id": j["job_id"], "motif": why})
            atomic_write(HEALTH_FILE, json.dumps({
                "runner": RUNNER_ID, "version": VERSION, "epoch": self.epoch,
                "held": [j["job_id"] for j in active],
                "local_active_job": active[0]["job_id"] if active else None,
                "local_session": (active[0].get("session_id")
                                  if active else None),
                "slots": self.store.slot_map(),
                "gated": gated,
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
        purged = self.store.slot_map()  # purge les affectations orphelines
        LOG("slots repris:", purged)
        held = self.held()
        r = self.broker.hello(INFO, held)
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
        job = self.store.get_job(job_id)
        if not job or job["local_state"] in ("done", "abandoned"):
            return
        # A terminal may have been queued immediately before a crash, before
        # local_state was committed. Publication owns this job now.
        with self.store.lock:
            pending = self.store.db.execute(
                "SELECT payload FROM outbox WHERE job_id=? AND kind='transition'", (job_id,)
            ).fetchall()
        if any(json.loads(r[0]).get("to") in ("completed", "failed", "cancelled", "lost", "timeout") for r in pending):
            self.store.set_job(job_id, local_state="done")
            return
        # Porte d'admission 2.3 : pas de thread/spawn tant que le job n'est
        # pas admissible (verrou workspace_write ou pool sature). Le job
        # reste `claimed` dans held -> lease renouvele, reprise au prochain
        # tour. Les jobs en recovery (slot conserve) passent toujours.
        ok, _why = self.store.gate(job_id)
        if not ok:
            return
        s = Supervisor(self, job_id)
        self.supervisors[job_id] = s
        s.start()

    def flush_outbox(self):
        deadline = time.monotonic() + 2
        for item in self.store.outbox_peek(100):
            if time.monotonic() >= deadline:
                break
            job = self.store.get_job(item["job_id"])
            if not job or job.get("local_state") == "abandoned":
                self.store.outbox_del(item["seq"])
                continue
            fencing = item["fencing"]
            if fencing != job["fencing"]:
                self.store.outbox_del(item["seq"])
                continue
            payload = json.loads(item["payload"])
            try:
                if item["kind"] == "event":
                    result = self.broker.event(self.epoch, item["job_id"], fencing,
                                               item["event_id"], **payload)
                    if result.get("ignored"):
                        self._abandon(item["job_id"])
                else:
                    rest = {k: v for k, v in payload.items()
                            if k not in ("src", "to")}
                    result = self.broker.transition(self.epoch, item["job_id"], fencing,
                                                    payload.get("src", "claimed"), payload["to"], **rest)
                    if result.get("state") != payload["to"]:
                        self._abandon(item["job_id"])
                self.store.outbox_del(item["seq"])
                self.last_broker_ok = time.time()
            except urllib.error.HTTPError as e:
                body = ""
                try:
                    body = e.read().decode()[:200]
                except Exception:
                    pass
                if e.code == 409 and "superseded" in body:
                    raise SystemExit("runner session superseded")
                if e.code == 409 and "state_conflict" in body:
                    state = self.broker.post("job-state", {"epoch": self.epoch, "job_id": item["job_id"],
                                                           "fencing": fencing})["state"]
                    if state == payload.get("to") or (payload.get("to") == "starting" and state == "running"):
                        self.store.outbox_del(item["seq"])
                    elif state in ("claimed", "starting", "running") and payload.get("to") in ("failed", "lost", "cancelled"):
                        payload["src"] = state
                        if state == "claimed" and payload["to"] == "lost":
                            payload["to"] = "failed"
                        self.store.outbox_payload(item["seq"], payload)
                        break  # preserve per-job event ordering on retry
                    else:
                        self._abandon(item["job_id"])
                        self.store.outbox_del(item["seq"])
                    continue
                if e.code in (409, 404) and any(
                        k in body for k in ("stale_fencing",
                                            "unknown_job",
                                            "invalid_transition")):
                    LOG("fencing refuse, abandon local:", item["job_id"],
                        body[:80])
                    self._abandon(item["job_id"])
                    self.store.outbox_del(item["seq"])
                else:
                    self.store.outbox_bump(item["seq"])
                    raise
            except Exception:
                self.store.outbox_bump(item["seq"])
                raise

    def _abandon(self, job_id):
        self.store.set_job(job_id, local_state="abandoned")
        sup = self.supervisors.get(job_id)
        if sup:
            sup.stop_ev.set()

    def loop(self):
        self.boot()
        last_flush = 0
        while True:
            try:
                for j in self.store.active_jobs():
                    self.ensure_supervisor(j["job_id"])
                # Publication is independent of heartbeat acceptance, including
                # terminals left behind by a previous poller process.
                self.flush_outbox()
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
                free_slots = min(MAX_PARALLEL - len(self.store.active_jobs()),
                                 len(self.store.free_slots()))
                if free_slots > 0:
                    for job in self.broker.claim(
                            self.epoch, free_slots).get("jobs", []):
                        jid = job["job_id"]
                        prompt_path = self.store.store_prompt(
                            jid, job.get("prompt") or "")
                        self.store.upsert_job(jid, job["fencing"], self.epoch,
                                              "claimed", prompt_path=prompt_path,
                                              workspace_id=job.get("workspace_id"),
                                              mode=job.get("mode") or "read_only")
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
                    self.epoch = self.broker.hello(INFO, self.held_payload())["epoch"]
                    atomic_write(EPOCH_FILE, str(self.epoch))
                    self.store.set_meta("epoch", str(self.epoch))
                    self.last_broker_ok = time.time()
                except Exception as e2:
                    LOG("re-hello echec:", type(e2).__name__)


if __name__ == "__main__":
    import fcntl
    os.makedirs(SPOOL, exist_ok=True)
    with open(os.path.join(SPOOL, ".poller.lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        Poller().loop()
