"""Broker durable : SQLite, source d'autorité de l'état des jobs.

Garanties :
- chaque transition est un compare-and-set (état source + fencing token + runner +
  epoch de session) exécuté dans une transaction IMMEDIATE ;
- un job n'est jamais réassigné après `starting` : bail expiré => `lost` ;
- un runner redémarré (nouvel epoch) ne peut plus écrire pour l'ancienne session ;
- les événements sont dédupliqués par (job_id, event_id) ;
- la sortie est bornée par job ; les prompts et sorties sont purgés après rétention.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import orch_protocol as P
from orch_protocol.redact import redact

log = logging.getLogger("orch.store")

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS runners (
  id TEXT PRIMARY KEY,
  epoch INTEGER NOT NULL DEFAULT 0,
  last_seen REAL,
  hello_at REAL,
  version TEXT,
  max_parallel INTEGER NOT NULL DEFAULT 1,
  runtimes_json TEXT NOT NULL DEFAULT '[]',
  workspaces_json TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  runner_id TEXT NOT NULL,
  runtime TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  mode TEXT NOT NULL,
  prompt TEXT,
  prompt_chars INTEGER NOT NULL,
  idem_key TEXT UNIQUE,
  idem_hash TEXT,
  state TEXT NOT NULL,
  fencing INTEGER NOT NULL DEFAULT 0,
  attempt INTEGER NOT NULL DEFAULT 0,
  epoch INTEGER,
  lease_expires REAL,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  timeout_s INTEGER NOT NULL,
  created_at REAL NOT NULL,
  claimed_at REAL,
  started_at REAL,
  finished_at REAL,
  updated_at REAL NOT NULL,
  exit_code INTEGER,
  last_activity TEXT,
  result_summary TEXT,
  error TEXT,
  runtime_session_id TEXT,
  output_chars INTEGER NOT NULL DEFAULT 0,
  output_chunks INTEGER NOT NULL DEFAULT 0,
  output_truncated INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state, runner_id, created_at);
CREATE TABLE IF NOT EXISTS events (
  job_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  received_at REAL NOT NULL,
  PRIMARY KEY (job_id, event_id)
);
CREATE TABLE IF NOT EXISTS output (
  job_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  text TEXT NOT NULL,
  PRIMARY KEY (job_id, seq)
);
CREATE TABLE IF NOT EXISTS transitions (
  job_id TEXT NOT NULL,
  at REAL NOT NULL,
  src TEXT,
  dst TEXT NOT NULL,
  actor TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS jobs_terminal_immutable
BEFORE UPDATE OF state ON jobs
WHEN OLD.state IN ('completed','failed','timeout','cancelled','lost') AND NEW.state != OLD.state
BEGIN SELECT RAISE(ABORT, 'terminal state is immutable'); END;
-- Supervision riche : journal structuré borné + missions. Créées ici pour les
-- bases neuves ; les bases existantes sont migrées par _migrate() (ADD COLUMN
-- idempotent, nouvelles tables IF NOT EXISTS : rollback = redéployer l'ancien src).
CREATE TABLE IF NOT EXISTS job_events (
  job_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  at REAL NOT NULL,
  kind TEXT NOT NULL,
  detail TEXT,
  PRIMARY KEY (job_id, seq)
);
CREATE INDEX IF NOT EXISTS job_events_job ON job_events(job_id, seq);
CREATE TABLE IF NOT EXISTS runner_env (
  runner_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  branch TEXT,
  head TEXT,
  dirty INTEGER,
  at REAL NOT NULL,
  PRIMARY KEY (runner_id, workspace_id)
);
CREATE TABLE IF NOT EXISTS missions (
  id TEXT PRIMARY KEY,
  objective TEXT NOT NULL,
  acceptance_json TEXT NOT NULL DEFAULT '[]',
  max_attempts INTEGER NOT NULL DEFAULT 2,
  attempts INTEGER NOT NULL DEFAULT 0,
  current_job_id TEXT,
  state TEXT NOT NULL DEFAULT 'executing',
  validation_state TEXT,
  validation_note TEXT,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS mission_attempts (
  mission_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  job_id TEXT NOT NULL,
  at REAL NOT NULL,
  PRIMARY KEY (mission_id, attempt_no)
);
"""

MAX_CLAIM_ATTEMPTS = 3


class BrokerError(Exception):
    """Refus métier ; `code` est stable et renvoyé tel quel au client."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code
        self.message = message or code


@dataclass
class Retention:
    prompt_s: int = 7 * 86_400
    output_s: int = 7 * 86_400
    meta_s: int = 90 * 86_400


class Store:
    def __init__(self, path: str | Path, clock: Callable[[], float] = time.time) -> None:
        self.path = str(path)
        self.clock = clock
        self._lock = threading.RLock()
        self._db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None, timeout=30)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.executescript(SCHEMA)
        self._migrate()

    # ------------------------------------------------------------------ utils
    def close(self) -> None:
        with self._lock:
            self._db.close()

    def _tx(self):
        store = self

        class _Tx:
            def __enter__(self_inner):
                store._lock.acquire()
                store._db.execute("BEGIN IMMEDIATE")
                return store._db

            def __exit__(self_inner, exc_type, exc, tb):
                try:
                    if exc_type is None:
                        store._db.execute("COMMIT")
                    else:
                        store._db.execute("ROLLBACK")
                finally:
                    store._lock.release()
                return False

        return _Tx()

    def _log_transition(self, db, job_id: str, src: str | None, dst: str, actor: str) -> None:
        db.execute(
            "INSERT INTO transitions(job_id, at, src, dst, actor) VALUES (?,?,?,?,?)",
            (job_id, self.clock(), src, dst, actor),
        )
        log.info("job_transition job_id=%s %s->%s actor=%s", job_id, src, dst, actor)

    def _migrate(self) -> None:
        """Migration idempotente des bases v1 : nouvelles colonnes NULL (= inconnu)
        et nouvelles tables. Sûre à rejouer ; l'ancien code ignore ces ajouts."""
        with self._lock:
            cols = {r["name"] for r in self._db.execute("PRAGMA table_info(jobs)").fetchall()}
            # DDL littéraux (pas d'interpolation : exigence semgrep) ; chaque
            # colonne n'est ajoutée que si absente.
            if "last_output_at" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN last_output_at REAL")
            if "last_event_at" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN last_event_at REAL")
            if "telemetry_at" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN telemetry_at REAL")
            if "proc_pid" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN proc_pid INTEGER")
            if "proc_started_at" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN proc_started_at REAL")
            if "proc_alive" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN proc_alive INTEGER")
            if "child_procs" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN child_procs INTEGER")
            if "current_tool" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN current_tool TEXT")
            if "stall_suspect_at" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN stall_suspect_at REAL")
            if "stall_at" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN stall_at REAL")
            if "recovery_state" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN recovery_state TEXT")
            if "recovery_detail" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN recovery_detail TEXT")
            if "resume_count" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN resume_count INTEGER NOT NULL DEFAULT 0")
            log.info("migrated jobs columns ok")

    def _emit(self, db, job_id: str, kind: str, detail: str | None = None) -> int:
        """Journal structuré borné : événement ordonné persistant (seq par job).
        `detail` est redacted + borné. Les plus vieux au-delà de MAX_EVENTS_PER_JOB
        sont supprimés (pas de transcript complet). Retourne le seq."""
        now = self.clock()
        row = db.execute("SELECT COALESCE(MAX(seq), -1) AS m FROM job_events WHERE job_id=?", (job_id,)).fetchone()
        seq = int(row["m"]) + 1
        clean = P.clip(redact(detail), 500) if detail else None
        db.execute(
            "INSERT INTO job_events(job_id, seq, at, kind, detail) VALUES (?,?,?,?,?)",
            (job_id, seq, now, kind, clean),
        )
        db.execute(
            "DELETE FROM job_events WHERE job_id=? AND seq <= (SELECT COALESCE(MAX(seq), -1) - ? FROM job_events WHERE job_id=?)",
            (job_id, P.MAX_EVENTS_PER_JOB, job_id),
        )
        return seq

    def _event_seq(self, db, job_id: str) -> int:
        row = db.execute("SELECT COALESCE(MAX(seq), -1) AS m FROM job_events WHERE job_id=?", (job_id,)).fetchone()
        return int(row["m"])

    # ---------------------------------------------------------------- runners
    def _runner_epoch_ok(self, db, runner_id: str, epoch: int) -> None:
        row = db.execute("SELECT epoch FROM runners WHERE id=?", (runner_id,)).fetchone()
        if row is None:
            raise BrokerError("unknown_runner", "runner non enregistré (hello requis)")
        if int(row["epoch"]) != int(epoch):
            raise BrokerError("superseded", "session runner remplacée par une connexion plus récente")

    def hello(self, runner_id: str, info: dict[str, Any], held: list[dict[str, Any]]) -> int:
        """Ouvre une nouvelle session runner et réconcilie les jobs qu'il détenait.

        `held` = jobs que le processus runner gère réellement en ce moment, PLUS
        les jobs de son journal local de reprise (`recovering:true`, même fencing)
        après un redémarrage / une coupure. Tout job actif du broker absent de `held` :
        - claimed  -> queued (rien n'a été lancé, relance sûre) ;
        - starting/running jamais parqués -> `suspended` (bail prolongé de
          RECOVERY_GRACE_S, jamais relancé en silence) ; déjà parqués et toujours
          absents -> on conserve le parking sans prolonger (grâce bornée) ;
        - grâce expirée (bail dépassé) -> `lost`.
        Les jobs présents dans `held` restent attachés à la nouvelle session ;
        `recovering:true` les marque `recovering` (reprise en cours).
        """
        now = self.clock()
        runtimes = [r for r in info.get("runtimes", []) if isinstance(r, dict) and r.get("id") in P.RUNTIMES]
        workspaces = [w for w in info.get("workspaces", []) if isinstance(w, dict) and P.valid_id(w.get("id"))]
        max_parallel = max(1, min(int(info.get("max_parallel", 1)), 16))
        held_map: dict[str, int] = {}
        held_flags: dict[str, dict[str, bool]] = {}
        for h in held:
            if not isinstance(h, dict):
                continue
            try:
                held_map[h.get("job_id")] = int(h.get("fencing", -1))
            except (TypeError, ValueError):
                continue
            held_flags[h.get("job_id")] = {
                "recovering": bool(h.get("recovering")),
                "suspended": bool(h.get("suspended")),
            }
        with self._tx() as db:
            row = db.execute("SELECT epoch FROM runners WHERE id=?", (runner_id,)).fetchone()
            epoch = (int(row["epoch"]) if row else 0) + 1
            db.execute(
                """INSERT INTO runners(id, epoch, last_seen, hello_at, version, max_parallel, runtimes_json, workspaces_json)
                   VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET epoch=excluded.epoch, last_seen=excluded.last_seen,
                     hello_at=excluded.hello_at, version=excluded.version, max_parallel=excluded.max_parallel,
                     runtimes_json=excluded.runtimes_json, workspaces_json=excluded.workspaces_json""",
                (
                    runner_id,
                    epoch,
                    now,
                    now,
                    str(info.get("version", ""))[:40],
                    max_parallel,
                    json.dumps(runtimes)[:20_000],
                    json.dumps(workspaces)[:20_000],
                ),
            )
            active = db.execute(
                "SELECT id, state, fencing, recovery_state, lease_expires FROM jobs WHERE runner_id=? AND state IN ('claimed','starting','running')",
                (runner_id,),
            ).fetchall()
            for job in active:
                if held_map.get(job["id"]) == int(job["fencing"]):
                    flags = held_flags.get(job["id"], {})
                    if flags.get("recovering") or flags.get("suspended"):
                        rec = P.RECOVERING if flags.get("recovering") else P.SUSPENDED
                        db.execute(
                            "UPDATE jobs SET epoch=?, lease_expires=?, updated_at=?, recovery_state=?, resume_count=resume_count+? WHERE id=?",
                            (epoch, now + P.LEASE_S, now, rec, 1 if rec == P.RECOVERING else 0, job["id"]),
                        )
                        self._emit(db, job["id"], P.EV_RUNNER_RECOVERING, f"runner reconnecté (epoch {epoch}) : reprise {rec}")
                    else:
                        db.execute(
                            "UPDATE jobs SET epoch=?, lease_expires=?, updated_at=?, recovery_state=NULL, recovery_detail=NULL WHERE id=?",
                            (epoch, now + P.LEASE_S, now, job["id"]),
                        )
                    continue
                self._emit(
                    db, job["id"], P.EV_RUNNER_DISCONNECT,
                    f"runner reconnecté (epoch {epoch}) sans détenir ce job",
                )
                if job["state"] == P.CLAIMED:
                    self._requeue(db, job["id"], "runner_restart")
                elif (job["lease_expires"] is not None and float(job["lease_expires"]) < now) and job["recovery_state"] in (P.RECOVERING, P.SUSPENDED):
                    self._set_terminal(
                        db, job["id"], job["state"], P.LOST, "broker",
                        error="grâce de reprise expirée sans rattachement : issue inconnue, non relancé",
                    )
                elif job["recovery_state"] in (P.RECOVERING, P.SUSPENDED):
                    pass  # parking conservé sans prolongation : la grâce reste bornée
                else:
                    self._park_suspended(
                        db, job["id"], job["state"],
                        "runner redémarré ou reconnecté sans ce job : parqué en attente de reprise, non relancé",
                    )
            # Snapshot d'environnement par workspace (git, si le runner l'observe) :
            # cache broker rafraîchi au hello, jamais de secrets (branch/head/dirty seuls).
            for ws_id, snap in (info.get("env") or {}).items():
                if not P.valid_id(ws_id) or not isinstance(snap, dict):
                    continue
                db.execute(
                    """INSERT INTO runner_env(runner_id, workspace_id, branch, head, dirty, at)
                       VALUES (?,?,?,?,?,?)
                       ON CONFLICT(runner_id, workspace_id) DO UPDATE SET
                         branch=excluded.branch, head=excluded.head, dirty=excluded.dirty, at=excluded.at""",
                    (
                        runner_id, ws_id,
                        str(snap.get("branch") or "")[:120] or None,
                        str(snap.get("head") or "")[:40] or None,
                        1 if snap.get("dirty") else 0,
                        now,
                    ),
                )
        log.info("runner_connected runner_id=%s epoch=%s", runner_id, epoch)
        return epoch

    def heartbeat(self, runner_id: str, epoch: int, held: list[dict[str, Any]]) -> dict[str, Any]:
        now = self.clock()
        cancel: list[str] = []
        unknown: list[str] = []
        with self._tx() as db:
            self._runner_epoch_ok(db, runner_id, epoch)
            db.execute("UPDATE runners SET last_seen=? WHERE id=?", (now, runner_id))
            for h in held:
                if not isinstance(h, dict):
                    continue
                job_id = h.get("job_id")
                try:
                    fencing = int(h.get("fencing", -1))
                except (TypeError, ValueError):
                    unknown.append(str(job_id))
                    continue
                row = db.execute(
                    "SELECT state, fencing, cancel_requested, recovery_state FROM jobs WHERE id=? AND runner_id=?",
                    (job_id, runner_id),
                ).fetchone()
                if row is None or int(row["fencing"]) != fencing or row["state"] not in P.ACTIVE:
                    unknown.append(str(job_id))  # le runner doit tuer ce processus / oublier ce parking
                    continue
                if h.get("suspended"):
                    db.execute(
                        "UPDATE jobs SET lease_expires=?, epoch=?, recovery_state=?, recovery_detail=? WHERE id=?",
                        (now + P.LEASE_S, epoch, P.SUSPENDED,
                         P.clip(str(h.get("recovery_detail") or "parqué par le runner : reprise explicite requise"), 500),
                         job_id),
                    )
                elif h.get("recovering"):
                    db.execute(
                        "UPDATE jobs SET lease_expires=?, epoch=?, recovery_state=? WHERE id=?",
                        (now + P.LEASE_S, epoch, P.RECOVERING, job_id),
                    )
                else:
                    db.execute(
                        "UPDATE jobs SET lease_expires=?, epoch=?, recovery_state=NULL, recovery_detail=NULL WHERE id=?",
                        (now + P.LEASE_S, epoch, job_id),
                    )
                if row["cancel_requested"]:
                    cancel.append(str(job_id))
                # Télémétrie d'exécution (optionnelle, runners récents) : état du
                # processus observé localement. Ne touche PAS à updated_at (un ping
                # de présence n'est pas un changement significatif pour job_wait).
                if any(k in h for k in ("pid", "proc_alive", "proc_started_at", "child_procs", "tool")):
                    try:
                        pid = h.get("pid")
                        pid = int(pid) if pid is not None else None
                    except (TypeError, ValueError):
                        pid = None
                    alive = h.get("proc_alive")
                    alive = int(bool(alive)) if alive is not None else None
                    try:
                        pstarted = h.get("proc_started_at")
                        pstarted = float(pstarted) if pstarted is not None else None
                    except (TypeError, ValueError):
                        pstarted = None
                    try:
                        children = h.get("child_procs")
                        children = int(children) if children is not None else None
                        if children is not None and children < 0:
                            children = None  # requête Job Object en échec : inconnu, pas 0
                    except (TypeError, ValueError):
                        children = None
                    db.execute(
                        """UPDATE jobs SET telemetry_at=?, proc_pid=?, proc_started_at=?,
                             proc_alive=?, child_procs=?, current_tool=?
                           WHERE id=?""",
                        (
                            now, pid, pstarted, alive, children,
                            P.clip(redact(str(h["tool"])[:120]), 120) if h.get("tool") else None,
                            job_id,
                        ),
                    )
        return {"cancel": cancel, "abandon": unknown}

    def runners(self) -> list[dict[str, Any]]:
        now = self.clock()
        with self._lock:
            rows = self._db.execute("SELECT * FROM runners ORDER BY id").fetchall()
            out = []
            for r in rows:
                active = self._db.execute(
                    "SELECT COUNT(*) FROM jobs WHERE runner_id=? AND state IN ('claimed','starting','running')",
                    (r["id"],),
                ).fetchone()[0]
                queued = self._db.execute(
                    "SELECT COUNT(*) FROM jobs WHERE runner_id=? AND state='queued'", (r["id"],)
                ).fetchone()[0]
                online = r["last_seen"] is not None and now - float(r["last_seen"]) <= P.ONLINE_WINDOW_S
                out.append(
                    {
                        "id": r["id"],
                        "status": "online" if online else "offline",
                        "last_seen": _iso(r["last_seen"]),
                        "seconds_since_seen": None if r["last_seen"] is None else round(now - r["last_seen"], 1),
                        "runtimes": json.loads(r["runtimes_json"]),
                        "workspaces": json.loads(r["workspaces_json"]),
                        "max_parallel": r["max_parallel"],
                        "active_jobs": active,
                        "queued_jobs": queued,
                        "runner_version": r["version"],
                    }
                )
            return out

    # ------------------------------------------------------------------- jobs
    def create_job(
        self,
        runner_id: str,
        runtime: str,
        workspace_id: str,
        prompt: str,
        mode: str,
        timeout_s: int | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Crée un job `queued`. Retourne (job, created). Refuse avant toute exécution
        si runner, runtime, workspace ou mode sont hors de ce que le runner a annoncé."""
        if not P.valid_id(runner_id):
            raise BrokerError("invalid_runner", "runner_id invalide")
        if runtime not in P.RUNTIMES:
            raise BrokerError("invalid_runtime", f"runtime inconnu ; autorisés : {', '.join(P.RUNTIMES)}")
        if not P.valid_id(workspace_id):
            raise BrokerError("invalid_workspace", "workspace_id invalide (identifiant d'allowlist attendu, pas un chemin)")
        if mode not in P.MODES:
            raise BrokerError("invalid_mode", f"mode inconnu ; autorisés : {', '.join(P.MODES)}")
        if not isinstance(prompt, str) or not prompt.strip():
            raise BrokerError("invalid_prompt", "prompt vide")
        if "\x00" in prompt or len(prompt) > P.MAX_PROMPT_CHARS:
            raise BrokerError("invalid_prompt", f"prompt invalide (NUL ou > {P.MAX_PROMPT_CHARS} caractères)")
        timeout_s = int(timeout_s or P.DEFAULT_TIMEOUT_S)
        if not 30 <= timeout_s <= P.MAX_TIMEOUT_S:
            raise BrokerError("invalid_timeout", f"timeout_s entre 30 et {P.MAX_TIMEOUT_S}")
        if idempotency_key is not None and (not isinstance(idempotency_key, str) or not 8 <= len(idempotency_key) <= 128):
            raise BrokerError("invalid_idempotency_key", "idempotency_key : 8 à 128 caractères")

        req_hash = hashlib.sha256(
            json.dumps([runner_id, runtime, workspace_id, mode, timeout_s, prompt], ensure_ascii=False).encode()
        ).hexdigest()
        now = self.clock()
        with self._tx() as db:
            if idempotency_key:
                existing = db.execute("SELECT * FROM jobs WHERE idem_key=?", (idempotency_key,)).fetchone()
                if existing is not None:
                    if existing["idem_hash"] != req_hash:
                        raise BrokerError("idempotency_conflict", "idempotency_key déjà utilisée pour une autre requête")
                    return self._job_view(existing), False
            runner = db.execute("SELECT * FROM runners WHERE id=?", (runner_id,)).fetchone()
            if runner is None:
                raise BrokerError("unknown_runner", "runner inconnu : il ne s'est jamais connecté")
            rts = {r["id"]: r for r in json.loads(runner["runtimes_json"])}
            if runtime not in rts or not rts[runtime].get("available"):
                raise BrokerError("runtime_unavailable", f"runtime {runtime} non autorisé ou indisponible sur {runner_id}")
            wss = {w["id"]: w for w in json.loads(runner["workspaces_json"])}
            if workspace_id not in wss:
                raise BrokerError("workspace_denied", f"workspace {workspace_id} absent de l'allowlist de {runner_id}")
            if mode not in wss[workspace_id].get("modes", []):
                raise BrokerError("mode_denied", f"mode {mode} non autorisé pour le workspace {workspace_id}")
            job_id = str(uuid.uuid4())
            db.execute(
                """INSERT INTO jobs(id, runner_id, runtime, workspace_id, mode, prompt, prompt_chars, idem_key, idem_hash,
                     state, timeout_s, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (job_id, runner_id, runtime, workspace_id, mode, prompt, len(prompt), idempotency_key, req_hash,
                 P.QUEUED, timeout_s, now, now),
            )
            self._log_transition(db, job_id, None, P.QUEUED, "mcp")
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return self._job_view(row), True

    def claim(self, runner_id: str, epoch: int, free_slots: int) -> list[dict[str, Any]]:
        now = self.clock()
        with self._tx() as db:
            self._runner_epoch_ok(db, runner_id, epoch)
            db.execute("UPDATE runners SET last_seen=? WHERE id=?", (now, runner_id))
            runner = db.execute("SELECT max_parallel FROM runners WHERE id=?", (runner_id,)).fetchone()
            active = db.execute(
                "SELECT COUNT(*) FROM jobs WHERE runner_id=? AND state IN ('claimed','starting','running')",
                (runner_id,),
            ).fetchone()[0]
            n = min(max(0, int(free_slots)), int(runner["max_parallel"]) - int(active))
            if n <= 0:
                return []
            rows = db.execute(
                "SELECT * FROM jobs WHERE runner_id=? AND state='queued' ORDER BY created_at LIMIT ?",
                (runner_id, n),
            ).fetchall()
            claimed = []
            for row in rows:
                cur = db.execute(
                    """UPDATE jobs SET state='claimed', fencing=fencing+1, attempt=attempt+1, epoch=?,
                         lease_expires=?, claimed_at=?, updated_at=?
                       WHERE id=? AND state='queued'""",
                    (epoch, now + P.LEASE_S, now, now, row["id"]),
                )
                if cur.rowcount != 1:
                    continue
                self._log_transition(db, row["id"], P.QUEUED, P.CLAIMED, f"runner:{runner_id}")
                self._emit(db, row["id"], P.EV_JOB_CLAIMED, f"pris par {runner_id}")
                fresh = db.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
                claimed.append(
                    {
                        "protocol_version": P.PROTOCOL_VERSION,
                        "job_id": fresh["id"],
                        "fencing": fresh["fencing"],
                        "runtime": fresh["runtime"],
                        "workspace_id": fresh["workspace_id"],
                        "mode": fresh["mode"],
                        "prompt": fresh["prompt"],
                        "timeout_s": fresh["timeout_s"],
                        "lease_s": P.LEASE_S,
                    }
                )
            return claimed

    def transition(
        self,
        runner_id: str,
        epoch: int,
        job_id: str,
        fencing: int,
        src: str,
        dst: str,
        exit_code: int | None = None,
        result_summary: str | None = None,
        error: str | None = None,
        runtime_session_id: str | None = None,
    ) -> dict[str, Any]:
        if (src, dst) not in P.RUNNER_TRANSITIONS:
            raise BrokerError("invalid_transition", f"{src}->{dst} interdit au runner")
        if dst == P.COMPLETED and exit_code != 0:
            raise BrokerError("invalid_transition", "completed exige exit_code=0")
        now = self.clock()
        with self._tx() as db:
            self._runner_epoch_ok(db, runner_id, epoch)
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None or row["runner_id"] != runner_id:
                raise BrokerError("unknown_job", "job inconnu pour ce runner")
            if int(row["fencing"]) != int(fencing):
                raise BrokerError("stale_fencing", "fencing token périmé")
            if row["state"] == dst:
                return self._job_view(row)  # retry idempotent de la même transition
            if row["state"] != src:
                raise BrokerError("state_conflict", f"état actuel {row['state']}, attendu {src}")
            terminal = dst in P.TERMINAL
            # Requête fixe : un champ absent (None) conserve sa valeur via COALESCE.
            # Une transition terminale solde la reprise (recovery_state=NULL) mais garde
            # resume_count comme historique ; une transition non terminale conserve l'état.
            cur = db.execute(
                """UPDATE jobs SET state=?, updated_at=?, last_event_at=?, lease_expires=?,
                     started_at=COALESCE(?, started_at), finished_at=COALESCE(?, finished_at),
                     exit_code=COALESCE(?, exit_code), result_summary=COALESCE(?, result_summary),
                     error=COALESCE(?, error), runtime_session_id=COALESCE(?, runtime_session_id),
                     recovery_state=CASE WHEN ? THEN NULL ELSE recovery_state END,
                     recovery_detail=CASE WHEN ? THEN NULL ELSE recovery_detail END
                   WHERE id=? AND state=? AND fencing=?""",
                (
                    dst,
                    now,
                    now,
                    None if terminal else now + P.LEASE_S,
                    now if dst == P.RUNNING else None,
                    now if terminal else None,
                    None if exit_code is None else int(exit_code),
                    None if result_summary is None else P.clip(redact(result_summary), P.MAX_SUMMARY_CHARS),
                    None if error is None else P.clip(redact(error), P.MAX_ERROR_CHARS),
                    None if runtime_session_id is None else str(runtime_session_id)[:128],
                    1 if terminal else 0,
                    1 if terminal else 0,
                    job_id,
                    src,
                    fencing,
                ),
            )
            if cur.rowcount != 1:
                raise BrokerError("state_conflict", "transition concurrente")
            self._log_transition(db, job_id, src, dst, f"runner:{runner_id}")
            if (src, dst) == (P.CLAIMED, P.STARTING):
                self._emit(db, job_id, P.EV_RUNTIME_SPAWNED, f"runtime {row['runtime']}")
            elif (src, dst) == (P.STARTING, P.RUNNING):
                self._emit(db, job_id, P.EV_PROCESS_RUNNING, "processus agent repris")
            elif terminal:
                self._emit(db, job_id, P.EV_PROCESS_EXIT, f"from={src} exit={exit_code}")
                self._mission_on_job_terminal(db, job_id, dst)
            return self._job_view(db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())

    def event(
        self,
        runner_id: str,
        epoch: int,
        job_id: str,
        fencing: int,
        event_id: str,
        activity: str | None = None,
        output: str | None = None,
        runtime_session_id: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(event_id, str) or not 8 <= len(event_id) <= 64:
            raise BrokerError("invalid_event", "event_id invalide")
        now = self.clock()
        with self._tx() as db:
            self._runner_epoch_ok(db, runner_id, epoch)
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None or row["runner_id"] != runner_id:
                raise BrokerError("unknown_job", "job inconnu pour ce runner")
            if int(row["fencing"]) != int(fencing):
                raise BrokerError("stale_fencing", "fencing token périmé")
            try:
                db.execute("INSERT INTO events(job_id, event_id, received_at) VALUES (?,?,?)", (job_id, event_id, now))
            except sqlite3.IntegrityError:
                return {"duplicate": True, "cancel": bool(row["cancel_requested"])}
            if row["state"] not in P.ACTIVE:
                return {"duplicate": False, "ignored": True, "cancel": False}
            chunks, chars, truncated = int(row["output_chunks"]), int(row["output_chars"]), int(row["output_truncated"])
            prev_step = chars // P.OUTPUT_PROGRESS_STEP_CHARS
            got_output = False
            if output:
                text = redact(output)[: P.MAX_CHUNK_CHARS]
                room = P.MAX_OUTPUT_CHARS_PER_JOB - chars
                if room <= 0:
                    truncated = 1
                else:
                    if len(text) > room:
                        text = text[:room]
                        truncated = 1
                    db.execute("INSERT INTO output(job_id, seq, text) VALUES (?,?,?)", (job_id, chunks, text))
                    chunks, chars = chunks + 1, chars + len(text)
                    got_output = True
            new_activity = P.clip(redact(activity.strip()), P.MAX_ACTIVITY_CHARS) if activity else None
            db.execute(
                """UPDATE jobs SET updated_at=?, last_event_at=?, last_output_at=?,
                     lease_expires=?, last_activity=COALESCE(?, last_activity),
                     runtime_session_id=COALESCE(?, runtime_session_id),
                     output_chunks=?, output_chars=?, output_truncated=?,
                     stall_suspect_at=NULL, stall_at=NULL
                   WHERE id=?""",
                (
                    now,
                    now,
                    now if got_output else row["last_output_at"],
                    now + P.LEASE_S,
                    new_activity,
                    str(runtime_session_id)[:128] if runtime_session_id else None,
                    chunks,
                    chars,
                    truncated,
                    job_id,
                ),
            )
            if got_output and chars // P.OUTPUT_PROGRESS_STEP_CHARS > prev_step:
                self._emit(db, job_id, P.EV_OUTPUT_PROGRESS, f"{chars} caractères reçus")
            if new_activity and new_activity != (row["last_activity"] or None):
                self._emit(db, job_id, P.EV_ACTIVITY, new_activity)
            return {"duplicate": False, "cancel": bool(row["cancel_requested"])}

    def cancel(self, job_id: str) -> dict[str, Any]:
        now = self.clock()
        with self._tx() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return {"job_id": job_id, "result": "unknown_job"}
            if row["state"] in P.TERMINAL:
                return {"job_id": job_id, "result": "already_finished", "state": row["state"]}
            if row["state"] == P.QUEUED:
                self._set_terminal(db, job_id, P.QUEUED, P.CANCELLED, "mcp", error="annulé avant prise en charge")
                self._emit(db, job_id, P.EV_CANCEL_REQUESTED, "annulé en file par MCP")
                return {"job_id": job_id, "result": "cancelled", "state": P.CANCELLED}
            db.execute("UPDATE jobs SET cancel_requested=1, updated_at=? WHERE id=?", (now, job_id))
            self._emit(db, job_id, P.EV_CANCEL_REQUESTED, f"annulation demandée (état {row['state']})")
            log.info("job_cancel_requested job_id=%s state=%s", job_id, row["state"])
            return {"job_id": job_id, "result": "cancel_requested", "state": row["state"]}

    # --------------------------------------------------------------- lecture
    def get_job(self, job_id: str, tail_chars: int = 2_000) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return None
            runner = self._db.execute(
                "SELECT last_seen FROM runners WHERE id=?", (row["runner_id"],)
            ).fetchone()
            view = self._job_view(row, runner["last_seen"] if runner else None)
            tail_chars = max(0, min(int(tail_chars), 8_000))
            if tail_chars and row["output_chunks"]:
                chunks = self._db.execute(
                    "SELECT text FROM output WHERE job_id=? ORDER BY seq DESC LIMIT 16", (job_id,)
                ).fetchall()
                text = "".join(c["text"] for c in reversed(chunks))
                view["output_tail"] = text[-tail_chars:]
            return view

    def list_jobs(
        self,
        state: str | None = None,
        runtime: str | None = None,
        workspace_id: str | None = None,
        runner_id: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        where, args = [], []
        for col, val in (("state", state), ("runtime", runtime), ("workspace_id", workspace_id), ("runner_id", runner_id)):
            if val:
                where.append(f"{col}=?")
                args.append(val)
        sql = "SELECT * FROM jobs"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC LIMIT ?"
        args.append(max(1, min(int(limit), 100)))
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
        return [
            {
                "job_id": r["id"],
                "state": r["state"],
                "runtime": r["runtime"],
                "workspace_id": r["workspace_id"],
                "created_at": _iso(r["created_at"]),
                "finished_at": _iso(r["finished_at"]),
                "exit_code": r["exit_code"],
                "last_activity": r["last_activity"],
            }
            for r in rows
        ]

    def read_output(self, job_id: str, cursor: int = 0, limit: int = 8_000) -> dict[str, Any] | None:
        limit = max(1, min(int(limit), P.MAX_OUTPUT_PAGE))
        cursor = max(0, int(cursor))
        with self._lock:
            row = self._db.execute("SELECT output_chunks, output_truncated, state FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return None
            parts, size, seq = [], 0, cursor
            while seq < int(row["output_chunks"]) and size < limit:
                chunk = self._db.execute("SELECT text FROM output WHERE job_id=? AND seq=?", (job_id, seq)).fetchone()
                if chunk is None:
                    break  # purgé
                parts.append(chunk["text"])
                size += len(chunk["text"])
                seq += 1
            text = "".join(parts)
            return {
                "job_id": job_id,
                "state": row["state"],
                "cursor": cursor,
                "next_cursor": seq if seq < int(row["output_chunks"]) else None,
                "text": text[:limit] if len(text) > limit else text,
                "truncated_by_broker": bool(row["output_truncated"]),
            }

    # ----------------------------------------------------------- maintenance
    def reap(self) -> dict[str, int]:
        """Bails expirés, timeouts durs et détection de stalls. Appelé périodiquement par le serveur.

        Politique sûre : la détection de stall ÉMET un événement (notify) ; elle ne
        relance ni n'annule jamais seule. Première expiration d'un bail starting/running
        -> parking `suspended` (grâce RECOVERY_GRACE_S, non relancé) ; seconde expiration
        (grâce dépassée) -> `lost`. Un `lost` garde 'issue inconnue' avec les
        couches broker/runner/process pour diagnostiquer la cause observable."""
        now = self.clock()
        stats = {"requeued": 0, "lost": 0, "suspended": 0, "timeout_cancel": 0, "failed": 0, "stalled": 0, "suspected_stall": 0}
        with self._tx() as db:
            for row in db.execute(
                "SELECT id, state, attempt, cancel_requested, recovery_state FROM jobs WHERE state IN ('claimed','starting','running') AND lease_expires < ?",
                (now,),
            ).fetchall():
                self._emit(db, row["id"], P.EV_LEASE_EXPIRED, f"bail expiré en état {row['state']}")
                if row["state"] == P.CLAIMED:
                    if row["cancel_requested"]:
                        self._set_terminal(db, row["id"], P.CLAIMED, P.CANCELLED, "reaper", error="annulé, bail expiré avant lancement")
                    elif int(row["attempt"]) >= MAX_CLAIM_ATTEMPTS:
                        self._set_terminal(db, row["id"], P.CLAIMED, P.FAILED, "reaper", error="bail expiré trop de fois avant lancement")
                        stats["failed"] += 1
                    else:
                        self._requeue(db, row["id"], "lease_expired")
                        self._emit(db, row["id"], P.EV_REQUEUED, "bail expiré avant lancement : remis en file")
                        stats["requeued"] += 1
                elif row["recovery_state"] in (P.RECOVERING, P.SUSPENDED):
                    self._set_terminal(
                        db, row["id"], row["state"], P.LOST, "reaper",
                        error="grâce de reprise expirée (runner muet) : issue inconnue, non relancé",
                    )
                    stats["lost"] += 1
                else:
                    self._park_suspended(
                        db, row["id"], row["state"],
                        "bail expiré (runner muet) après lancement : parqué en attente de reprise, non relancé",
                    )
                    stats["suspended"] += 1
            # Stalls : processus vivant (télémétrie) + silence d'activité/output.
            # Edge-triggered via stall_suspect_at/stall_at (réarmés à la prochaine activité).
            for row in db.execute(
                """SELECT id, state, proc_alive, last_event_at, started_at, created_at,
                          stall_suspect_at, stall_at, lease_expires
                   FROM jobs WHERE state IN ('starting','running') AND lease_expires >= ? AND proc_alive = 1""",
                (now,),
            ).fetchall():
                last_sig = row["last_event_at"] or row["started_at"] or row["created_at"] or now
                silence = now - last_sig
                if silence >= P.STALL_S and not row["stall_at"]:
                    db.execute("UPDATE jobs SET stall_at=? WHERE id=?", (now, row["id"]))
                    self._emit(db, row["id"], P.EV_STALLED, f"aucune activité/output depuis {int(silence)} s (processus vivant)")
                    stats["stalled"] += 1
                elif silence >= P.STALL_SUSPECT_S and not row["stall_suspect_at"]:
                    db.execute("UPDATE jobs SET stall_suspect_at=? WHERE id=?", (now, row["id"]))
                    self._emit(db, row["id"], P.EV_SUSPECTED_STALL, f"aucune activité/output depuis {int(silence)} s (processus vivant)")
                    stats["suspected_stall"] += 1
            for row in db.execute(
                "SELECT id FROM jobs WHERE state='running' AND cancel_requested=0 AND started_at + timeout_s + 120 < ?",
                (now,),
            ).fetchall():
                db.execute("UPDATE jobs SET cancel_requested=1, updated_at=? WHERE id=?", (now, row["id"]))
                self._emit(db, row["id"], P.EV_TIMEOUT_MARKED, "timeout dur dépassé : annulation demandée")
                stats["timeout_cancel"] += 1
        return stats

    def purge(self, retention: Retention = Retention()) -> dict[str, int]:
        now = self.clock()
        with self._tx() as db:
            p = db.execute(
                "UPDATE jobs SET prompt=NULL WHERE prompt IS NOT NULL AND state IN ('completed','failed','timeout','cancelled','lost') AND finished_at < ?",
                (now - retention.prompt_s,),
            ).rowcount
            o = db.execute(
                "DELETE FROM output WHERE job_id IN (SELECT id FROM jobs WHERE finished_at IS NOT NULL AND finished_at < ?)",
                (now - retention.output_s,),
            ).rowcount
            old = [r["id"] for r in db.execute(
                "SELECT id FROM jobs WHERE finished_at IS NOT NULL AND finished_at < ?", (now - retention.meta_s,)
            ).fetchall()]
            for job_id in old:
                db.execute("DELETE FROM events WHERE job_id=?", (job_id,))
                db.execute("DELETE FROM transitions WHERE job_id=?", (job_id,))
                db.execute("DELETE FROM job_events WHERE job_id=?", (job_id,))
                db.execute("DELETE FROM jobs WHERE id=?", (job_id,))
            db.execute("DELETE FROM events WHERE received_at < ?", (now - retention.output_s,))
            db.execute("DELETE FROM job_events WHERE at < ?", (now - retention.output_s,))
            db.execute(
                "DELETE FROM mission_attempts WHERE mission_id IN (SELECT id FROM missions WHERE state IN ('validated','failed') AND updated_at < ?)",
                (now - retention.meta_s,),
            )
            db.execute(
                "DELETE FROM missions WHERE state IN ('validated','failed') AND updated_at < ?",
                (now - retention.meta_s,),
            )
        return {"prompts": p, "output_chunks": o, "jobs": len(old)}

    # -------------------------------------------------------------- missions
    def create_mission(
        self,
        objective: str,
        acceptance_criteria: list[str],
        max_attempts: int = 2,
        runner_id: str = "",
        runtime: str = "",
        workspace_id: str = "",
        mode: str = "read_only",
        timeout_s: int | None = None,
        prompt: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Crée une mission + sa première tentative (un job). `completed` (exit 0)
        ne valide JAMAIS la mission : le job terminal exit 0 passe la mission en
        `needs_validation`, tout autre terminal en `incomplete`. Aucun retry auto."""
        if not isinstance(objective, str) or not objective.strip() or len(objective) > P.MAX_OBJECTIVE_CHARS:
            raise BrokerError("invalid_objective", f"objectif requis (1..{P.MAX_OBJECTIVE_CHARS} car.)")
        if (
            not isinstance(acceptance_criteria, list)
            or not 1 <= len(acceptance_criteria) <= P.MAX_CRITERIA
            or any(not isinstance(c, str) or not c.strip() or len(c) > P.MAX_CRITERION_CHARS for c in acceptance_criteria)
        ):
            raise BrokerError("invalid_criteria", f"1..{P.MAX_CRITERIA} critères non vides requis")
        max_attempts = int(max_attempts or 2)
        if not 1 <= max_attempts <= P.MAX_MISSION_ATTEMPTS:
            raise BrokerError("invalid_max_attempts", f"max_attempts 1..{P.MAX_MISSION_ATTEMPTS}")
        first_prompt = prompt if isinstance(prompt, str) and prompt.strip() else objective
        job, _ = self.create_job(runner_id, runtime, workspace_id, first_prompt, mode, timeout_s, idempotency_key)
        now = self.clock()
        mission_id = str(uuid.uuid4())
        with self._tx() as db:
            db.execute(
                """INSERT INTO missions(id, objective, acceptance_json, max_attempts, attempts,
                     current_job_id, state, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    mission_id, objective, json.dumps(acceptance_criteria, ensure_ascii=False)[:12_000],
                    max_attempts, 1, job["job_id"], P.MISSION_EXECUTING, now, now,
                ),
            )
            db.execute(
                "INSERT INTO mission_attempts(mission_id, attempt_no, job_id, at) VALUES (?,?,?,?)",
                (mission_id, 1, job["job_id"], now),
            )
        return self.get_mission(mission_id) or {"error": "internal", "mission_id": mission_id}

    def get_mission(self, mission_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM missions WHERE id=?", (mission_id,)).fetchone()
            if row is None:
                return None
            attempts = self._db.execute(
                "SELECT attempt_no, job_id FROM mission_attempts WHERE mission_id=? ORDER BY attempt_no",
                (mission_id,),
            ).fetchall()
            job_summary = None
            if row["current_job_id"]:
                j = self._db.execute(
                    "SELECT state, exit_code, result_summary FROM jobs WHERE id=?", (row["current_job_id"],)
                ).fetchone()
                if j:
                    job_summary = {"job_id": row["current_job_id"], "state": j["state"], "exit_code": j["exit_code"]}
            return {
                "mission_id": row["id"],
                "objective": row["objective"],
                "acceptance_criteria": json.loads(row["acceptance_json"]),
                "max_attempts": row["max_attempts"],
                "attempts": row["attempts"],
                "current_job_id": row["current_job_id"],
                "current_job": job_summary,
                "state": row["state"],
                "validation_state": row["validation_state"],
                "validation_note": row["validation_note"],
                "attempt_job_ids": [a["job_id"] for a in attempts],
                "created_at": _iso(row["created_at"]),
                "updated_at": _iso(row["updated_at"]),
            }

    def retry_mission(self, mission_id: str, prompt: str | None = None) -> dict[str, Any]:
        """Nouvelle tentative de la MÊME mission (nouveau job). Exige : mission en
        needs_validation/incomplete, tentative précédente terminale, attempts < max.
        Jamais de relance aveugle : l'appelant MCP décide après examen du journal."""
        with self._tx() as db:
            m = db.execute("SELECT * FROM missions WHERE id=?", (mission_id,)).fetchone()
            if m is None:
                raise BrokerError("unknown_mission", "mission inconnue")
            if m["state"] not in (P.MISSION_NEEDS_VALIDATION, P.MISSION_INCOMPLETE):
                raise BrokerError("mission_not_retryable", f"mission {m['state']} : retry refusé")
            if int(m["attempts"]) >= int(m["max_attempts"]):
                raise BrokerError("max_attempts_reached", "plus de tentatives autorisées")
            cur = db.execute("SELECT state FROM jobs WHERE id=?", (m["current_job_id"],)).fetchone() if m["current_job_id"] else None
            if cur is None or cur["state"] not in P.TERMINAL:
                raise BrokerError("attempt_still_active", "la tentative en cours n'est pas terminée")
            job_spec = db.execute(
                "SELECT runner_id, runtime, workspace_id, mode, timeout_s FROM jobs WHERE id=?", (m["current_job_id"],)
            ).fetchone()
        job, _ = self.create_job(
            job_spec["runner_id"], job_spec["runtime"], job_spec["workspace_id"],
            prompt if isinstance(prompt, str) and prompt.strip() else m["objective"],
            job_spec["mode"], job_spec["timeout_s"],
        )
        now = self.clock()
        with self._tx() as db:
            attempt_no = int(m["attempts"]) + 1
            db.execute(
                "UPDATE missions SET attempts=?, current_job_id=?, state=?, validation_state=NULL, updated_at=? WHERE id=?",
                (attempt_no, job["job_id"], P.MISSION_EXECUTING, now, mission_id),
            )
            db.execute(
                "INSERT INTO mission_attempts(mission_id, attempt_no, job_id, at) VALUES (?,?,?,?)",
                (mission_id, attempt_no, job["job_id"], now),
            )
        return self.get_mission(mission_id) or {"error": "internal", "mission_id": mission_id}

    def validate_mission(self, mission_id: str, verdict: str, note: str | None = None) -> dict[str, Any]:
        """Validation humaine (ChatGPT) : seule elle fait passer une mission à
        `validated`. `completed` du processus ≠ mission réussie."""
        if verdict not in ("validated", "incomplete", "blocked", "failed"):
            raise BrokerError("invalid_verdict", "verdict : validated | incomplete | blocked | failed")
        now = self.clock()
        with self._tx() as db:
            m = db.execute("SELECT * FROM missions WHERE id=?", (mission_id,)).fetchone()
            if m is None:
                raise BrokerError("unknown_mission", "mission inconnue")
            if m["state"] not in (P.MISSION_NEEDS_VALIDATION, P.MISSION_INCOMPLETE, P.MISSION_BLOCKED):
                raise BrokerError("mission_not_validatable", f"mission {m['state']} : validation refusée")
            clean_note = P.clip(redact(note.strip()), 2_000) if isinstance(note, str) and note.strip() else None
            db.execute(
                "UPDATE missions SET state=?, validation_state=?, validation_note=?, updated_at=? WHERE id=?",
                (verdict, verdict, clean_note, now, mission_id),
            )
        return self.get_mission(mission_id) or {"error": "internal", "mission_id": mission_id}

    def _mission_on_job_terminal(self, db, job_id: str, job_state: str) -> None:
        """Hook : un job terminal fait progresser sa mission, sans jamais valider.
        exit 0 -> needs_validation ; sinon -> incomplete. Pas de retry automatique."""
        m = db.execute(
            "SELECT id, state FROM missions WHERE current_job_id=? AND state=?", (job_id, P.MISSION_EXECUTING)
        ).fetchone()
        if m is None:
            return
        nxt = P.MISSION_NEEDS_VALIDATION if job_state == P.COMPLETED else P.MISSION_INCOMPLETE
        db.execute(
            "UPDATE missions SET state=?, updated_at=? WHERE id=?", (nxt, self.clock(), m["id"])
        )
        log.info("mission_progress mission_id=%s job %s -> %s", m["id"], job_state, nxt)

    # -------------------------------------------------------------- internes
    def _requeue(self, db, job_id: str, reason: str) -> None:
        db.execute(
            "UPDATE jobs SET state='queued', lease_expires=NULL, epoch=NULL, updated_at=? WHERE id=? AND state='claimed'",
            (self.clock(), job_id),
        )
        self._log_transition(db, job_id, P.CLAIMED, P.QUEUED, f"broker:{reason}")

    def _park_suspended(self, db, job_id: str, src: str, reason: str) -> None:
        """Parking explicite : état conservé, bail prolongé de RECOVERY_GRACE_S,
        jamais relancé. Grâce bornée : à la prochaine expiration -> `lost`."""
        now = self.clock()
        cur = db.execute(
            """UPDATE jobs SET lease_expires=?, updated_at=?, last_event_at=?,
                 recovery_state=?, recovery_detail=?
               WHERE id=? AND state=?""",
            (now + P.RECOVERY_GRACE_S, now, now, P.SUSPENDED, P.clip(reason, 500), job_id, src),
        )
        if cur.rowcount == 1:
            self._emit(db, job_id, P.EV_JOB_SUSPENDED, reason)

    def _set_terminal(self, db, job_id: str, src: str, dst: str, actor: str, error: str | None = None) -> None:
        now = self.clock()
        cur = db.execute(
            "UPDATE jobs SET state=?, finished_at=?, updated_at=?, last_event_at=?, lease_expires=NULL, error=COALESCE(?, error) WHERE id=? AND state=?",
            (dst, now, now, now, error, job_id, src),
        )
        if cur.rowcount == 1:
            self._log_transition(db, job_id, src, dst, actor)
            if src == P.QUEUED:
                self._emit(db, job_id, P.EV_PROCESS_EXIT, f"from={src} : aucun processus lancé")
            else:
                self._emit(db, job_id, P.EV_PROCESS_EXIT, f"from={src} par {actor}")
            self._mission_on_job_terminal(db, job_id, dst)

    def _job_view(self, row: sqlite3.Row, runner_last_seen: float | None = None) -> dict[str, Any]:
        now = self.clock()
        started, finished = row["started_at"], row["finished_at"]
        duration = None
        if started:
            duration = round((finished or now) - started, 1)
        lease_exp = row["lease_expires"]
        lease_valid = lease_exp is not None and lease_exp > now and row["state"] in P.ACTIVE
        hb_age = round(now - runner_last_seen, 1) if runner_last_seen is not None else None
        runner_online = runner_last_seen is not None and now - runner_last_seen <= P.ONLINE_WINDOW_S
        alive = row["proc_alive"]
        alive = None if alive is None else bool(alive)
        children = row["child_procs"]
        children = None if children is None else int(children)
        view = {
            "job_id": row["id"],
            "runner_id": row["runner_id"],
            "runtime": row["runtime"],
            "workspace_id": row["workspace_id"],
            "mode": row["mode"],
            "state": row["state"],
            "cancel_requested": bool(row["cancel_requested"]),
            "created_at": _iso(row["created_at"]),
            "started_at": _iso(started),
            "finished_at": _iso(finished),
            "duration_s": duration,
            "exit_code": row["exit_code"],
            "last_activity": row["last_activity"],
            "result_summary": row["result_summary"],
            "error": row["error"],
            "runtime_session_id": row["runtime_session_id"],
            "output_chars": row["output_chars"],
            "output_truncated": bool(row["output_truncated"]),
            "attempt": row["attempt"],
            "recovery_state": _col(row, "recovery_state"),
            "recovery_detail": _col(row, "recovery_detail"),
            "resume_count": int(_col(row, "resume_count") or 0),
            # --- supervision riche : état structuré d'exécution (null = non observé)
            "runner_last_seen_at": _iso(runner_last_seen),
            "runner_heartbeat_age_s": hb_age,
            "process_alive": alive,
            "pid": row["proc_pid"],
            "process_started_at": _iso(row["proc_started_at"]),
            "last_output_at": _iso(row["last_output_at"]),
            "last_event_at": _iso(row["last_event_at"]),
            "current_activity": row["last_activity"],
            "current_tool": row["current_tool"],
            "current_command_sanitized": None,  # non observable via les adapters : jamais inventé
            "child_process_count": children,
            "output_chunks": row["output_chunks"],
            "execution_health": self._execution_health(row, runner_last_seen, now),
            "broker_health": {
                "lease_expires_at": _iso(lease_exp),
                "lease_valid": bool(lease_valid),
            },
            "runner_health": {
                "status": "online" if runner_online else "offline",
                "seconds_since_seen": hb_age,
            },
            "runtime_process_health": {
                "alive": alive,
                "pid": row["proc_pid"],
                "started_at": _iso(row["proc_started_at"]),
                "child_process_count": children,
            },
        }
        return view

    def _execution_health(self, row: sqlite3.Row, runner_last_seen: float | None, now: float) -> str | None:
        """Santé d'exécution calculée, jamais inventée : chaque signal manquant
        dégrade vers une valeur prudente (unknown -> couches à null)."""
        state = row["state"]
        if state in P.TERMINAL:
            return None  # un état terminal se lit via state/exit_code, pas via la santé
        if runner_last_seen is None or now - runner_last_seen > P.ONLINE_WINDOW_S:
            return P.RUNNER_DISCONNECTED
        if state in (P.QUEUED, P.CLAIMED):
            return P.IDLE  # en attente de prise en charge / lancement
        alive = row["proc_alive"]
        if alive is not None and not alive:
            return P.PROCESS_DEAD
        last_sig = row["last_event_at"] or row["started_at"] or row["created_at"]
        silence = now - (last_sig or now)
        if alive and silence >= P.STALL_S:
            return P.STALLED
        if alive and silence >= P.STALL_SUSPECT_S:
            return P.SUSPECTED_STALL
        if not row["output_chunks"] and silence < 120:
            return P.IDLE  # démarrage récent, rien reçu encore
        if alive:
            return P.HEALTHY
        return P.IDLE  # télémétrie absente (vieux runner) mais bail frais : pas de faux signal

    # ------------------------------------------------- journal d'événements
    def read_events(self, job_id: str, after_seq: int = -1, limit: int = 50) -> dict[str, Any] | None:
        """Événements structurés ordonnés et paginés (pas de transcript)."""
        limit = max(1, min(int(limit), 200))
        after_seq = max(-1, int(after_seq))
        with self._lock:
            exists = self._db.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,)).fetchone()
            if exists is None:
                return None
            rows = self._db.execute(
                "SELECT seq, at, kind, detail FROM job_events WHERE job_id=? AND seq > ? ORDER BY seq LIMIT ?",
                (job_id, after_seq, limit),
            ).fetchall()
            last = self._db.execute("SELECT COALESCE(MAX(seq), -1) AS m FROM job_events WHERE job_id=?", (job_id,)).fetchone()
            return {
                "job_id": job_id,
                "events": [
                    {"seq": r["seq"], "at": _iso(r["at"]), "kind": r["kind"], "detail": r["detail"]}
                    for r in rows
                ],
                "next_seq": rows[-1]["seq"] + 1 if rows else after_seq + 1,
                "last_seq": int(last["m"]),
            }

    # ------------------------------------------------------- runner inspect
    def runner_inspect(self, runner_id: str) -> dict[str, Any] | None:
        """Snapshot compact runner/environnement : versions, capacités, workspaces,
        état réseau/broker, jobs actifs enrichis, git par workspace si observé.
        Jamais de secrets, jamais de dump d'environnement."""
        now = self.clock()
        with self._lock:
            r = self._db.execute("SELECT * FROM runners WHERE id=?", (runner_id,)).fetchone()
            if r is None:
                return None
            online = r["last_seen"] is not None and now - float(r["last_seen"]) <= P.ONLINE_WINDOW_S
            env = [
                {
                    "workspace_id": e["workspace_id"],
                    "branch": e["branch"],
                    "head": e["head"],
                    "dirty": bool(e["dirty"]),
                    "observed_at": _iso(e["at"]),
                }
                for e in self._db.execute(
                    "SELECT * FROM runner_env WHERE runner_id=? ORDER BY workspace_id", (runner_id,)
                ).fetchall()
            ]
            active_ids = self._db.execute(
                "SELECT id FROM jobs WHERE runner_id=? AND state IN ('claimed','starting','running') ORDER BY created_at",
                (runner_id,),
            ).fetchall()
            active = []
            for a in active_ids:
                row = self._db.execute("SELECT * FROM jobs WHERE id=?", (a["id"],)).fetchone()
                v = self._job_view(row, r["last_seen"])
                v.pop("output_tail", None)
                active.append(v)
            return {
                "runner_id": r["id"],
                "status": "online" if online else "offline",
                "last_seen": _iso(r["last_seen"]),
                "seconds_since_seen": None if r["last_seen"] is None else round(now - r["last_seen"], 1),
                "runner_version": r["version"],
                "max_parallel": r["max_parallel"],
                "runtimes": json.loads(r["runtimes_json"]),
                "workspaces": json.loads(r["workspaces_json"]),
                "workspace_git": env,
                "active_jobs": active,
            }

    # ------------------------------------------------------------- attente
    def wait_for_change(self, job_id: str, since_seq: int = -1, timeout_s: float = P.WAIT_DEFAULT_S) -> dict[str, Any] | None:
        """Long-poll borné : se réveille sur changement significatif (état, sortie,
        événement) ou à expiration du timeout. Réveil par polling 0,2 s, jamais plus
        de WAIT_MAX_S. N'est PAS un fond de tâche : le réveil exige un tour actif."""
        timeout_s = max(0.0, min(float(timeout_s), P.WAIT_MAX_S))
        since_seq = max(-1, int(since_seq))
        import time as _t

        deadline = _t.monotonic() + timeout_s
        with self._lock:
            row = self._db.execute("SELECT updated_at, state FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return None
            baseline_updated, baseline_state = float(row["updated_at"]), row["state"]
            base_seq = self._event_seq(self._db, job_id)
        woke_by = "timeout"
        while _t.monotonic() < deadline:
            _t.sleep(0.2)
            with self._lock:
                row = self._db.execute("SELECT updated_at, state FROM jobs WHERE id=?", (job_id,)).fetchone()
                if row is None:
                    return None
                seq = self._event_seq(self._db, job_id)
                if row["state"] in P.TERMINAL and row["state"] != baseline_state:
                    woke_by = "terminal"
                    break
                if float(row["updated_at"]) != baseline_updated or seq != base_seq:
                    woke_by = "change" if row["state"] == baseline_state else "state"
                    break
                if seq > since_seq > base_seq:
                    woke_by = "event"
                    break
        view = self.get_job(job_id, tail_chars=0)
        if view is None:
            return None
        return {
            "job_id": job_id,
            "state": view["state"],
            "woke_by": woke_by,
            "execution_health": view["execution_health"],
            "last_event_seq": self._last_seq(job_id),
        }

    def _last_seq(self, job_id: str) -> int:
        with self._lock:
            row = self._db.execute("SELECT COALESCE(MAX(seq), -1) AS m FROM job_events WHERE job_id=?", (job_id,)).fetchone()
            return int(row["m"])


def _col(row: sqlite3.Row, name: str) -> Any:
    try:
        return row[name]
    except (IndexError, KeyError):
        return None  # base non migrée (vieux broker en rollback) : inconnu, pas inventé


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(ts)))
