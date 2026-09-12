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

    # ---------------------------------------------------------------- runners
    def _runner_epoch_ok(self, db, runner_id: str, epoch: int) -> None:
        row = db.execute("SELECT epoch FROM runners WHERE id=?", (runner_id,)).fetchone()
        if row is None:
            raise BrokerError("unknown_runner", "runner non enregistré (hello requis)")
        if int(row["epoch"]) != int(epoch):
            raise BrokerError("superseded", "session runner remplacée par une connexion plus récente")

    def hello(self, runner_id: str, info: dict[str, Any], held: list[dict[str, Any]]) -> int:
        """Ouvre une nouvelle session runner et réconcilie les jobs qu'il détenait.

        `held` = jobs que le processus runner gère réellement en ce moment (après
        un redémarrage : liste vide). Tout job actif du broker absent de `held` :
        - claimed  -> queued (rien n'a été lancé, relance sûre) ;
        - starting/running -> lost (issue inconnue, jamais relancé en silence).
        Les jobs présents dans `held` restent attachés à la nouvelle session.
        """
        now = self.clock()
        runtimes = [r for r in info.get("runtimes", []) if isinstance(r, dict) and r.get("id") in P.RUNTIMES]
        workspaces = [w for w in info.get("workspaces", []) if isinstance(w, dict) and P.valid_id(w.get("id"))]
        max_parallel = max(1, min(int(info.get("max_parallel", 1)), 16))
        held_map = {h.get("job_id"): int(h.get("fencing", -1)) for h in held if isinstance(h, dict)}
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
                "SELECT id, state, fencing FROM jobs WHERE runner_id=? AND state IN ('claimed','starting','running')",
                (runner_id,),
            ).fetchall()
            for job in active:
                if held_map.get(job["id"]) == int(job["fencing"]):
                    db.execute(
                        "UPDATE jobs SET epoch=?, lease_expires=?, updated_at=? WHERE id=?",
                        (epoch, now + P.LEASE_S, now, job["id"]),
                    )
                    continue
                if job["state"] == P.CLAIMED:
                    self._requeue(db, job["id"], "runner_restart")
                else:
                    self._set_terminal(
                        db, job["id"], job["state"], P.LOST, "broker",
                        error="runner redémarré ou reconnecté sans ce job : issue inconnue, non relancé",
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
                job_id = h.get("job_id")
                fencing = int(h.get("fencing", -1))
                row = db.execute(
                    "SELECT state, fencing, cancel_requested FROM jobs WHERE id=? AND runner_id=?",
                    (job_id, runner_id),
                ).fetchone()
                if row is None or int(row["fencing"]) != fencing or row["state"] not in P.ACTIVE:
                    unknown.append(str(job_id))  # le runner doit tuer ce processus
                    continue
                db.execute(
                    "UPDATE jobs SET lease_expires=?, epoch=? WHERE id=?", (now + P.LEASE_S, epoch, job_id)
                )
                if row["cancel_requested"]:
                    cancel.append(str(job_id))
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
            cur = db.execute(
                """UPDATE jobs SET state=?, updated_at=?, lease_expires=?,
                     started_at=COALESCE(?, started_at), finished_at=COALESCE(?, finished_at),
                     exit_code=COALESCE(?, exit_code), result_summary=COALESCE(?, result_summary),
                     error=COALESCE(?, error), runtime_session_id=COALESCE(?, runtime_session_id)
                   WHERE id=? AND state=? AND fencing=?""",
                (
                    dst,
                    now,
                    None if terminal else now + P.LEASE_S,
                    now if dst == P.RUNNING else None,
                    now if terminal else None,
                    None if exit_code is None else int(exit_code),
                    None if result_summary is None else P.clip(redact(result_summary), P.MAX_SUMMARY_CHARS),
                    None if error is None else P.clip(redact(error), P.MAX_ERROR_CHARS),
                    None if runtime_session_id is None else str(runtime_session_id)[:128],
                    job_id,
                    src,
                    fencing,
                ),
            )
            if cur.rowcount != 1:
                raise BrokerError("state_conflict", "transition concurrente")
            self._log_transition(db, job_id, src, dst, f"runner:{runner_id}")
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
            db.execute(
                """UPDATE jobs SET updated_at=?, lease_expires=?, last_activity=COALESCE(?, last_activity),
                     runtime_session_id=COALESCE(?, runtime_session_id),
                     output_chunks=?, output_chars=?, output_truncated=?
                   WHERE id=?""",
                (
                    now,
                    now + P.LEASE_S,
                    P.clip(redact(activity.strip()), P.MAX_ACTIVITY_CHARS) if activity else None,
                    str(runtime_session_id)[:128] if runtime_session_id else None,
                    chunks,
                    chars,
                    truncated,
                    job_id,
                ),
            )
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
                return {"job_id": job_id, "result": "cancelled", "state": P.CANCELLED}
            db.execute("UPDATE jobs SET cancel_requested=1, updated_at=? WHERE id=?", (now, job_id))
            log.info("job_cancel_requested job_id=%s state=%s", job_id, row["state"])
            return {"job_id": job_id, "result": "cancel_requested", "state": row["state"]}

    # --------------------------------------------------------------- lecture
    def get_job(self, job_id: str, tail_chars: int = 2_000) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return None
            view = self._job_view(row)
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
        """Bails expirés et timeouts durs. Appelé périodiquement par le serveur."""
        now = self.clock()
        stats = {"requeued": 0, "lost": 0, "timeout_cancel": 0, "failed": 0}
        with self._tx() as db:
            for row in db.execute(
                "SELECT id, state, attempt, cancel_requested FROM jobs WHERE state IN ('claimed','starting','running') AND lease_expires < ?",
                (now,),
            ).fetchall():
                if row["state"] == P.CLAIMED:
                    if row["cancel_requested"]:
                        self._set_terminal(db, row["id"], P.CLAIMED, P.CANCELLED, "reaper", error="annulé, bail expiré avant lancement")
                    elif int(row["attempt"]) >= MAX_CLAIM_ATTEMPTS:
                        self._set_terminal(db, row["id"], P.CLAIMED, P.FAILED, "reaper", error="bail expiré trop de fois avant lancement")
                        stats["failed"] += 1
                    else:
                        self._requeue(db, row["id"], "lease_expired")
                        stats["requeued"] += 1
                else:
                    self._set_terminal(
                        db, row["id"], row["state"], P.LOST, "reaper",
                        error="bail expiré (runner muet) après lancement : issue inconnue, non relancé",
                    )
                    stats["lost"] += 1
            for row in db.execute(
                "SELECT id FROM jobs WHERE state='running' AND cancel_requested=0 AND started_at + timeout_s + 120 < ?",
                (now,),
            ).fetchall():
                db.execute("UPDATE jobs SET cancel_requested=1, updated_at=? WHERE id=?", (now, row["id"]))
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
                db.execute("DELETE FROM jobs WHERE id=?", (job_id,))
            db.execute("DELETE FROM events WHERE received_at < ?", (now - retention.output_s,))
        return {"prompts": p, "output_chunks": o, "jobs": len(old)}

    # -------------------------------------------------------------- internes
    def _requeue(self, db, job_id: str, reason: str) -> None:
        db.execute(
            "UPDATE jobs SET state='queued', lease_expires=NULL, epoch=NULL, updated_at=? WHERE id=? AND state='claimed'",
            (self.clock(), job_id),
        )
        self._log_transition(db, job_id, P.CLAIMED, P.QUEUED, f"broker:{reason}")

    def _set_terminal(self, db, job_id: str, src: str, dst: str, actor: str, error: str | None = None) -> None:
        now = self.clock()
        cur = db.execute(
            "UPDATE jobs SET state=?, finished_at=?, updated_at=?, lease_expires=NULL, error=COALESCE(?, error) WHERE id=? AND state=?",
            (dst, now, now, error, job_id, src),
        )
        if cur.rowcount == 1:
            self._log_transition(db, job_id, src, dst, actor)

    def _job_view(self, row: sqlite3.Row) -> dict[str, Any]:
        now = self.clock()
        started, finished = row["started_at"], row["finished_at"]
        duration = None
        if started:
            duration = round((finished or now) - started, 1)
        return {
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
        }


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(ts)))
