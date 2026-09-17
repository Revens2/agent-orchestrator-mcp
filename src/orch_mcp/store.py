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

# ------------------------------------------------- contrat de suivi (follow-through)
# Un timeout de wait pendant que le job continue N'EST PAS une raison de répondre
# à l'utilisateur : le caller DOIT rappeler l'attente. Ces blocs machine-lisibles
# rendent la boucle explicite (testée) au lieu de reposer sur une phrase doc.
FOLLOW_UNTIL = "terminal"
FOLLOW_WAIT_S = 25
FOLLOW_NEXT_WAIT = "agent_job_wait"
FOLLOW_NEXT_DONE = "agent_job_get"


def _paused(row) -> bool:
    """Le job porte-t-il une pause humaine en cours ? (colonne absente sur une
    base pas encore migrée => False, jamais une exception)."""
    try:
        return row["paused_at"] is not None
    except (IndexError, KeyError):
        return False


def follow_for_job(state: str, last_event_seq: int, wait_timeout_s: float = P.WAIT_DEFAULT_S) -> dict[str, Any]:
    """Bloc de suivi pour un job non terminal : quoi appeler, avec quel curseur,
    jusqu'à quoi. `terminal=True` => le job ne bougera plus (inspecter/valider)."""
    terminal = state in P.TERMINAL
    return {
        "must_follow": not terminal,
        "terminal": terminal,
        "should_continue": not terminal,
        "next_tool": FOLLOW_NEXT_DONE if terminal else FOLLOW_NEXT_WAIT,
        "wait_timeout_s": int(wait_timeout_s),
        "until": FOLLOW_UNTIL,
        "since_seq": int(last_event_seq),
    }


def follow_for_wait(
    state: str, woke_by: str, last_event_seq: int, human: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Bloc de suivi pour un retour de wait : `woke_by=timeout` + non terminal
    => rappeler `agent_job_wait` IMMÉDIATEMENT (même tour), pas de réponse user.

    `human` (bloc `human_action_required`) est le SEUL arrêt intermédiaire
    légitime : l'agent est vivant mais attend une action de l'utilisateur
    (nouvelle clé d'API, reconnexion, feu vert). Le suivi s'arrête alors —
    `stop_reason='waiting_for_human'` — pour que le caller PARLE à l'utilisateur
    au lieu d'attendre dans le vide ; il reprendra avec `agent_job_wait`."""
    terminal = state in P.TERMINAL
    blocked = bool(human) and not terminal
    out = {
        "must_follow": not terminal and not blocked,
        "terminal": terminal,
        "should_continue": not terminal and not blocked,
        "next_tool": FOLLOW_NEXT_DONE if terminal else FOLLOW_NEXT_WAIT,
        "wait_timeout_s": FOLLOW_WAIT_S,
        "until": FOLLOW_UNTIL,
        "since_seq": int(last_event_seq),
        "woke_by": woke_by,
    }
    if human:
        out["human_action_required"] = human
        if blocked:
            out["stop_reason"] = "waiting_for_human"
            out["resume_with"] = FOLLOW_NEXT_WAIT
    return out

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
-- Alertes infra (observabilité Telegram unifiée). Écriture réservée aux
-- ingesteurs locaux ; lecture via infra_alert_list/get (MCP read-only).
-- SCHEMA est rejoué à chaque init (IF NOT EXISTS) : les bases existantes
-- gagnent la table sans migration ; rollback = redéployer l'ancien src.
CREATE TABLE IF NOT EXISTS infra_alerts (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  service TEXT NOT NULL,
  severity TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'active',
  title TEXT NOT NULL,
  detail TEXT,
  fingerprint TEXT NOT NULL,
  occurrences INTEGER NOT NULL DEFAULT 1,
  first_at REAL NOT NULL,
  last_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS infra_alerts_src ON infra_alerts(source, state, last_at);
CREATE INDEX IF NOT EXISTS infra_alerts_fp ON infra_alerts(fingerprint, last_at);
-- Questions en attente (Photon/iMessage) : état explicite waiting_for_user.
-- SCHEMA rejoué à chaque init (IF NOT EXISTS) : rollback = ancien src.
CREATE TABLE IF NOT EXISTS pending_questions (
  id TEXT PRIMARY KEY,
  origin TEXT NOT NULL,
  session_ref TEXT NOT NULL,
  runtime TEXT NOT NULL,
  title TEXT NOT NULL,
  question TEXT NOT NULL,
  options_json TEXT NOT NULL DEFAULT '[]',
  fingerprint TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  notify_after_s INTEGER NOT NULL DEFAULT 300,
  notified_at REAL,
  notify_status TEXT NOT NULL DEFAULT 'pending',
  answer TEXT,
  answered_at REAL,
  answer_from TEXT,
  created_at REAL NOT NULL,
  expires_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS pending_questions_due ON pending_questions(status, notified_at, created_at);
CREATE INDEX IF NOT EXISTS pending_questions_fp ON pending_questions(fingerprint, status);
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
            if "recovery_since" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN recovery_since REAL")
            # Pause humaine explicite (signal de vie). NULL = jamais mis en pause :
            # l'ancien code ignore ces colonnes, le rollback reste possible.
            if "paused_at" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN paused_at REAL")
            if "pause_reason" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN pause_reason TEXT")
            if "pause_note" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN pause_note TEXT")
            if "pause_expires" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN pause_expires REAL")
            if "pause_source" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN pause_source TEXT")
            if "pause_count" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN pause_count INTEGER NOT NULL DEFAULT 0")
            if "resumed_at" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN resumed_at REAL")
            # Blocage humain constaté sur un job DÉJÀ terminal (l'agent est mort
            # sur un quota épuisé) : ce n'est pas une panne, c'est une action
            # humaine à faire puis un retry explicite.
            if "blocker_reason" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN blocker_reason TEXT")
            if "blocker_sign" not in cols:
                self._db.execute("ALTER TABLE jobs ADD COLUMN blocker_sign TEXT")
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

    @staticmethod
    def _telemetry_values(h: dict[str, Any]) -> dict[str, Any] | None:
        """Normalise la télémétrie d'exécution d'un payload runner (held d'un
        heartbeat, event de sortie, ou transition). Retourne None si aucune clé
        présente (non observé : on ne touche à rien, jamais inventé) ; sinon un
        mapping colonne -> valeur (None = observé-mais-inconnu, ex. requête Job
        Object en échec). Mapping UNIQUE pour les trois chemins (heartbeat,
        event, transition) : la sortie et la télémétrie restent cohérentes."""
        if not isinstance(h, dict) or not any(
            k in h for k in ("pid", "proc_alive", "proc_started_at", "child_procs", "tool")
        ):
            return None
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
        return {
            "proc_pid": pid,
            "proc_started_at": pstarted,
            "proc_alive": alive,
            "child_procs": children,
            "current_tool": P.clip(redact(str(h["tool"])[:120]), 120) if h.get("tool") else None,
        }

    def _store_telemetry(self, db, job_id: str, tele: dict[str, Any] | None) -> None:
        """Persiste une télémétrie normalisée (horodatée). Ne touche PAS à
        updated_at : un ping de présence n'est pas un changement significatif
        pour job_wait (seuls event/transition réveillent)."""
        if not tele:
            return
        db.execute(
            """UPDATE jobs SET telemetry_at=?, proc_pid=?, proc_started_at=?,
                 proc_alive=?, child_procs=?, current_tool=?
               WHERE id=?""",
            (
                self.clock(),
                tele["proc_pid"],
                tele["proc_started_at"],
                tele["proc_alive"],
                tele["child_procs"],
                tele["current_tool"],
                job_id,
            ),
        )

    # ---------------------------------------------------------------- runners
    def _execution_lease(self, db, row, tele=None, supervisor_alive=None, allow_recovery=False):
        """Bound recovery independently of heartbeats, output and reconnects.

        Missing telemetry never cancels a negative observation. Only positive
        process evidence within the recovery window can restore a normal lease.
        Old runners without instrumentation retain their existing protocol.
        """
        now = self.clock()
        if row["state"] not in (P.STARTING, P.RUNNING):
            return now + P.LEASE_S
        # Pause humaine explicite : le compte à rebours de recovery est SUSPENDU,
        # jamais remis à zéro ni falsifié (recovery_since garde sa valeur, la
        # télémétrie garde la sienne). Un processus mort pendant que l'utilisateur
        # change sa clé d'API ne devient donc pas `lost` en 60 s.
        if _paused(row):
            return now + P.LEASE_S
        since = row["recovery_since"]
        if since is not None and now >= since + P.PROCESS_RECOVERY_S:
            self._set_terminal(
                db, row["id"], row["state"], P.LOST, "broker",
                error="processus ou supervision indisponible : recovery expiree, issue inconnue, non relance",
            )
            return None
        alive = tele.get("proc_alive") if tele else None
        if alive == 0 or supervisor_alive is False:
            # Upgrade also respects a previous explicit dead observation.
            if since is None:
                since = row["telemetry_at"] if row["proc_alive"] == 0 and row["telemetry_at"] else now
        elif alive == 1 and supervisor_alive is not False and allow_recovery:
            since = None
        elif since is None and row["proc_alive"] == 0:
            since = row["telemetry_at"] or now
        db.execute("UPDATE jobs SET recovery_since=? WHERE id=?", (since, row["id"]))
        return min(now + P.LEASE_S, since + P.PROCESS_RECOVERY_S) if since is not None else now + P.LEASE_S

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
        held_map = {h.get("job_id"): h for h in held if isinstance(h, dict)}
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
                "SELECT * FROM jobs WHERE runner_id=? AND state IN ('claimed','starting','running')",
                (runner_id,),
            ).fetchall()
            for job in active:
                h = held_map.get(job["id"], {})
                if int(h.get("fencing", -1)) == int(job["fencing"]):
                    tele = self._telemetry_values(h)
                    lease = self._execution_lease(db, job, tele, h.get("supervisor_alive"), allow_recovery=True)
                    if lease is None:
                        continue
                    db.execute(
                        "UPDATE jobs SET epoch=?, lease_expires=?, updated_at=? WHERE id=?",
                        (epoch, lease, now, job["id"]),
                    )
                    self._store_telemetry(db, job["id"], tele)
                    continue
                self._emit(
                    db, job["id"], P.EV_RUNNER_DISCONNECT,
                    f"runner reconnecté (epoch {epoch}) sans détenir ce job",
                )
                if job["state"] == P.CLAIMED:
                    self._requeue(db, job["id"], "runner_restart")
                else:
                    self._set_terminal(
                        db, job["id"], job["state"], P.LOST, "broker",
                        error="runner redémarré ou reconnecté sans ce job : issue inconnue, non relancé",
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
                job_id = h.get("job_id")
                fencing = int(h.get("fencing", -1))
                row = db.execute(
                    "SELECT * FROM jobs WHERE id=? AND runner_id=?",
                    (job_id, runner_id),
                ).fetchone()
                if row is None or int(row["fencing"]) != fencing or row["state"] not in P.ACTIVE:
                    unknown.append(str(job_id))  # le runner doit tuer ce processus
                    continue
                tele = self._telemetry_values(h)
                lease = self._execution_lease(db, row, tele, h.get("supervisor_alive"), allow_recovery=True)
                if lease is None:
                    unknown.append(str(job_id))
                    continue
                db.execute("UPDATE jobs SET lease_expires=?, epoch=? WHERE id=?", (lease, epoch, job_id))
                if row["cancel_requested"]:
                    cancel.append(str(job_id))
                # Télémétrie d'exécution (optionnelle, runners récents) : état du
                # processus observé localement. Mapping unique (_telemetry_values) :
                # absent = non observé (on ne touche à rien), jamais inventé.
                self._store_telemetry(db, job_id, tele)
        return {"cancel": cancel, "abandon": unknown}

    def runner_job_state(self, runner_id: str, epoch: int, job_id: str, fencing: int) -> dict[str, Any]:
        """Read-only fenced reconciliation, without prompt/output disclosure."""
        with self._lock:
            self._runner_epoch_ok(self._db, runner_id, epoch)
            row = self._db.execute("SELECT runner_id,state,fencing,cancel_requested FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None or row["runner_id"] != runner_id:
                raise BrokerError("unknown_job", "job inconnu pour ce runner")
            if int(row["fencing"]) != fencing:
                raise BrokerError("stale_fencing", "fencing token périmé")
            return {"state": row["state"], "cancel_requested": bool(row["cancel_requested"])}

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
        allowed_modes = P.RUNTIME_MODES.get(runtime)
        if allowed_modes is not None and mode not in allowed_modes:
            raise BrokerError(
                "mode_denied",
                f"runtime {runtime} : mode {mode} non supporté (confinement workspace non démontrable) ; "
                f"autorisés : {', '.join(allowed_modes)}",
            )
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
        telemetry: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if (src, dst) not in P.RUNNER_TRANSITIONS:
            raise BrokerError("invalid_transition", f"{src}->{dst} interdit au runner")
        if dst == P.COMPLETED and exit_code != 0:
            raise BrokerError("invalid_transition", "completed exige exit_code=0")
        now = self.clock()
        tele = self._telemetry_values(telemetry or {})
        with self._tx() as db:
            self._runner_epoch_ok(db, runner_id, epoch)
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None or row["runner_id"] != runner_id:
                raise BrokerError("unknown_job", "job inconnu pour ce runner")
            if int(row["fencing"]) != int(fencing):
                raise BrokerError("stale_fencing", "fencing token périmé")
            if row["state"] == dst:
                self._store_telemetry(db, job_id, tele)  # retry idempotent : télémétrie quand même fraîche
                return self._job_view(db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())
            if row["state"] != src:
                raise BrokerError("state_conflict", f"état actuel {row['state']}, attendu {src}")
            terminal = dst in P.TERMINAL
            # Un job qui MEURT sur un quota épuisé ou une authentification expirée
            # n'est pas un agent défaillant : c'est une action humaine à faire.
            # On l'enregistre pour ne jamais l'annoncer comme un échec technique.
            blocker = None
            if terminal and dst != P.COMPLETED:
                blocker = P.match_pause_reason(error) or P.match_pause_reason(result_summary)
                if blocker is None and _paused(row):
                    blocker = (row["pause_reason"] or P.PAUSE_MANUAL, row["pause_note"] or "")
            lease = None if terminal else self._execution_lease(db, row, tele)
            if not terminal and lease is None:
                return self._job_view(db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())
            # Requête fixe : un champ absent (None) conserve sa valeur via COALESCE.
            cur = db.execute(
                """UPDATE jobs SET state=?, updated_at=?, last_event_at=?, lease_expires=?,
                     started_at=COALESCE(?, started_at), finished_at=COALESCE(?, finished_at),
                     exit_code=COALESCE(?, exit_code), result_summary=COALESCE(?, result_summary),
                     error=COALESCE(?, error), runtime_session_id=COALESCE(?, runtime_session_id)
                   WHERE id=? AND state=? AND fencing=?""",
                (
                    dst,
                    now,
                    now,
                    lease,
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
            if terminal:
                # Un job figé ne peut plus être « en pause » : la cause devient
                # un blocage constaté, exposé comme action humaine requise.
                db.execute(
                    "UPDATE jobs SET paused_at=NULL, pause_reason=NULL, pause_expires=NULL, "
                    "pause_source=NULL, blocker_reason=?, blocker_sign=? WHERE id=?",
                    (blocker[0] if blocker else None,
                     P.clip(redact(blocker[1]), P.MAX_PAUSE_NOTE_CHARS) if blocker and blocker[1] else None,
                     job_id),
                )
            self._log_transition(db, job_id, src, dst, f"runner:{runner_id}")
            # Télémétrie jointe à la transition (surtout STARTING->RUNNING : le
            # pid est connu dès le spawn, pas au prochain heartbeat). Sans elle,
            # un job court (sortie avant le premier heartbeat) restait à null.
            self._store_telemetry(db, job_id, tele)
            if (src, dst) == (P.CLAIMED, P.STARTING):
                self._emit(db, job_id, P.EV_RUNTIME_SPAWNED, f"runtime {row['runtime']}")
            elif (src, dst) == (P.STARTING, P.RUNNING):
                self._emit(db, job_id, P.EV_PROCESS_RUNNING, "processus agent repris")
            elif terminal:
                self._emit(db, job_id, P.EV_PROCESS_EXIT, f"from={src} exit={exit_code}")
                if dst == P.FAILED and isinstance(error, str) and error.startswith(P.SESSION_CORRUPTED_PREFIX):
                    # Session reconnue corrompue : événement structuré visible,
                    # session abandonnée (jamais réutilisée : le retry crée un
                    # nouveau job = nouvelle session + handoff, pas de transcript).
                    self._emit(db, job_id, P.EV_SESSION_CORRUPTED, error[:500])
                if blocker:
                    self._emit(db, job_id, P.EV_PAUSED,
                               f"{P.PAUSE_MESSAGES.get(blocker[0], blocker[0])} : action humaine requise")
                self._mission_on_job_terminal(db, job_id, dst, blocked=bool(blocker))
            return self._job_view(db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())

    def record_runner_question(
        self,
        runner_id: str,
        epoch: int,
        job_id: str,
        fencing: int,
        runtime: str,
        title: str,
        question: str,
        options: list[str] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Question explicite remontée par le runner (convention [[QUESTION]]).
        Vérifie epoch/fencing comme event() : un runner ne déclare une question
        que pour SON job actif. session_ref = job_id (routage exact)."""
        with self._lock:
            self._runner_epoch_ok(self._db, runner_id, epoch)
            row = self._db.execute("SELECT runner_id, fencing FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None or row["runner_id"] != runner_id:
                raise BrokerError("unknown_job", "job inconnu pour ce runner")
            if int(row["fencing"]) != int(fencing):
                raise BrokerError("stale_fencing", "fencing token périmé")
        return self.record_question("agent", job_id, runtime, title, question, options)

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
        telemetry: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not isinstance(event_id, str) or not 8 <= len(event_id) <= 64:
            raise BrokerError("invalid_event", "event_id invalide")
        now = self.clock()
        tele = self._telemetry_values(telemetry or {})
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
            lease = self._execution_lease(db, row, tele)
            if lease is None:
                return {"duplicate": False, "ignored": True, "cancel": False}
            chunks, chars, truncated = int(row["output_chunks"]), int(row["output_chars"]), int(row["output_truncated"])
            prev_step = chars // P.OUTPUT_PROGRESS_STEP_CHARS
            got_output, text = False, ""
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
                    lease,
                    new_activity,
                    str(runtime_session_id)[:128] if runtime_session_id else None,
                    chunks,
                    chars,
                    truncated,
                    job_id,
                ),
            )
            # Détection automatique d'un blocage qui demande une action humaine
            # (quota épuisé, authentification expirée) sur la sortie de l'agent :
            # signatures EXACTES, jamais un match naïf. Le broker le fait ici pour
            # que tous les runtimes en bénéficient sans MAJ du runner sur le PC.
            hit = P.match_pause_reason(text) if got_output else None
            if hit and not _paused(row):
                reason, sign = hit
                db.execute(
                    """UPDATE jobs SET paused_at=?, pause_reason=?, pause_expires=?,
                         pause_source=?, pause_count=pause_count + 1,
                         pause_note=COALESCE(pause_note, ?)
                       WHERE id=?""",
                    (now, reason, now + P.PAUSE_DEFAULT_S, P.PAUSE_SRC_RUNNER,
                     f"détecté dans la sortie de l'agent : {sign}", job_id),
                )
                self._emit(db, job_id, P.EV_PAUSED,
                           f"{P.PAUSE_MESSAGES[reason]} (détecté automatiquement : {sign})")
            elif _paused(row) and (got_output or new_activity) and not hit:
                # L'agent reparle et ce qu'il dit n'est PAS un nouveau blocage :
                # l'utilisateur a fait ce qu'il avait à faire, la pause se lève.
                self._resume(db, row, P.PAUSE_SRC_RUNNER, "l'agent a repris son activité")
            if got_output and chars // P.OUTPUT_PROGRESS_STEP_CHARS > prev_step:
                self._emit(db, job_id, P.EV_OUTPUT_PROGRESS, f"{chars} caractères reçus")
            if new_activity and new_activity != (row["last_activity"] or None):
                self._emit(db, job_id, P.EV_ACTIVITY, new_activity)
            # Télémétrie piggyback (le runner joint son snapshot à chaque flush) :
            # la sortie et l'état processus restent cohérents même si le job vit
            # moins d'un intervalle de heartbeat.
            self._store_telemetry(db, job_id, tele)
            return {"duplicate": False, "cancel": bool(row["cancel_requested"])}

    # --------------------------------------------- pause humaine (signal de vie)
    def _pause_block(self, row, now: float | None = None) -> dict[str, Any] | None:
        """Bloc `human_action_required` lisible, ou None si rien n'attend l'humain.

        Deux cas, une seule forme : le job est en pause (vivant, silence voulu),
        ou le job est terminal sur un blocage humain constaté (quota épuisé,
        authentification expirée) — auquel cas ce n'est PAS une panne à annoncer
        comme telle, mais une action à faire puis un retry explicite."""
        now = self.clock() if now is None else now
        if _paused(row):
            reason = row["pause_reason"] or P.PAUSE_MANUAL
            expires = row["pause_expires"]
            return {
                "required": True,
                "reason": reason,
                "message": P.PAUSE_MESSAGES.get(reason, "action de l'utilisateur attendue"),
                "note": row["pause_note"],
                "since": _iso(row["paused_at"]),
                "paused_for_s": round(now - float(row["paused_at"]), 1),
                "expires_at": _iso(expires),
                "expires_in_s": round(float(expires) - now, 1) if expires else None,
                "declared_by": row["pause_source"],
                "job_is_alive": True,
                "resume_with": "agent_job_resume",
                "note_for_caller": (
                    "l'agent n'est PAS en panne : le silence est voulu. Dites-le à "
                    "l'utilisateur, puis reprenez le suivi (agent_job_wait) une fois "
                    "l'action faite."
                ),
            }
        try:
            blocker = row["blocker_reason"]
        except (IndexError, KeyError):
            blocker = None
        if blocker:
            return {
                "required": True,
                "reason": blocker,
                "message": P.PAUSE_MESSAGES.get(blocker, "action de l'utilisateur attendue"),
                "note": row["blocker_sign"],
                "since": _iso(row["finished_at"] or row["updated_at"]),
                "job_is_alive": False,
                "resume_with": "agent_mission_retry",
                "note_for_caller": (
                    "la tentative s'est arrêtée sur un blocage qui demande une action "
                    "humaine, pas sur un défaut de l'agent : ne l'annoncez pas comme un "
                    "échec technique. Une fois l'action faite, relancez explicitement."
                ),
            }
        return None

    def pause_job(
        self,
        job_id: str,
        reason: str = P.PAUSE_MANUAL,
        note: str | None = None,
        expected_s: int | None = None,
        source: str = P.PAUSE_SRC_HUMAN,
    ) -> dict[str, Any]:
        """Déclare une attente HUMAINE sur un job actif (idempotent).

        Effet : suspend les trois comptes à rebours du broker (recovery vers
        `lost`, détection de stall, timeout dur) dans la limite de `expected_s`
        (borné à P.PAUSE_MAX_S), et rend le silence lisible
        (`execution_health=waiting_for_human`). Aucune observation n'est
        falsifiée : la télémétrie reste ce qu'elle est."""
        if reason not in P.PAUSE_REASONS:
            raise BrokerError("invalid_pause", f"raison inconnue (attendu : {', '.join(P.PAUSE_REASONS)})")
        if source not in P.PAUSE_SOURCES:
            raise BrokerError("invalid_pause", "source de pause inconnue")
        window = P.PAUSE_DEFAULT_S if expected_s is None else int(expected_s)
        window = max(60, min(window, P.PAUSE_MAX_S))
        clean = P.clip(redact(note.strip()), P.MAX_PAUSE_NOTE_CHARS) if note and note.strip() else None
        now = self.clock()
        with self._tx() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return {"job_id": job_id, "result": "unknown_job"}
            if row["state"] in P.TERMINAL:
                return {"job_id": job_id, "result": "already_finished", "state": row["state"]}
            already = _paused(row)
            db.execute(
                """UPDATE jobs SET paused_at=COALESCE(paused_at, ?), pause_reason=?,
                     pause_note=COALESCE(?, pause_note), pause_expires=?, pause_source=?,
                     pause_count=pause_count + ?, updated_at=?,
                     stall_suspect_at=NULL, stall_at=NULL
                   WHERE id=?""",
                (now, reason, clean, now + window, source, 0 if already else 1, now, job_id),
            )
            if not already:
                self._emit(
                    db, job_id, P.EV_PAUSED,
                    f"{P.PAUSE_MESSAGES.get(reason, reason)} (source {source})" + (f" : {clean}" if clean else ""),
                )
            fresh = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return {
                "job_id": job_id,
                "result": "already_paused" if already else "paused",
                "state": fresh["state"],
                "human_action_required": self._pause_block(fresh, now),
            }

    def _resume(self, db, row, source: str, detail: str) -> None:
        """Lève la pause et redémarre proprement les comptes à rebours : une
        observation négative antérieure repart de MAINTENANT (jamais
        rétroactivement, sinon l'attente humaine compterait contre le job)."""
        now = self.clock()
        recovery = now if row["proc_alive"] == 0 else None
        db.execute(
            """UPDATE jobs SET paused_at=NULL, pause_reason=NULL, pause_expires=NULL,
                 pause_source=NULL, resumed_at=?, updated_at=?, recovery_since=?,
                 lease_expires=MAX(COALESCE(lease_expires, 0), ?),
                 stall_suspect_at=NULL, stall_at=NULL
               WHERE id=?""",
            (now, now, recovery, now + P.LEASE_S, row["id"]),
        )
        self._emit(db, row["id"], P.EV_RESUMED, f"{detail} (source {source})")

    def resume_job(self, job_id: str, source: str = P.PAUSE_SRC_HUMAN, note: str | None = None) -> dict[str, Any]:
        """Lève une pause humaine (idempotent). L'utilisateur a fait ce qu'il
        avait à faire : le suivi normal reprend."""
        if source not in P.PAUSE_SOURCES:
            raise BrokerError("invalid_pause", "source de pause inconnue")
        clean = P.clip(redact(note.strip()), P.MAX_PAUSE_NOTE_CHARS) if note and note.strip() else None
        with self._tx() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return {"job_id": job_id, "result": "unknown_job"}
            if not _paused(row):
                return {"job_id": job_id, "result": "not_paused", "state": row["state"]}
            self._resume(db, row, source, clean or "reprise demandée")
            fresh = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return {"job_id": job_id, "result": "resumed", "state": fresh["state"]}

    def liveness(self, job_id: str) -> dict[str, Any] | None:
        """SIGNAL DE VIE : « est-ce que ça avance, et depuis quand ? »

        Réponse fondée UNIQUEMENT sur des observations datées (heartbeat runner,
        télémétrie processus, sortie, événements). `verdict` résume, `evidence`
        énumère les preuves avec leur âge : rien n'est inventé, un signal non
        observé est absent de la liste au lieu d'être supposé bon."""
        now = self.clock()
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return None
            hb = self._db.execute("SELECT last_seen FROM runners WHERE id=?", (row["runner_id"],)).fetchone()
        last_seen = hb["last_seen"] if hb else None
        evidence: list[dict[str, Any]] = []

        def add(signal: str, ts: float | None, **extra: Any) -> None:
            if ts is None:
                return
            evidence.append({"signal": signal, "at": _iso(ts), "age_s": round(now - float(ts), 1), **extra})

        add("runner_heartbeat", last_seen, runner_id=row["runner_id"])
        add("process_telemetry", row["telemetry_at"],
            alive=None if row["proc_alive"] is None else bool(row["proc_alive"]), pid=row["proc_pid"])
        add("agent_output", row["last_output_at"], chars=row["output_chars"])
        add("job_event", row["last_event_at"], activity=row["last_activity"])
        freshest = min((e["age_s"] for e in evidence), default=None)
        human = self._pause_block(row, now)
        state = row["state"]
        if state in P.TERMINAL:
            verdict, keep_waiting = "finished", False
        elif human:
            verdict, keep_waiting = "waiting_for_human", False
        elif last_seen is None or now - float(last_seen) > P.ONLINE_WINDOW_S:
            verdict, keep_waiting = "lost_contact", True
        elif state in (P.QUEUED, P.CLAIMED, P.STARTING):
            verdict, keep_waiting = "starting", True
        elif freshest is None:
            verdict, keep_waiting = "unknown", True
        else:
            verdict, keep_waiting = "working", True
        messages = {
            "working": "l'agent travaille",
            "starting": "l'agent démarre",
            "waiting_for_human": (human or {}).get("message", "action de l'utilisateur attendue"),
            "lost_contact": "plus de signe du PC runner",
            "unknown": "aucun signal daté encore observé",
            "finished": f"job terminé ({state})",
        }
        message = messages[verdict]
        if verdict in ("working", "starting") and freshest is not None:
            message += f" ; dernier signe de vie il y a {freshest:.0f} s"
        return {
            "job_id": job_id,
            "state": state,
            "alive": verdict in ("working", "starting", "waiting_for_human"),
            "verdict": verdict,
            "message": message,
            "freshest_signal_age_s": freshest,
            "evidence": evidence,
            "keep_waiting": keep_waiting,
            "next_poll_after_s": FOLLOW_WAIT_S,
            "human_action_required": human,
            "execution_health": self._execution_health(row, last_seen, now),
        }

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
                "display_title": P.display_title(r["prompt"], fallback=f"job {r['id'][:8]}"),
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
        relance ni n'annule jamais seule. Un `lost` garde 'issue inconnue' avec les
        couches broker/runner/process pour diagnostiquer la cause observable."""
        now = self.clock()
        stats = {"requeued": 0, "lost": 0, "timeout_cancel": 0, "failed": 0, "stalled": 0,
                 "suspected_stall": 0, "pause_expired": 0}
        with self._tx() as db:
            # Une pause humaine est bornée : passé son échéance, les comptes à
            # rebours repartent de MAINTENANT (jamais rétroactivement — l'attente
            # de l'utilisateur ne doit pas compter contre le job).
            for row in db.execute(
                "SELECT * FROM jobs WHERE paused_at IS NOT NULL AND pause_expires IS NOT NULL AND pause_expires <= ?",
                (now,),
            ).fetchall():
                reason = row["pause_reason"] or P.PAUSE_MANUAL
                self._resume(db, row, "broker", f"pause expirée sans reprise ({reason})")
                self._emit(db, row["id"], P.EV_PAUSE_EXPIRED,
                           f"aucune reprise après la fenêtre de pause ({reason}) : supervision normale rétablie")
                stats["pause_expired"] += 1
            for row in db.execute(
                "SELECT * FROM jobs WHERE state IN ('starting','running') AND paused_at IS NULL "
                "AND recovery_since IS NOT NULL AND recovery_since + ? <= ?", (P.PROCESS_RECOVERY_S, now),
            ).fetchall():
                self._execution_lease(db, row)
                stats["lost"] += 1
            for row in db.execute(
                "SELECT id, state, attempt, cancel_requested FROM jobs WHERE state IN ('claimed','starting','running') AND lease_expires < ?",
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
                else:
                    self._set_terminal(
                        db, row["id"], row["state"], P.LOST, "reaper",
                        error="bail expiré (runner muet) après lancement : issue inconnue, non relancé",
                    )
                    stats["lost"] += 1
            # Stalls : processus vivant (télémétrie) + silence d'activité/output.
            # Edge-triggered via stall_suspect_at/stall_at (réarmés à la prochaine activité).
            for row in db.execute(
                """SELECT id, state, proc_alive, last_event_at, started_at, created_at,
                          stall_suspect_at, stall_at, lease_expires
                   FROM jobs WHERE state IN ('starting','running') AND paused_at IS NULL
                         AND lease_expires >= ? AND proc_alive = 1""",
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
                "SELECT id FROM jobs WHERE state='running' AND cancel_requested=0 AND paused_at IS NULL "
                "AND started_at + timeout_s + 120 < ?",
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
            a = db.execute(
                "DELETE FROM infra_alerts WHERE last_at < ?", (now - P.ALERT_RETENTION_S,)
            ).rowcount
            expired = db.execute(
                "UPDATE pending_questions SET status='expired' WHERE status='open' AND expires_at <= ?",
                (now,),
            ).rowcount
            q = db.execute(
                "DELETE FROM pending_questions WHERE status IN ('answered','expired') "
                "AND COALESCE(answered_at, expires_at, 0) < ?",
                (now - Retention().output_s,),
            ).rowcount
        return {"prompts": p, "output_chunks": o, "jobs": len(old), "alerts": a, "questions": q + expired}

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
                "display_title": P.display_title(row["objective"], fallback=f"mission {row['id'][:8]}"),
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
        needs_validation/incomplete/blocked, tentative précédente terminale,
        attempts < max. `blocked` est retryable parce que le blocage est une action
        humaine (nouvelle clé d'API, reconnexion) : une fois faite, la relance est
        le geste normal. Jamais de relance aveugle pour autant : l'appelant MCP
        décide après examen du journal."""
        with self._tx() as db:
            m = db.execute("SELECT * FROM missions WHERE id=?", (mission_id,)).fetchone()
            if m is None:
                raise BrokerError("unknown_mission", "mission inconnue")
            if m["state"] not in (P.MISSION_NEEDS_VALIDATION, P.MISSION_INCOMPLETE, P.MISSION_BLOCKED):
                raise BrokerError("mission_not_retryable", f"mission {m['state']} : retry refusé")
            if int(m["attempts"]) >= int(m["max_attempts"]):
                raise BrokerError("max_attempts_reached", "plus de tentatives autorisées")
            cur = db.execute("SELECT state FROM jobs WHERE id=?", (m["current_job_id"],)).fetchone() if m["current_job_id"] else None
            if cur is None or cur["state"] not in P.TERMINAL:
                raise BrokerError("attempt_still_active", "la tentative en cours n'est pas terminée")
            job_spec = db.execute(
                "SELECT runner_id, runtime, workspace_id, mode, timeout_s, error FROM jobs WHERE id=?", (m["current_job_id"],)
            ).fetchone()
            prev_corrupted = bool(job_spec and isinstance(job_spec["error"], str)
                                  and job_spec["error"].startswith(P.SESSION_CORRUPTED_PREFIX))
            if prev_corrupted:
                # Anti-boucle : N corruptions consécutives sur la même mission
                # => intervention humaine, pas une nouvelle tentative aveugle.
                chain = db.execute(
                    """SELECT j.error FROM mission_attempts a JOIN jobs j ON j.id = a.job_id
                       WHERE a.mission_id=? ORDER BY a.attempt_no DESC LIMIT ?""",
                    (mission_id, P.MAX_CONSECUTIVE_CORRUPTIONS),
                ).fetchall()
                if (len(chain) == P.MAX_CONSECUTIVE_CORRUPTIONS and all(
                    isinstance(r["error"], str) and r["error"].startswith(P.SESSION_CORRUPTED_PREFIX) for r in chain
                )):
                    raise BrokerError(
                        "session_corruption_loop",
                        f"{P.MAX_CONSECUTIVE_CORRUPTIONS} sessions corrompues d'affilée : "
                        "corriger la cause (config/plugin/auth opencode) avant tout retry",
                    )
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
            if prev_corrupted:
                # Nouvelle session propre, une seule fois par retry : handoff
                # minimal durable (objectif + job précédent + prochaine action),
                # jamais le transcript corrompu.
                self._emit(
                    db, job["job_id"], P.EV_SESSION_RECREATED,
                    f"session corrompue abandonnée (job {m['current_job_id']}) ; "
                    f"reprendre l'objectif « {P.display_title(m['objective'], fallback='mission')} » "
                    "via progress et fichiers concernés",
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

    def _mission_on_job_terminal(self, db, job_id: str, job_state: str, blocked: bool = False) -> None:
        """Hook : un job terminal fait progresser sa mission, sans jamais valider.
        exit 0 -> needs_validation ; blocage humain (quota, auth) -> blocked, qui
        dit « il manque une action de l'utilisateur », pas « l'agent a échoué » ;
        sinon -> incomplete. Pas de retry automatique dans aucun cas."""
        m = db.execute(
            "SELECT id, state FROM missions WHERE current_job_id=? AND state=?", (job_id, P.MISSION_EXECUTING)
        ).fetchone()
        if m is None:
            return
        if job_state == P.COMPLETED:
            nxt = P.MISSION_NEEDS_VALIDATION
        elif blocked:
            nxt = P.MISSION_BLOCKED
        else:
            nxt = P.MISSION_INCOMPLETE
        db.execute(
            "UPDATE missions SET state=?, updated_at=? WHERE id=?", (nxt, self.clock(), m["id"])
        )
        log.info("mission_progress mission_id=%s job %s -> %s", m["id"], job_state, nxt)

    # ------------------------------------------------------- alertes infra
    @staticmethod
    def _alert_fingerprint(source: str, service: str, title: str) -> str:
        h = hashlib.sha256()
        h.update(source.encode() + b"\x00" + service.encode() + b"\x00" + title.encode())
        return h.hexdigest()

    def record_alert(
        self,
        source: str,
        service: str,
        severity: str,
        title: str,
        detail: str | None = None,
        fingerprint: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Persiste une alerte sortante (ingesteurs locaux du VPS uniquement).

        Déduplication anti-spam : même empreinte revue dans
        ALERT_DEDUP_WINDOW_S et non résolue => `occurrences` + 1 (avec
        escalade de sévérité et détail rafraîchi), pas de nouvelle ligne.
        Retourne (alerte, created)."""
        if source not in P.ALERT_SOURCES:
            raise BrokerError("invalid_source", f"source : {' | '.join(P.ALERT_SOURCES)}")
        if not isinstance(service, str) or not service.strip() or len(service) > P.MAX_ALERT_SERVICE_CHARS:
            raise BrokerError("invalid_service", f"service requis (1..{P.MAX_ALERT_SERVICE_CHARS} car.)")
        if severity not in P.ALERT_SEVERITIES:
            raise BrokerError("invalid_severity", f"sévérité : {' | '.join(P.ALERT_SEVERITIES)}")
        if not isinstance(title, str) or not title.strip() or len(title) > P.MAX_ALERT_TITLE_CHARS:
            raise BrokerError("invalid_title", f"titre requis (1..{P.MAX_ALERT_TITLE_CHARS} car.)")
        service = service.strip()
        title = title.strip()
        if fingerprint is not None and (
            not isinstance(fingerprint, str) or not fingerprint.strip() or len(fingerprint) > 128
        ):
            raise BrokerError("invalid_fingerprint", "empreinte 1..128 car.")
        fp = fingerprint.strip() if isinstance(fingerprint, str) and fingerprint.strip() else self._alert_fingerprint(source, service, title)
        clean_detail = P.clip(redact(detail), P.MAX_ALERT_DETAIL_CHARS) if detail else None
        now = self.clock()
        with self._tx() as db:
            row = db.execute(
                "SELECT * FROM infra_alerts WHERE fingerprint=? ORDER BY last_at DESC LIMIT 1", (fp,)
            ).fetchone()
            if (
                row is not None
                and row["state"] != P.ALERT_RESOLVED
                and float(row["last_at"]) >= now - P.ALERT_DEDUP_WINDOW_S
            ):
                sev = row["severity"]
                if P.ALERT_SEVERITIES.index(severity) > P.ALERT_SEVERITIES.index(sev):
                    sev = severity
                db.execute(
                    "UPDATE infra_alerts SET occurrences=occurrences+1, last_at=?, severity=?, detail=? WHERE id=?",
                    (now, sev, clean_detail if clean_detail is not None else row["detail"], row["id"]),
                )
                log.info("alert_dedup alert_id=%s fp=%s occurrences=%d", row["id"], fp[:12], int(row["occurrences"]) + 1)
                alert_id, created = row["id"], False
            else:
                alert_id, created = str(uuid.uuid4()), True
                db.execute(
                    """INSERT INTO infra_alerts(id, source, service, severity, state, title, detail,
                         fingerprint, occurrences, first_at, last_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (alert_id, source, service, severity, P.ALERT_ACTIVE, title, clean_detail,
                     fp, 1, now, now),
                )
                log.info("alert_recorded alert_id=%s source=%s severity=%s service=%s", alert_id, source, severity, service)
            db.execute(
                "DELETE FROM infra_alerts WHERE id NOT IN "
                "(SELECT id FROM infra_alerts ORDER BY last_at DESC LIMIT ?)",
                (P.MAX_ALERTS,),
            )
        return self.get_alert(alert_id) or {"error": "internal", "alert_id": alert_id}, created

    @staticmethod
    def _alert_compact(row) -> dict[str, Any]:
        return {
            "alert_id": row["id"],
            "source": row["source"],
            "service": row["service"],
            "severity": row["severity"],
            "state": row["state"],
            "title": row["title"],
            "fingerprint": row["fingerprint"],
            "occurrences": row["occurrences"],
            "first_at": _iso(row["first_at"]),
            "last_at": _iso(row["last_at"]),
        }

    def list_alerts(
        self,
        source: str | None = None,
        severity: str | None = None,
        state: str | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Vue compacte filtrable (jamais le détail complet : voir get_alert)."""
        if source is not None and source not in P.ALERT_SOURCES:
            raise BrokerError("invalid_source", f"source : {' | '.join(P.ALERT_SOURCES)}")
        if severity is not None and severity not in P.ALERT_SEVERITIES:
            raise BrokerError("invalid_severity", f"sévérité : {' | '.join(P.ALERT_SEVERITIES)}")
        if state is not None and state not in P.ALERT_STATES:
            raise BrokerError("invalid_state", f"état : {' | '.join(P.ALERT_STATES)}")
        limit = max(1, min(int(limit or 20), P.MAX_ALERT_LIST))
        # SQL statique (pas de concaténation : exigence semgrep) ; chaque filtre
        # optionnel est neutralisé par son doublon NULL.
        query = (
            "SELECT * FROM infra_alerts "
            "WHERE (? IS NULL OR source=?) AND (? IS NULL OR severity=?) AND (? IS NULL OR state=?) "
            "AND (? IS NULL OR last_at>=?) AND (? IS NULL OR last_at<=?) "
            "ORDER BY last_at DESC LIMIT ?"
        )
        params: list[Any] = [source, source, severity, severity, state, state, since, since, until, until, limit]
        with self._lock:
            rows = self._db.execute(query, params).fetchall()
            alerts = [self._alert_compact(r) for r in rows]
        return {"alerts": alerts, "count": len(alerts)}

    def get_alert(self, alert_id: str) -> dict[str, Any] | None:
        """Alerte complète, détail borné (redacted). None si inconnue."""
        with self._lock:
            row = self._db.execute("SELECT * FROM infra_alerts WHERE id=?", (alert_id,)).fetchone()
            if row is None:
                return None
            out = self._alert_compact(row)
            out["detail"] = row["detail"]
            return out

    def set_alert_state(self, alert_id: str, state: str) -> dict[str, Any]:
        """Acquittement/résolution (CLI locale d'exploitation, pas le MCP)."""
        if state not in (P.ALERT_ACKED, P.ALERT_RESOLVED):
            raise BrokerError("invalid_state", "état : acked | resolved")
        now = self.clock()
        with self._tx() as db:
            row = db.execute("SELECT id FROM infra_alerts WHERE id=?", (alert_id,)).fetchone()
            if row is None:
                raise BrokerError("unknown_alert", "alerte inconnue")
            db.execute("UPDATE infra_alerts SET state=?, last_at=? WHERE id=?", (state, now, alert_id))
            log.info("alert_state alert_id=%s state=%s", alert_id, state)
        return self.get_alert(alert_id) or {"error": "internal", "alert_id": alert_id}

    # ------------------------------------------------- questions en attente
    @staticmethod
    def _question_fingerprint(session_ref: str, question: str) -> str:
        h = hashlib.sha256()
        h.update(session_ref.encode() + b"\x00" + question.encode())
        return h.hexdigest()

    def record_question(
        self,
        origin: str,
        session_ref: str,
        runtime: str,
        title: str,
        question: str,
        options: list[str] | None = None,
        notify_after_s: int | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Enregistre un état waiting_for_user explicite (jamais de regex sur `?`).

        Déduplication : même (session, question) ouverte et non expirée =>
        retourne l'existante (UN seul message Photon). Retourne (question, created)."""
        if origin not in ("agent", "chatgpt-web", "mission"):
            raise BrokerError("invalid_origin", "origin : agent | chatgpt-web | mission")
        if not isinstance(session_ref, str) or not session_ref.strip() or len(session_ref) > 128:
            raise BrokerError("invalid_session_ref", "session_ref requise (1..128 car.)")
        if runtime not in (*P.RUNTIMES, "chatgpt-web"):
            raise BrokerError("invalid_runtime", "runtime inconnu")
        if not isinstance(question, str) or not question.strip() or len(question) > P.MAX_QUESTION_CHARS:
            raise BrokerError("invalid_question", f"question requise (1..{P.MAX_QUESTION_CHARS} car.)")
        clean_opts: list[str] = []
        for o in options or []:
            if isinstance(o, str) and o.strip():
                clean_opts.append(o.strip()[: P.MAX_QUESTION_OPTION_CHARS])
            if len(clean_opts) >= P.MAX_QUESTION_OPTIONS:
                break
        session_ref = session_ref.strip()
        title = P.display_title(title, fallback=f"session {session_ref[:8]}")
        clean_q = P.clip(redact(question.strip()), P.MAX_QUESTION_CHARS) or ""
        fp = self._question_fingerprint(session_ref, clean_q)
        now = self.clock()
        wait = P.QUESTION_NOTIFY_AFTER_S if notify_after_s is None else max(1, min(int(notify_after_s), 3_600))
        with self._tx() as db:
            row = db.execute(
                "SELECT * FROM pending_questions WHERE fingerprint=? AND status='open' AND expires_at>? "
                "ORDER BY created_at DESC LIMIT 1",
                (fp, now),
            ).fetchone()
            if row is not None:
                log.info("question_dedup question_id=%s session=%s", row["id"], session_ref)
                return self._question_view(row), False
            qid = "q-" + uuid.uuid4().hex[:12]
            db.execute(
                """INSERT INTO pending_questions(id, origin, session_ref, runtime, title, question,
                     options_json, fingerprint, status, notify_after_s, notify_status, created_at, expires_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (qid, origin, session_ref, runtime, title, clean_q, json.dumps(clean_opts, ensure_ascii=False),
                 fp, P.Q_OPEN, wait, P.Q_NOTIFY_PENDING, now, now + P.QUESTION_EXPIRY_S),
            )
            log.info("question_recorded question_id=%s session=%s runtime=%s", qid, session_ref, runtime)
        return self.get_question(qid) or {"error": "internal", "question_id": qid}, True

    @staticmethod
    def _question_view(row, with_answer: bool = False) -> dict[str, Any]:
        out = {
            "question_id": row["id"],
            "origin": row["origin"],
            "session_ref": row["session_ref"],
            "runtime": row["runtime"],
            "title": row["title"],
            "question": row["question"],
            "options": json.loads(row["options_json"]),
            "status": row["status"],
            "notify_status": row["notify_status"],
            "notified_at": _iso(row["notified_at"]),
            "created_at": _iso(row["created_at"]),
            "expires_at": _iso(row["expires_at"]),
        }
        if with_answer:
            out["answer"] = row["answer"]
            out["answered_at"] = _iso(row["answered_at"])
            out["answer_from"] = row["answer_from"]
        return out

    def get_question(self, question_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM pending_questions WHERE id=?", (question_id,)).fetchone()
            return self._question_view(row, with_answer=True) if row is not None else None

    def list_questions(
        self, status: str | None = None, origin: str | None = None, limit: int = 20
    ) -> dict[str, Any]:
        if status is not None and status not in P.QUESTION_STATES:
            raise BrokerError("invalid_state", f"état : {' | '.join(P.QUESTION_STATES)}")
        if origin is not None and origin not in ("agent", "chatgpt-web", "mission"):
            raise BrokerError("invalid_origin", "origin : agent | chatgpt-web | mission")
        limit = max(1, min(int(limit or 20), 100))
        # SQL statique (exigence semgrep) ; filtres neutralisés par doublon NULL.
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM pending_questions "
                "WHERE (? IS NULL OR status=?) AND (? IS NULL OR origin=?) "
                "ORDER BY created_at DESC LIMIT ?",
                (status, status, origin, origin, limit),
            ).fetchall()
            out = [self._question_view(r) for r in rows]
        return {"questions": out, "count": len(out)}

    def due_questions(self, now: float | None = None) -> list[dict[str, Any]]:
        """Questions dues pour Photon : ouvertes, jamais notifiées, délai dépassé,
        non expirées. Le dispatcher envoie UN message par question due."""
        now = self.clock() if now is None else float(now)
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM pending_questions WHERE status='open' AND notified_at IS NULL "
                "AND created_at + notify_after_s <= ? AND expires_at > ? ORDER BY created_at",
                (now, now),
            ).fetchall()
            return [self._question_view(r) for r in rows]

    def mark_notified(self, question_id: str, notify_status: str) -> dict[str, Any] | None:
        """Marque la notification (sent = message parti ; deferred = sender
        indisponible, raison honnête visible ; failed = échec d'envoi)."""
        if notify_status not in P.QUESTION_NOTIFY_STATES:
            raise BrokerError("invalid_notify_status", "notify : pending | sent | deferred | failed")
        now = self.clock()
        with self._tx() as db:
            row = db.execute("SELECT id, notified_at FROM pending_questions WHERE id=?", (question_id,)).fetchone()
            if row is None:
                raise BrokerError("unknown_question", "question inconnue")
            if row["notified_at"] is not None:
                return self.get_question(question_id)  # idempotent : un seul message
            db.execute(
                "UPDATE pending_questions SET notified_at=?, notify_status=? WHERE id=?",
                (now, notify_status, question_id),
            )
            log.info("question_notified question_id=%s notify=%s", question_id, notify_status)
        return self.get_question(question_id)

    def answer_question(self, question_id: str, answer: str, answer_from: str) -> dict[str, Any]:
        """Réponse single-use routée à la session émettrice (correlation_id +
        expiration). `answer_from` = expéditeur allowlisté vérifié par l'appelant
        (Photon/Hermes) ; stocké pour audit. Jamais une commande shell."""
        if not isinstance(answer, str) or not answer.strip() or len(answer) > P.MAX_ANSWER_CHARS:
            raise BrokerError("invalid_answer", f"réponse requise (1..{P.MAX_ANSWER_CHARS} car.)")
        if not isinstance(answer_from, str) or not answer_from.strip() or len(answer_from) > 128:
            raise BrokerError("invalid_answer_from", "expéditeur requis (allowlist Photon)")
        now = self.clock()
        with self._lock:
            row = self._db.execute("SELECT * FROM pending_questions WHERE id=?", (question_id,)).fetchone()
            if row is None:
                raise BrokerError("unknown_question", "question inconnue")
            status, expires_at, session_ref = row["status"], float(row["expires_at"]), row["session_ref"]
        if status != P.Q_OPEN:
            raise BrokerError("question_closed", f"question {status} (replay refusé)")
        if expires_at <= now:
            # Expirée : marquer en transaction validée SÉPARÉE (un raise
            # annulerait la tx courante ; pas de tx imbriquée SQLite).
            with self._tx() as db_exp:
                db_exp.execute("UPDATE pending_questions SET status='expired' WHERE id=?", (question_id,))
            raise BrokerError("question_expired", "question expirée")
        clean = P.clip(redact(answer.strip()), P.MAX_ANSWER_CHARS) or ""
        with self._tx() as db:
            cur = db.execute(
                "UPDATE pending_questions SET status='answered', answer=?, answered_at=?, answer_from=? "
                "WHERE id=? AND status='open'",
                (clean, now, answer_from.strip(), question_id),
            )
            if cur.rowcount != 1:
                raise BrokerError("question_closed", "question déjà traitée (replay refusé)")
            log.info("question_answered question_id=%s session=%s", question_id, session_ref)
        return self.get_question(question_id) or {"error": "internal", "question_id": question_id}

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
        tele_at = row["telemetry_at"]
        view = {
            "job_id": row["id"],
            "display_title": P.display_title(row["prompt"], fallback=f"job {row['id'][:8]}"),
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
            # --- supervision riche : état structuré d'exécution (null = non observé)
            # telemetry_at/age : None + télémétrie null = runner sans
            # instrumentation (vieux runner) ou pas encore observé ; jamais inventé.
            "runner_last_seen_at": _iso(runner_last_seen),
            "runner_heartbeat_age_s": hb_age,
            "telemetry_at": _iso(tele_at),
            "telemetry_age_s": round(now - tele_at, 1) if tele_at is not None else None,
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
            # Attente humaine : présent (non null) => l'agent n'est pas en panne,
            # il manque un geste de l'utilisateur. À dire tel quel à l'utilisateur.
            "human_action_required": self._pause_block(row, now),
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
        if _paused(row):
            # Le silence est VOULU : ne jamais le présenter comme un stall ni une
            # panne. Les couches runner_health/runtime_process_health continuent
            # d'exposer les faits bruts (rien n'est caché).
            return P.WAITING_FOR_HUMAN
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
        de WAIT_MAX_S. N'est PAS un fond de tâche : le réveil exige un tour actif.

        Sémantique (woke_by) :
        - job déjà terminal à l'appel => retour IMMÉDIAT `terminal` (jamais
          d'attente jusqu'au timeout sur un état qui ne bougera plus) ;
        - événements non vus (`last_seq > since_seq`) => retour IMMÉDIAT `event` ;
        - pendant l'attente : `terminal` (état terminal atteint), `state`
          (autre changement d'état), `change` (sortie/activité sans changement
          d'état), `event` (nouvel événement au-delà de since_seq) ;
        - sinon `timeout` à expiration du délai borné.
        - contrat de suivi : chaque retour porte `terminal` (job figé ou non),
          `should_continue`/`must_follow` (= non terminal), `next_tool`
          (`agent_job_wait` tant que non terminal, `agent_job_get` sinon),
          `since_seq`/`last_event_seq` (curseur : rappeler avec
          `since_seq=last_event_seq`) et `until='terminal'`. Un `timeout` non
          terminal impose de rappeler IMMÉDIATEMENT dans le même tour."""
        timeout_s = max(0.0, min(float(timeout_s), P.WAIT_MAX_S))
        since_seq = max(-1, int(since_seq))
        import time as _t

        def _snapshot(woke_by: str) -> dict[str, Any] | None:
            view = self.get_job(job_id, tail_chars=0)
            if view is None:
                return None
            seq = self._last_seq(job_id)
            human = view.get("human_action_required")
            out: dict[str, Any] = {
                "job_id": job_id,
                "state": view["state"],
                "woke_by": woke_by,
                "execution_health": view["execution_health"],
                "last_event_seq": seq,
            }
            out.update(follow_for_wait(view["state"], woke_by, seq, human))
            out["last_event_seq"] = seq  # curseur à réutiliser en since_seq
            # SIGNAL DE VIE : joint à CHAQUE retour, y compris un timeout. Un
            # timeout accompagné d'un signal frais dit « vivant, rappelle-moi »,
            # jamais « c'est mort » : c'est ce qui évite d'abandonner à tort.
            out["liveness"] = self.liveness(job_id)
            return out

        with self._lock:
            row = self._db.execute(
                "SELECT updated_at, state, paused_at FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if row is None:
                return None
            baseline_updated, baseline_state = float(row["updated_at"]), row["state"]
            baseline_paused = row["paused_at"]
            base_seq = self._event_seq(self._db, job_id)
            # Réveil immédiat : état terminal déjà atteint (ne bougera plus).
            if baseline_state in P.TERMINAL:
                return _snapshot("terminal")
            # Réveil immédiat : une action de l'utilisateur est attendue. Attendre
            # dans le vide pendant qu'il manque un geste humain est exactement ce
            # qui faisait conclure au time-out.
            if baseline_paused is not None:
                return _snapshot("paused")
            # Réveil immédiat : le client est en retard (événements non vus).
            if base_seq > since_seq:
                return _snapshot("event")
        deadline = _t.monotonic() + timeout_s
        woke_by = "timeout"
        while _t.monotonic() < deadline:
            _t.sleep(0.2)
            with self._lock:
                row = self._db.execute(
                    "SELECT updated_at, state, paused_at FROM jobs WHERE id=?", (job_id,)
                ).fetchone()
                if row is None:
                    return None
                seq = self._event_seq(self._db, job_id)
                if row["state"] != baseline_state:
                    woke_by = "terminal" if row["state"] in P.TERMINAL else "state"
                    break
                if (row["paused_at"] is None) != (baseline_paused is None):
                    woke_by = "paused" if row["paused_at"] is not None else "resumed"
                    break
                if float(row["updated_at"]) != baseline_updated or seq != base_seq:
                    woke_by = "change"
                    break
                if seq > since_seq:
                    woke_by = "event"
                    break
        return _snapshot(woke_by)

    def wait_for_mission(
        self, mission_id: str, since_seq: int = -1, timeout_s: float = P.WAIT_DEFAULT_S
    ) -> dict[str, Any] | None:
        """Attente bornée sur la tentative COURANTE d'une mission (même sémantique
        que `wait_for_change`, jamais plus de WAIT_MAX_S). Retourne l'état mission
        + job + suivi : `executing` non terminal => rappeler ; `needs_validation`
        / `incomplete` => inspecter puis `agent_mission_validate` (jamais confondre
        `completed` process avec `validated` mission). Pas de callback de fond."""
        m = self.get_mission(mission_id)
        if m is None:
            return None
        job_id = m["current_job_id"]
        if not job_id:
            return {"mission_id": mission_id, "error": "no_attempt", **m}
        w = self.wait_for_change(job_id, since_seq, timeout_s)
        if w is None:
            return None
        m2 = self.get_mission(mission_id) or m
        state = m2["state"]
        human = w.get("human_action_required")
        pursuing = state == P.MISSION_EXECUTING and not w["terminal"] and not human
        if pursuing:
            nxt = "agent_mission_wait"
        elif state in (P.MISSION_NEEDS_VALIDATION, P.MISSION_INCOMPLETE, P.MISSION_BLOCKED):
            nxt = "agent_mission_validate"
        else:
            nxt = "agent_mission_get"
        out_human: dict[str, Any] = {}
        if human:
            out_human["human_action_required"] = human
            if not w["terminal"]:
                out_human["stop_reason"] = "waiting_for_human"
                out_human["resume_with"] = "agent_mission_wait"
        return {
            **out_human,
            "liveness": w.get("liveness"),
            "mission_id": mission_id,
            "mission_state": state,
            "job_id": job_id,
            "job_state": w["state"],
            "woke_by": w["woke_by"],
            "terminal": w["terminal"],
            "should_continue": pursuing,
            "must_follow": pursuing,
            "next_tool": nxt,
            "since_seq": w["since_seq"],
            "last_event_seq": w["last_event_seq"],
            "until": FOLLOW_UNTIL,
            "wait_timeout_s": FOLLOW_WAIT_S,
            "current_job_id": job_id,
        }

    def _last_seq(self, job_id: str) -> int:
        with self._lock:
            row = self._db.execute("SELECT COALESCE(MAX(seq), -1) AS m FROM job_events WHERE job_id=?", (job_id,)).fetchone()
            return int(row["m"])


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(ts)))
