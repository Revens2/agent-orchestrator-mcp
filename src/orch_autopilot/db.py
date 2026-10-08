"""Persistance `autopilot.db` (SQLite WAL), migrations versionnées et journal append-only.

Base distincte du broker : aucune atomicité inter-bases n'est prétendue. La cohérence
avec le broker passe par l'outbox (payload figé + clé d'idempotence) et la réconciliation.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from orch_protocol.redact import redact

MIGRATIONS: list[str] = [
    # v1 — schéma initial
    """
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE objectives (
        id TEXT PRIMARY KEY,
        client_key TEXT UNIQUE,
        title TEXT NOT NULL,
        goal TEXT NOT NULL,
        state TEXT NOT NULL,
        budget_json TEXT NOT NULL,
        spent_jobs INTEGER NOT NULL DEFAULT 0,
        spent_seconds INTEGER NOT NULL DEFAULT 0,
        version INTEGER NOT NULL DEFAULT 1,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
    );
    CREATE TABLE steps (
        id TEXT PRIMARY KEY,
        objective_id TEXT NOT NULL REFERENCES objectives(id),
        key TEXT NOT NULL,
        title TEXT NOT NULL,
        kind TEXT NOT NULL,
        workspace_id TEXT,
        prompt TEXT NOT NULL,
        timeout_s INTEGER NOT NULL,
        depends_json TEXT NOT NULL,
        expect_json TEXT NOT NULL,
        structural INTEGER NOT NULL DEFAULT 0,
        gate_state TEXT,
        cancel_requested INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL,
        attempt INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL,
        job_id TEXT,
        broker_state TEXT,
        result_text TEXT,
        evidence_json TEXT,
        rate_limit_waits INTEGER NOT NULL DEFAULT 0,
        first_rate_limit_at REAL,
        not_before REAL,
        last_error TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        UNIQUE(objective_id, key)
    );
    CREATE INDEX steps_state ON steps(state);
    CREATE TABLE events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        objective_id TEXT,
        step_id TEXT,
        at REAL NOT NULL,
        type TEXT NOT NULL,
        data_json TEXT NOT NULL
    );
    CREATE INDEX events_obj ON events(objective_id, id);
    CREATE TRIGGER events_no_update BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT, 'events_append_only'); END;
    CREATE TRIGGER events_no_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'events_append_only'); END;
    CREATE TABLE outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        objective_id TEXT,
        step_id TEXT,
        idempotency_key TEXT NOT NULL UNIQUE,
        payload TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at REAL NOT NULL,
        uncertain_since REAL,
        last_error TEXT,
        result_json TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
    );
    CREATE INDEX outbox_due ON outbox(state, next_attempt_at);
    CREATE TRIGGER outbox_payload_frozen BEFORE UPDATE OF payload, idempotency_key ON outbox
        BEGIN SELECT RAISE(ABORT, 'outbox_payload_frozen'); END;
    CREATE TABLE gate_decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        step_id TEXT NOT NULL,
        request_hash TEXT NOT NULL,
        question TEXT NOT NULL,
        answer TEXT,
        served_model TEXT,
        attested INTEGER NOT NULL,
        session_id TEXT,
        error TEXT,
        at REAL NOT NULL
    );
    CREATE TABLE consents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        action_hash TEXT NOT NULL UNIQUE,
        action_json TEXT NOT NULL,
        code_hash TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        expires_at REAL NOT NULL,
        created_at REAL NOT NULL,
        consumed_at REAL
    );
    CREATE TABLE budget_daily (day TEXT PRIMARY KEY, jobs INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE breakers (runtime TEXT PRIMARY KEY, consecutive INTEGER NOT NULL DEFAULT 0, open_until REAL);
    CREATE TRIGGER steps_terminal_immutable BEFORE UPDATE OF state ON steps
        WHEN OLD.state IN ('DONE','FAILED','CANCELLED') AND NEW.state != OLD.state
        BEGIN SELECT RAISE(ABORT, 'step_terminal_immutable'); END;
    CREATE TRIGGER objectives_terminal_immutable BEFORE UPDATE OF state ON objectives
        WHEN OLD.state IN ('DONE','FAILED','CANCELLED') AND NEW.state != OLD.state
        BEGIN SELECT RAISE(ABORT, 'objective_terminal_immutable'); END;
    """,
]

MAX_EVENT_DATA = 4_000


class AutopilotDB:
    def __init__(self, path: str | Path, clock: Callable[[], float] = time.time) -> None:
        self.path = str(path)
        self.clock = clock
        self._lock = threading.RLock()
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None, timeout=30)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    # ------------------------------------------------------------------ base
    def _migrate(self) -> None:
        with self._lock:
            has_meta = self._db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
            current = 0
            if has_meta:
                row = self._db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
                current = int(row["value"]) if row else 0
            for version, sql in enumerate(MIGRATIONS, start=1):
                if version <= current:
                    continue
                self._db.execute("BEGIN IMMEDIATE")
                try:
                    # instruction par instruction : executescript ferait un COMMIT implicite hors transaction
                    for stmt in _split_sql(sql):
                        self._db.execute(stmt)
                    self._db.execute(
                        "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (str(version),),
                    )
                    self._db.execute("COMMIT")
                except Exception:
                    self._db.execute("ROLLBACK")
                    raise

    def schema_version(self) -> int:
        row = self._db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        return int(row["value"]) if row else 0

    @contextlib.contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """Transaction IMMEDIATE sérialisée (un seul écrivain)."""
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            self._db.execute("COMMIT")

    def query(self, sql: str, args: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute(sql, args).fetchall()

    def one(self, sql: str, args: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._db.execute(sql, args).fetchone()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # ------------------------------------------------------------------ meta
    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.one("SELECT value FROM meta WHERE key=?", (key,))
        return row["value"] if row else default

    @staticmethod
    def set_meta(db: sqlite3.Connection, key: str, value: str) -> None:
        db.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value)
        )

    # ---------------------------------------------------------------- events
    def event(self, db: sqlite3.Connection, objective_id: str | None, step_id: str | None, type_: str, **data: Any) -> None:
        """Ajoute un événement (dans la transaction appelante). Données expurgées et bornées."""
        text = redact(json.dumps(data, ensure_ascii=False, default=str, sort_keys=True)) or "{}"
        if len(text) > MAX_EVENT_DATA:
            text = json.dumps({"truncated": True, "head": text[: MAX_EVENT_DATA - 100]}, ensure_ascii=False)
        db.execute(
            "INSERT INTO events(objective_id, step_id, at, type, data_json) VALUES(?,?,?,?,?)",
            (objective_id, step_id, self.clock(), type_, text),
        )

    def events(self, objective_id: str, after_id: int = 0, limit: int = 100) -> list[dict]:
        rows = self.query(
            "SELECT id, step_id, at, type, data_json FROM events WHERE objective_id=? AND id>? ORDER BY id LIMIT ?",
            (objective_id, after_id, max(1, min(limit, 500))),
        )
        return [
            {"id": r["id"], "step_id": r["step_id"], "at": r["at"], "type": r["type"], "data": json.loads(r["data_json"])}
            for r in rows
        ]


def _split_sql(script: str) -> list[str]:
    """Découpe un script en instructions, en gardant les corps de triggers (BEGIN ... END;) entiers."""
    out, buf = [], []
    for line in script.splitlines():
        if not line.strip():
            continue
        buf.append(line)
        joined = "\n".join(buf).strip()
        if not joined.endswith(";"):
            continue
        if "CREATE TRIGGER" in joined.upper() and not joined.upper().endswith("END;"):
            continue
        out.append(joined)
        buf = []
    if buf:
        out.append("\n".join(buf))
    return out
