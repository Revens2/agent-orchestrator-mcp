"""Superviseur déterministe : FSM des étapes, outbox, réconciliation, budgets, consentements.

Aucun LLM ne planifie ici : les étapes viennent de la spec soumise au cockpit. Le superviseur
n'appelle OpenCode (via le broker) que pour les étapes `read` / `local_write`, et le gate Opus
pour les décisions structurantes.
"""

from __future__ import annotations

import hmac
import json
import logging
import random
import secrets
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

import orch_autopilot as A
from orch_autopilot.broker import Broker, BrokerUnavailable, RuntimeForbidden
from orch_autopilot.db import AutopilotDB
from orch_autopilot.gate import Gate, GateResult, Notifier, UnavailableGate, is_attested
from orch_autopilot.policy import (
    Config,
    PolicyError,
    action_hash,
    backoff_s,
    build_prompt,
    canonical,
    idempotency_key,
    rate_limit_info,
    sha256,
    validate_objective,
    validate_output,
)
from orch_protocol.redact import redact

log = logging.getLogger("orch_autopilot")

MAX_RESULT_CHARS = 20_000
# Erreurs broker connues comme « rien n'a été créé » : retry sûr, worker indisponible.
WORKER_UNAVAILABLE = frozenset({"runtime_unavailable", "unknown_runner"})
CANCEL_OBJECTIVE, CANCEL_KILL = 1, 2
STEP_MUTABLE_COLUMNS = frozenset({
    "attempt", "job_id", "broker_state", "not_before", "cancel_requested", "result_text",
    "rate_limit_waits", "first_rate_limit_at",
})
RESOLUTIONS = {
    A.PENDING_GATE: ("approve_gate", "reject"),
    A.NEEDS_AUDIT: ("retry", "mark_done", "mark_failed"),
    A.NEEDS_EFFECT_AUDIT: ("retry", "mark_done", "mark_failed"),
    A.NEEDS_APPROVAL: ("approve", "reject"),
}


class AutopilotError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class Supervisor:
    def __init__(
        self,
        db: AutopilotDB,
        broker: Broker,
        cfg: Config,
        gate: Gate | None = None,
        notifier: Notifier | None = None,
        clock: Callable[[], float] = time.time,
        rng: random.Random | None = None,
    ) -> None:
        self.db = db
        self.broker = broker
        self.cfg = cfg
        self.gate = gate or UnavailableGate()
        self.notifier = notifier
        self.clock = clock
        self.rng = rng or random.Random()
        self.last_tick_at: float | None = None
        self._recover_outbox()

    # ================================================================= cockpit
    def submit(self, spec: dict, client_key: str | None = None) -> dict:
        try:
            norm = validate_objective(spec, self.cfg)
        except PolicyError as exc:
            raise AutopilotError(exc.code, exc.message) from exc
        spec_hash = sha256(canonical(norm))
        now = self.clock()
        with self.db.tx() as db:
            if client_key:
                row = db.execute("SELECT id, budget_json FROM objectives WHERE client_key=?", (client_key,)).fetchone()
                if row:
                    if json.loads(row["budget_json"]).get("_spec_hash") != spec_hash:
                        raise AutopilotError("idempotency_conflict", "client_key déjà utilisée avec une autre spec")
                    return {"objective_id": row["id"], "created": False}
            oid = "obj-" + uuid.uuid4().hex[:16]
            budget = {**norm["budget"], "_spec_hash": spec_hash}
            db.execute(
                "INSERT INTO objectives(id, client_key, title, goal, state, budget_json, created_at, updated_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (oid, client_key, norm["title"], norm["goal"], A.OBJ_ACTIVE, canonical(budget), now, now),
            )
            for s in norm["steps"]:
                db.execute(
                    "INSERT INTO steps(id, objective_id, key, title, kind, workspace_id, prompt, timeout_s, depends_json, "
                    "expect_json, structural, state, max_attempts, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"{oid}:{s['key']}", oid, s["key"], s["title"], s["kind"], s["workspace_id"], s["prompt"],
                     s["timeout_s"], canonical(s["depends_on"]), canonical(s["expect"]), int(s["structural"]),
                     A.PENDING, s["max_attempts"], now, now),
                )
            self.db.event(db, oid, None, "objective_submitted", title=norm["title"], steps=len(norm["steps"]))
        return {"objective_id": oid, "created": True}

    def set_objective_state(self, objective_id: str, action: str) -> dict:
        now = self.clock()
        with self.db.tx() as db:
            obj = db.execute("SELECT * FROM objectives WHERE id=?", (objective_id,)).fetchone()
            if obj is None:
                raise AutopilotError("unknown_objective", objective_id)
            if obj["state"] in A.OBJ_TERMINAL:
                return {"objective_id": objective_id, "state": obj["state"], "changed": False}
            if action == "pause":
                new = A.OBJ_PAUSED
            elif action == "resume":
                new = A.OBJ_ACTIVE
            elif action == "cancel":
                new = A.OBJ_CANCELLED
                for st in db.execute("SELECT * FROM steps WHERE objective_id=?", (objective_id,)).fetchall():
                    if st["state"] in A.STEP_TERMINAL:
                        continue
                    if st["state"] in (A.RUNNING, A.DISPATCHING):
                        self._request_cancel(db, st)
                    else:
                        self._step(db, st, A.CANCELLED, reason="objective_cancelled")
            else:
                raise AutopilotError("invalid_action", action)
            db.execute("UPDATE objectives SET state=?, updated_at=?, version=version+1 WHERE id=?", (new, now, objective_id))
            self.db.event(db, objective_id, None, f"objective_{action}")
        return {"objective_id": objective_id, "state": new, "changed": True}

    def set_kill_switch(self, on: bool, reason: str = "") -> dict:
        with self.db.tx() as db:
            self.db.set_meta(db, "kill_switch", "1" if on else "0")
            self.db.event(db, None, None, "kill_switch", on=on, reason=reason[:200])
        return {"kill_switch": on}

    def kill_switch_on(self, db=None) -> bool:
        if db is not None:
            row = db.execute("SELECT value FROM meta WHERE key='kill_switch'").fetchone()
            return bool(row and row["value"] == "1")
        return self.db.get_meta("kill_switch", "0") == "1"

    # ---------------------------------------------------------- consentements
    def request_approval(self, step_id: str, resolution: str) -> dict:
        """Crée une demande de consentement liée au hash exact de l'action. Le code part HORS BANDE."""
        if self.notifier is None:
            raise AutopilotError("no_out_of_band_channel", "aucun canal hors bande configuré : approbation impossible")
        st = self.db.one("SELECT * FROM steps WHERE id=?", (step_id,))
        if st is None:
            raise AutopilotError("unknown_step", step_id)
        allowed = RESOLUTIONS.get(st["state"], ())
        if resolution not in allowed:
            raise AutopilotError("invalid_resolution", f"{st['state']} accepte {list(allowed)}")
        now = self.clock()
        action = {"type": "resolve", "step_id": step_id, "from_state": st["state"], "attempt": st["attempt"],
                  "resolution": resolution, "nonce": secrets.token_hex(8)}
        ahash = action_hash(action)
        code = "".join(secrets.choice("ABCDEFGHJKMNPQRSTUVWXYZ23456789") for _ in range(8))
        with self.db.tx() as db:
            db.execute(
                "INSERT INTO consents(action_hash, action_json, code_hash, state, expires_at, created_at) VALUES(?,?,?,?,?,?)",
                (ahash, canonical(action), sha256(ahash + ":" + code), "pending", now + self.cfg.consent_ttl_s, now),
            )
            self.db.event(db, st["objective_id"], step_id, "approval_requested", action_hash=ahash, resolution=resolution)
        summary = f"Autopilot : {resolution} pour l'étape {st['key']} ({st['state']}, tentative {st['attempt']})."
        self.notifier.send(f"autopilot-{ahash[:12]}", f"{summary}\naction_hash={ahash}\ncode={code}\n")
        return {"action_hash": ahash, "summary": summary, "expires_at": now + self.cfg.consent_ttl_s,
                "next": "Saisir le code reçu hors bande avec autopilot_approve(action_hash, code)."}

    def approve(self, ahash: str, code: str) -> dict:
        now = self.clock()
        with self.db.tx() as db:
            c = db.execute("SELECT * FROM consents WHERE action_hash=?", (ahash,)).fetchone()
            if c is None:
                raise AutopilotError("unknown_consent", "action_hash inconnu")
            if c["state"] != "pending":
                raise AutopilotError("consent_not_pending", c["state"])
            if now > c["expires_at"]:
                db.execute("UPDATE consents SET state='expired' WHERE id=?", (c["id"],))
                return {"approved": False, "error": "consent_expired"}
            if not hmac.compare_digest(c["code_hash"], sha256(ahash + ":" + (code or "").strip().upper())):
                attempts = c["attempts"] + 1
                state = "revoked" if attempts >= self.cfg.consent_max_attempts else "pending"
                db.execute("UPDATE consents SET attempts=?, state=? WHERE id=?", (attempts, state, c["id"]))
                return {"approved": False, "error": "bad_code"}
            action = json.loads(c["action_json"])
            if action_hash(action) != ahash:
                raise AutopilotError("consent_hash_mismatch", "action altérée")
            st = db.execute("SELECT * FROM steps WHERE id=?", (action["step_id"],)).fetchone()
            if st is None or st["state"] != action["from_state"] or st["attempt"] != action["attempt"]:
                db.execute("UPDATE consents SET state='revoked' WHERE id=?", (c["id"],))
                return {"approved": False, "error": "stale_action", "detail": "l'étape a changé depuis la demande"}
            db.execute("UPDATE consents SET state='consumed', consumed_at=? WHERE id=?", (now, c["id"]))
            self._apply_resolution(db, st, action["resolution"])
            self.db.event(db, st["objective_id"], st["id"], "approval_consumed", action_hash=ahash,
                          resolution=action["resolution"])
        return {"approved": True, "step_id": action["step_id"], "resolution": action["resolution"]}

    def _apply_resolution(self, db, st, resolution: str) -> None:
        if resolution == "approve_gate":
            db.execute("UPDATE steps SET gate_state='human_approved' WHERE id=?", (st["id"],))
            if st["kind"] == "decision":
                self._step(db, st, A.DONE, evidence={"evidence_level": "human_attested", "gate": "human_override"})
            else:
                self._step(db, st, A.READY, reason="gate_human_approved")
        elif resolution in ("reject", "mark_failed"):
            self._step(db, st, A.FAILED, reason=f"human_{resolution}")
        elif resolution == "mark_done":
            self._step(db, st, A.DONE, evidence={"evidence_level": "human_attested"})
        elif resolution in ("retry", "approve"):
            # nouvelle tentative explicite : le plafond est relevé d'une unité, jamais au-delà sans nouvel accord
            db.execute("UPDATE steps SET max_attempts=MAX(max_attempts, attempt+1), not_before=NULL WHERE id=?", (st["id"],))
            self._step(db, st, A.READY, reason="human_retry")

    # ================================================================= lecture
    def objective(self, objective_id: str) -> dict:
        obj = self.db.one("SELECT * FROM objectives WHERE id=?", (objective_id,))
        if obj is None:
            raise AutopilotError("unknown_objective", objective_id)
        steps = self.db.query("SELECT * FROM steps WHERE objective_id=? ORDER BY created_at, rowid", (objective_id,))
        last = self.db.one("SELECT MAX(id) AS m FROM events WHERE objective_id=?", (objective_id,))
        budget = {k: v for k, v in json.loads(obj["budget_json"]).items() if not k.startswith("_")}
        return {
            "objective_id": obj["id"], "title": obj["title"], "goal": obj["goal"], "state": obj["state"],
            "budget": budget, "spent": {"jobs": obj["spent_jobs"], "job_seconds": obj["spent_seconds"]},
            "kill_switch": self.kill_switch_on(),
            "etag": sha256(f"{obj['id']}:{obj['version']}:{last['m'] if last else 0}")[:16],
            "steps": [self._step_view(s) for s in steps],
        }

    def list_objectives(self, state: str | None = None, limit: int = 20) -> list[dict]:
        sql, args = "SELECT id AS objective_id, title, state, updated_at FROM objectives", ()
        if state:
            sql, args = sql + " WHERE state=?", (state,)
        rows = self.db.query(sql + " ORDER BY updated_at DESC LIMIT ?", (*args, max(1, min(limit, 100))))
        return [dict(r) for r in rows]

    def handoff(self, objective_id: str) -> dict:
        """Reprise sans transcript : faits (preuves), hypothèses (déclarations d'agent), blocages, prochaine étape."""
        view = self.objective(objective_id)
        facts, hypotheses, blockers = [], [], []
        for s in view["steps"]:
            ev = s.get("evidence") or {}
            if s["state"] == A.DONE:
                item = {"step": s["key"], "evidence_level": ev.get("evidence_level"), "summary": s.get("result_head")}
                (facts if ev.get("evidence_level") == "human_attested" else hypotheses).append(item)
            if s["state"] in A.STEP_HUMAN or s["state"] in (A.BLOCKED_WORKER, A.FAILED):
                blockers.append({"step": s["key"], "state": s["state"], "reason": s.get("last_error"),
                                 "resolutions": list(RESOLUTIONS.get(s["state"], ()))})
        return {
            "objective_id": objective_id, "state": view["state"], "etag": view["etag"], "goal": view["goal"],
            "facts": facts, "hypotheses": hypotheses, "blockers": blockers,
            "next_action": self._next_action(view, blockers),
            "steps": [{"key": s["key"], "state": s["state"], "attempt": s["attempt"]} for s in view["steps"]],
            "note": "facts = attesté humain ; hypotheses = rapport d'agent validé par contrat de sortie, non prouvé indépendamment.",
        }

    @staticmethod
    def _next_action(view: dict, blockers: list[dict]) -> str:
        if view["state"] in A.OBJ_TERMINAL:
            return f"objectif {view['state']} : rien à faire"
        if view["kill_switch"]:
            return "kill switch actif : aucun envoi ; désactiver pour reprendre"
        if view["state"] == A.OBJ_PAUSED:
            return "objectif en pause : autopilot_resume pour reprendre"
        human = [b for b in blockers if b["resolutions"]]
        if human:
            b = human[0]
            return f"décision humaine requise sur {b['step']} ({b['state']}) : autopilot_approve resolution∈{b['resolutions']}"
        running = [s["key"] for s in view["steps"] if s["state"] in (A.RUNNING, A.DISPATCHING)]
        if running:
            return f"en cours : {', '.join(running)} (le superviseur suit seul, rien à faire)"
        return "le superviseur avance seul au prochain tick"

    @staticmethod
    def _step_view(s) -> dict:
        ev = json.loads(s["evidence_json"]) if s["evidence_json"] else None
        return {
            "step_id": s["id"], "key": s["key"], "title": s["title"], "kind": s["kind"], "state": s["state"],
            "attempt": s["attempt"], "max_attempts": s["max_attempts"], "job_id": s["job_id"],
            "broker_state": s["broker_state"], "gate_state": s["gate_state"], "last_error": s["last_error"],
            "not_before": s["not_before"], "evidence": ev, "result_head": (s["result_text"] or "")[:500] or None,
        }

    # ================================================================= boucle
    def tick(self) -> dict:
        stats = {"promoted": 0, "enqueued": 0, "sent": 0, "reconciled": 0}
        now = self.clock()
        if self.kill_switch_on():
            self._kill_active()
        stats["promoted"] = self._promote(now)
        stats["enqueued"] = self._enqueue_ready(now)
        stats["sent"] = self._dispatch(now)
        stats["reconciled"] = self._reconcile(now)
        self._finalize_objectives()
        self.last_tick_at = self.clock()
        return stats

    # ------------------------------------------------------------- transitions
    def _step(self, db, st, new: str, reason: str | None = None, evidence: dict | None = None, **cols) -> None:
        sets, args = ["state=?", "updated_at=?"], [new, self.clock()]
        if reason is not None:
            sets.append("last_error=?")
            args.append(reason[:500])
        if evidence is not None:
            sets.append("evidence_json=?")
            args.append(canonical(evidence))
        for k, v in cols.items():
            if k not in STEP_MUTABLE_COLUMNS:
                raise ValueError(f"colonne non modifiable : {k}")
            sets.append(f"{k}=?")
            args.append(v)
        # noms de colonnes issus d'une allowlist fixe (jamais d'entrée externe) ; valeurs liées
        db.execute(f"UPDATE steps SET {', '.join(sets)} WHERE id=?", (*args, st["id"]))  # nosemgrep
        db.execute("UPDATE objectives SET version=version+1, updated_at=? WHERE id=?", (self.clock(), st["objective_id"]))
        self.db.event(db, st["objective_id"], st["id"], "step_state", frm=st["state"], to=new, reason=reason)

    def _promote(self, now: float) -> int:
        n = 0
        with self.db.tx() as db:
            rows = db.execute(
                "SELECT s.* FROM steps s JOIN objectives o ON o.id=s.objective_id "
                "WHERE o.state=? AND s.state IN (?,?)", (A.OBJ_ACTIVE, A.PENDING, A.COOLDOWN)
            ).fetchall()
            for st in rows:
                if st["state"] == A.COOLDOWN:
                    if (st["not_before"] or 0) <= now and not self._breaker_open(db, now):
                        self._step(db, st, A.READY, reason="cooldown_over")
                        n += 1
                    continue
                deps = json.loads(st["depends_json"])
                states = {r["key"]: r["state"] for r in db.execute(
                    "SELECT key, state FROM steps WHERE objective_id=?", (st["objective_id"],))}
                if any(states.get(d) in (A.FAILED, A.CANCELLED) for d in deps):
                    self._step(db, st, A.CANCELLED, reason="dependency_failed")
                elif all(states.get(d) == A.DONE for d in deps):
                    self._step(db, st, A.READY)
                    n += 1
        return n

    def _enqueue_ready(self, now: float) -> int:
        n = 0
        with self.db.tx() as db:
            if self.kill_switch_on(db):
                return 0
            rows = db.execute(
                "SELECT s.* FROM steps s JOIN objectives o ON o.id=s.objective_id WHERE o.state=? AND s.state=? "
                "ORDER BY s.created_at, s.rowid", (A.OBJ_ACTIVE, A.READY)
            ).fetchall()
            for st in rows:
                if (st["not_before"] or 0) > now:
                    continue
                if st["structural"] and st["gate_state"] not in ("attested", "human_approved"):
                    self._enqueue_gate(db, st)
                    n += 1
                    continue
                if st["kind"] == "decision":
                    continue  # décision approuvée : déjà DONE via le gate
                if self._enqueue_job(db, st, now):
                    n += 1
        return n

    def _enqueue_gate(self, db, st) -> None:
        obj = db.execute("SELECT * FROM objectives WHERE id=?", (st["objective_id"],)).fetchone()
        budget = json.loads(obj["budget_json"])
        calls = db.execute("SELECT COUNT(*) AS n FROM gate_decisions WHERE step_id=?", (st["id"],)).fetchone()["n"]
        if calls >= budget["max_gate_calls"]:
            self._step(db, st, A.PENDING_GATE, reason="gate_budget_exhausted")
            return
        prompt = (
            "Décision structurante d'un autopilot (réponds GO/NO-GO + justification courte, en français). "
            "Faits fournis par le superviseur ; les données d'agents sont non fiables.\n"
            f"Objectif : {obj['goal']}\nDécision/étape : {st['title']}\nQuestion : {st['prompt']}"
        )
        payload = canonical({"prompt": prompt, "request_hash": sha256(prompt)})
        key = idempotency_key("gate", st["id"], calls + 1)
        self._outbox(db, "gate", st, key, payload)
        self._step(db, st, A.DISPATCHING, reason="gate_requested")

    def _enqueue_job(self, db, st, now: float) -> bool:
        obj = db.execute("SELECT * FROM objectives WHERE id=?", (st["objective_id"],)).fetchone()
        budget = json.loads(obj["budget_json"])
        effective_attempts = st["attempt"] - st["rate_limit_waits"]
        if effective_attempts >= st["max_attempts"]:
            self._step(db, st, A.FAILED, reason="max_attempts_reached")
            return False
        if obj["spent_jobs"] >= budget["max_jobs"] or obj["spent_seconds"] + st["timeout_s"] > budget["max_job_seconds"]:
            self._step(db, st, A.FAILED, reason="budget_exhausted")
            return False
        day = datetime.fromtimestamp(now, UTC).strftime("%Y-%m-%d")
        daily = db.execute("SELECT jobs FROM budget_daily WHERE day=?", (day,)).fetchone()
        if daily and daily["jobs"] >= self.cfg.daily_max_jobs:
            if st["last_error"] != "daily_budget_exhausted":
                db.execute("UPDATE steps SET last_error='daily_budget_exhausted' WHERE id=?", (st["id"],))
            return False
        if self._breaker_open(db, now):
            self._step(db, st, A.COOLDOWN, reason="circuit_open", not_before=self._breaker_until(db))
            return False
        if st["kind"] == "local_write" and self._workspace_busy(db, st):
            return False  # un seul job d'écriture (ou audit pendant) par workspace
        attempt = st["attempt"] + 1
        upstream = [dict(r) for r in db.execute(
            "SELECT key, result_text FROM steps WHERE objective_id=? AND key IN (SELECT value FROM json_each(?))",
            (st["objective_id"], st["depends_json"]))]
        prompt = build_prompt(dict(obj), {**dict(st), "attempt": attempt}, upstream)
        key = idempotency_key("job", st["id"], attempt)
        payload = {
            "runner_id": self.cfg.runner_id, "runtime": self.cfg.runtime, "workspace_id": st["workspace_id"],
            "prompt": prompt, "mode": A.KIND_MODE[st["kind"]], "timeout_s": st["timeout_s"], "idempotency_key": key,
        }
        self._outbox(db, "start_job", st, key, canonical(payload))
        db.execute("UPDATE objectives SET spent_jobs=spent_jobs+1, spent_seconds=spent_seconds+? WHERE id=?",
                   (st["timeout_s"], obj["id"]))
        db.execute("INSERT INTO budget_daily(day, jobs) VALUES(?,1) ON CONFLICT(day) DO UPDATE SET jobs=jobs+1", (day,))
        self._step(db, st, A.DISPATCHING, attempt=attempt, job_id=None, broker_state=None, not_before=None,
                   cancel_requested=0)
        return True

    def _workspace_busy(self, db, st) -> bool:
        row = db.execute(
            "SELECT 1 FROM steps WHERE workspace_id=? AND kind='local_write' AND id!=? AND state IN (?,?,?)",
            (st["workspace_id"], st["id"], A.DISPATCHING, A.RUNNING, A.NEEDS_AUDIT),
        ).fetchone()
        return row is not None

    def _outbox(self, db, kind: str, st, key: str, payload: str) -> None:
        now = self.clock()
        db.execute(
            "INSERT INTO outbox(kind, objective_id, step_id, idempotency_key, payload, state, next_attempt_at, created_at, "
            "updated_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(idempotency_key) DO NOTHING",
            (kind, st["objective_id"], st["id"], key, payload, "pending", now, now, now),
        )

    def _request_cancel(self, db, st, mode: int = CANCEL_OBJECTIVE) -> None:
        """mode 1 = annulation d'objectif (étape CANCELLED) ; mode 2 = kill switch (étape reprenable / auditée)."""
        db.execute("UPDATE steps SET cancel_requested=? WHERE id=?", (mode, st["id"]))
        if st["job_id"]:
            self._outbox(db, "cancel_job", st, idempotency_key("cancel", st["id"], st["attempt"]),
                         canonical({"job_id": st["job_id"]}))
            return
        # pas encore de job : annuler l'intention si elle n'est jamais partie ; sinon _on_started annulera le job créé
        n = db.execute("UPDATE outbox SET state='cancelled', updated_at=? WHERE step_id=? AND kind='start_job' "
                       "AND state='pending' AND uncertain_since IS NULL", (self.clock(), st["id"])).rowcount
        if n:
            self._step(db, st, A.CANCELLED if mode == CANCEL_OBJECTIVE else A.READY, reason="cancelled_before_send",
                       cancel_requested=0 if mode == CANCEL_KILL else mode)

    def _kill_active(self) -> None:
        with self.db.tx() as db:
            for st in db.execute("SELECT * FROM steps WHERE state IN (?,?) AND cancel_requested=0",
                                 (A.RUNNING, A.DISPATCHING)).fetchall():
                self._request_cancel(db, st, CANCEL_KILL)
                self.db.event(db, st["objective_id"], st["id"], "kill_switch_cancel")

    # ---------------------------------------------------------------- breaker
    def _breaker_open(self, db, now: float) -> bool:
        row = db.execute("SELECT open_until FROM breakers WHERE runtime=?", (self.cfg.runtime,)).fetchone()
        return bool(row and row["open_until"] and row["open_until"] > now)

    def _breaker_until(self, db) -> float | None:
        row = db.execute("SELECT open_until FROM breakers WHERE runtime=?", (self.cfg.runtime,)).fetchone()
        return row["open_until"] if row else None

    def _breaker_hit(self, db, now: float) -> None:
        db.execute("INSERT INTO breakers(runtime, consecutive) VALUES(?,0) ON CONFLICT(runtime) DO NOTHING", (self.cfg.runtime,))
        db.execute("UPDATE breakers SET consecutive=consecutive+1 WHERE runtime=?", (self.cfg.runtime,))
        n = db.execute("SELECT consecutive FROM breakers WHERE runtime=?", (self.cfg.runtime,)).fetchone()["consecutive"]
        if n >= self.cfg.breaker_threshold:
            db.execute("UPDATE breakers SET open_until=? WHERE runtime=?", (now + self.cfg.breaker_cooldown_s, self.cfg.runtime))
            self.db.event(db, None, None, "circuit_open", runtime=self.cfg.runtime, consecutive=n)

    def _breaker_reset(self, db) -> None:
        db.execute("UPDATE breakers SET consecutive=0, open_until=NULL WHERE runtime=?", (self.cfg.runtime,))

    # ---------------------------------------------------------------- outbox
    def _recover_outbox(self) -> None:
        """Au démarrage : un envoi `sending` interrompu a une issue incertaine -> rejouer les MÊMES octets."""
        now = self.clock()
        with self.db.tx() as db:
            for row in db.execute("SELECT id, step_id, objective_id FROM outbox WHERE state='sending'").fetchall():
                db.execute("UPDATE outbox SET state='pending', uncertain_since=COALESCE(uncertain_since, ?), "
                           "next_attempt_at=?, updated_at=? WHERE id=?", (now, now, now, row["id"]))
                self.db.event(db, row["objective_id"], row["step_id"], "outbox_recovered", outbox_id=row["id"])

    def _dispatch(self, now: float) -> int:
        sent = 0
        due = self.db.query("SELECT id FROM outbox WHERE state='pending' AND next_attempt_at<=? ORDER BY id LIMIT 20", (now,))
        for (oid,) in due:
            with self.db.tx() as db:
                row = db.execute("SELECT * FROM outbox WHERE id=? AND state='pending'", (oid,)).fetchone()
                if row is None:
                    continue
                if row["kind"] == "start_job" and self.kill_switch_on(db):
                    if row["uncertain_since"] is None:
                        # jamais parti : on peut l'abandonner sans risque de doublon
                        db.execute("UPDATE outbox SET state='cancelled', updated_at=? WHERE id=?", (now, oid))
                        st = db.execute("SELECT * FROM steps WHERE id=?", (row["step_id"],)).fetchone()
                        if st and st["state"] == A.DISPATCHING:
                            self._step(db, st, A.READY, reason="kill_switch")
                    continue
                db.execute("UPDATE outbox SET state='sending', attempts=attempts+1, updated_at=? WHERE id=?", (now, oid))
            self._send(row)
            sent += 1
        return sent

    def _send(self, row) -> None:
        payload = json.loads(row["payload"])
        kind = row["kind"]
        try:
            if kind == "start_job":
                result = self.broker.start_job(payload)
            elif kind == "cancel_job":
                result = self.broker.cancel_job(payload["job_id"])
            elif kind == "gate":
                result = self.gate.ask(payload["prompt"])
            else:
                result = {"error": "unknown_outbox_kind"}
        except BrokerUnavailable as exc:
            self._send_uncertain(row, str(exc))
            return
        except RuntimeForbidden as exc:
            with self.db.tx() as db:
                db.execute("UPDATE outbox SET state='failed', last_error=?, updated_at=? WHERE id=?",
                           (str(exc)[:300], self.clock(), row["id"]))
                st = db.execute("SELECT * FROM steps WHERE id=?", (row["step_id"],)).fetchone()
                if st and st["state"] not in A.STEP_TERMINAL:
                    self._step(db, st, A.FAILED, reason="runtime_forbidden")
            return
        with self.db.tx() as db:
            st = db.execute("SELECT * FROM steps WHERE id=?", (row["step_id"],)).fetchone()
            if kind == "start_job":
                self._on_started(db, row, st, result)
            elif kind == "gate":
                self._on_gate(db, row, st, payload, result)
            else:
                db.execute("UPDATE outbox SET state='acked', result_json=?, updated_at=? WHERE id=?",
                           (canonical(result)[:2000], self.clock(), row["id"]))

    def _send_uncertain(self, row, error: str) -> None:
        now = self.clock()
        with self.db.tx() as db:
            first = row["uncertain_since"] or now
            if now - first > self.cfg.replay_window_s:
                db.execute("UPDATE outbox SET state='expired', last_error=?, updated_at=? WHERE id=?", (error, now, row["id"]))
                st = db.execute("SELECT * FROM steps WHERE id=?", (row["step_id"],)).fetchone()
                if st and st["state"] == A.DISPATCHING:
                    target = A.PENDING_GATE if row["kind"] == "gate" else A.NEEDS_AUDIT
                    self._step(db, st, target, reason="send_outcome_unknown_after_replay_window")
                return
            delay = backoff_s(self.cfg, row["attempts"], None, self.rng)
            db.execute("UPDATE outbox SET state='pending', uncertain_since=?, last_error=?, next_attempt_at=?, updated_at=? "
                       "WHERE id=?", (first, error[:300], now + delay, now, row["id"]))

    def _on_started(self, db, row, st, result: dict) -> None:
        now = self.clock()
        if isinstance(result, dict) and result.get("job_id"):
            db.execute("UPDATE outbox SET state='acked', result_json=?, updated_at=? WHERE id=?",
                       (canonical({"job_id": result["job_id"], "created": result.get("created")}), now, row["id"]))
            if st and st["state"] in (A.DISPATCHING, A.BLOCKED_WORKER):
                self._step(db, st, A.RUNNING, job_id=result["job_id"], broker_state=result.get("state"))
                if st["cancel_requested"]:
                    self._outbox(db, "cancel_job", st, idempotency_key("cancel", st["id"], st["attempt"]),
                                 canonical({"job_id": result["job_id"]}))
            return
        code = (result or {}).get("error", "unknown_error")
        if code in WORKER_UNAVAILABLE:
            # le broker a refusé : aucun job créé, ré-essai sûr plus tard avec les mêmes octets
            db.execute("UPDATE outbox SET state='pending', last_error=?, next_attempt_at=?, updated_at=? WHERE id=?",
                       (code, now + self.cfg.breaker_cooldown_s, now, row["id"]))
            if st and st["state"] == A.DISPATCHING:
                self._step(db, st, A.BLOCKED_WORKER, reason=f"opencode indisponible : {code}")
            return
        db.execute("UPDATE outbox SET state='failed', last_error=?, updated_at=? WHERE id=?", (code, now, row["id"]))
        if st and st["state"] in (A.DISPATCHING, A.BLOCKED_WORKER):
            target = A.NEEDS_AUDIT if code == "idempotency_conflict" else A.FAILED
            self._step(db, st, target, reason=f"broker: {code}")

    def _on_gate(self, db, row, st, payload: dict, result: GateResult) -> None:
        now = self.clock()
        attested = is_attested(result, self.cfg.expected_served_model)
        db.execute(
            "INSERT INTO gate_decisions(step_id, request_hash, question, answer, served_model, attested, session_id, error, at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (row["step_id"], payload["request_hash"], payload["prompt"][:4000], redact(result.answer or "")[:8000],
             result.served_model, int(attested), result.session_id, result.error, now),
        )
        db.execute("UPDATE outbox SET state='acked', result_json=?, updated_at=? WHERE id=?",
                   (canonical({"attested": attested, "served_model": result.served_model, "error": result.error}), now, row["id"]))
        if st is None or st["state"] != A.DISPATCHING:
            return
        if attested:
            db.execute("UPDATE steps SET gate_state='attested' WHERE id=?", (st["id"],))
            if st["kind"] == "decision":
                self._step(db, st, A.DONE, result_text=redact(result.answer)[:MAX_RESULT_CHARS],
                           evidence={"evidence_level": "opus_attested", "served_model": result.served_model})
            else:
                self._step(db, st, A.READY, reason="gate_attested")
        else:
            reason = result.error or f"modèle servi non attesté (served_model={result.served_model!r})"
            self._step(db, st, A.PENDING_GATE, reason=reason,
                       result_text=redact(result.answer or "")[:MAX_RESULT_CHARS] or None)

    # ------------------------------------------------------------ réconciliation
    def _reconcile(self, now: float) -> int:
        n = 0
        for st in self.db.query("SELECT * FROM steps WHERE state=? AND job_id IS NOT NULL", (A.RUNNING,)):
            try:
                job = self.broker.get_job(st["job_id"])
            except BrokerUnavailable:
                continue
            n += 1
            if job.get("error") == "unknown_job":
                with self.db.tx() as db:
                    self._step(db, st, A.NEEDS_AUDIT, reason="job introuvable côté broker (purgé ?) : jamais resoumis")
                continue
            state = job.get("state")
            if state not in A.BROKER_TERMINAL:
                if state != st["broker_state"]:
                    with self.db.tx() as db:
                        db.execute("UPDATE steps SET broker_state=? WHERE id=?", (state, st["id"]))
                continue
            output = ""
            if state in ("completed", "failed", "timeout"):
                try:
                    output = self.broker.read_output(st["job_id"])
                except BrokerUnavailable:
                    continue
            with self.db.tx() as db:
                cur = db.execute("SELECT * FROM steps WHERE id=?", (st["id"],)).fetchone()
                if cur["state"] != A.RUNNING:
                    continue
                self._on_terminal(db, cur, job, output, now)
        # résultats tardifs : les jobs d'étapes sorties de RUNNING sont observés, jamais promus en DONE
        for st in self.db.query("SELECT * FROM steps WHERE state IN (?,?) AND job_id IS NOT NULL",
                                (A.NEEDS_AUDIT, A.NEEDS_EFFECT_AUDIT)):
            try:
                job = self.broker.get_job(st["job_id"])
            except BrokerUnavailable:
                continue
            if job.get("state") and job.get("state") != st["broker_state"]:
                with self.db.tx() as db:
                    db.execute("UPDATE steps SET broker_state=? WHERE id=?", (job["state"], st["id"]))
                    self.db.event(db, st["objective_id"], st["id"], "late_result", frm=st["broker_state"], to=job["state"])
        return n

    def _on_terminal(self, db, st, job: dict, output: str, now: float) -> None:
        state = job["state"]
        summary = job.get("result_summary") or ""
        text = redact(f"{summary}\n{output}" if output else summary) or ""
        db.execute("UPDATE steps SET broker_state=? WHERE id=?", (state, st["id"]))
        expect = json.loads(st["expect_json"])
        if st["cancel_requested"]:
            if st["kind"] == "local_write":
                # même annulé, le job a pu écrire avant d'être tué
                self._step(db, st, A.NEEDS_AUDIT, reason=f"annulation demandée, job {state} : écriture à auditer")
            elif st["cancel_requested"] == CANCEL_KILL:
                self._step(db, st, A.READY, reason="kill_switch", cancel_requested=0)
            else:
                self._step(db, st, A.CANCELLED, reason="cancel_requested")
            return
        if state == "completed":
            ok, evidence = validate_output(expect, text)
            if ok:
                self._breaker_reset(db)
                self._step(db, st, A.DONE, evidence=evidence, result_text=text[-MAX_RESULT_CHARS:])
                return
            limited, retry_after = rate_limit_info(text)
            if limited:
                self._cooldown(db, st, retry_after, now)
                return
            self._after_failure(db, st, "validation_failed", now, evidence)
            return
        if state in ("failed", "timeout"):
            limited, retry_after = rate_limit_info(text)
            if limited:
                self._cooldown(db, st, retry_after, now)
                return
            self._after_failure(db, st, f"broker_{state}", now)
            return
        if state == "lost":
            # le processus peut encore tourner : lecture = reprise après délai de grâce ; écriture = audit
            if st["kind"] == "read":
                self._retry_or_fail(db, st, "broker_lost", now + self.cfg.lost_grace_s)
            else:
                self._step(db, st, A.NEEDS_AUDIT if st["kind"] == "local_write" else A.NEEDS_EFFECT_AUDIT,
                           reason="broker_lost : issue inconnue, audit avant toute reprise")
            return
        # cancelled sans demande de notre part
        self._after_failure(db, st, "broker_cancelled_externally", now)

    def _after_failure(self, db, st, reason: str, now: float, evidence: dict | None = None) -> None:
        if st["kind"] == "read":
            self._retry_or_fail(db, st, reason, now + backoff_s(self.cfg, st["attempt"], None, self.rng), evidence)
        else:
            self._step(db, st, A.NEEDS_AUDIT, reason=f"{reason} : écriture locale à auditer", evidence=evidence)

    def _retry_or_fail(self, db, st, reason: str, not_before: float, evidence: dict | None = None) -> None:
        if st["attempt"] - st["rate_limit_waits"] >= st["max_attempts"]:
            self._step(db, st, A.FAILED, reason=f"{reason} ; tentatives épuisées", evidence=evidence)
        else:
            self._step(db, st, A.READY, reason=reason, evidence=evidence, not_before=not_before)

    def _cooldown(self, db, st, retry_after: float | None, now: float) -> None:
        self._breaker_hit(db, now)
        first = st["first_rate_limit_at"] or now
        waits = st["rate_limit_waits"] + 1
        obj = db.execute("SELECT budget_json FROM objectives WHERE id=?", (st["objective_id"],)).fetchone()
        budget = json.loads(obj["budget_json"])
        if waits > budget["max_rate_limit_waits"] or now - first > budget["max_rate_limit_wall_s"]:
            self._step(db, st, A.FAILED, reason="rate_limit : plafond d'attentes atteint (aucune évasion de quota)",
                       rate_limit_waits=waits, first_rate_limit_at=first)
            return
        until = now + backoff_s(self.cfg, waits, retry_after, self.rng)
        breaker = self._breaker_until(db)
        if breaker and breaker > until:
            until = breaker
        self._step(db, st, A.COOLDOWN, reason="rate_limited", not_before=until, rate_limit_waits=waits,
                   first_rate_limit_at=first)

    def _finalize_objectives(self) -> None:
        with self.db.tx() as db:
            for obj in db.execute("SELECT id, state FROM objectives WHERE state IN (?,?)", (A.OBJ_ACTIVE, A.OBJ_PAUSED)).fetchall():
                states = [r["state"] for r in db.execute("SELECT state FROM steps WHERE objective_id=?", (obj["id"],))]
                if not states or not all(s in A.STEP_TERMINAL for s in states):
                    continue
                new = A.OBJ_DONE if all(s == A.DONE for s in states) else A.OBJ_FAILED
                db.execute("UPDATE objectives SET state=?, version=version+1, updated_at=? WHERE id=?", (new, self.clock(), obj["id"]))
                self.db.event(db, obj["id"], None, "objective_finished", state=new)
