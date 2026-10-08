"""Fakes pour les tests autopilot : broker (sémantique d'idempotence du vrai broker), horloge, gate, notifier."""

from __future__ import annotations

import hashlib
import json

from orch_autopilot.broker import BrokerUnavailable, check_start_payload
from orch_autopilot.gate import GateResult


class Clock:
    def __init__(self, t: float = 1_800_000_000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, s: float) -> None:
        self.t += s


class FakeBroker:
    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {}
        self.by_key: dict[str, tuple[str, str]] = {}
        self.outputs: dict[str, str] = {}
        self.start_calls: list[dict] = []
        self.cancel_calls: list[str] = []
        self.opencode_available = True
        self.down = False                # toute requête échoue AVANT application
        self.drop_response_once = False  # la création est appliquée puis la réponse est perdue
        self._n = 0

    # API Broker
    def runners(self) -> list[dict]:
        return [{"id": "main-windows-pc", "status": "online", "runtimes": [{"id": "opencode", "available": self.opencode_available}]}]

    def start_job(self, payload: dict) -> dict:
        check_start_payload(payload)
        if self.down:
            raise BrokerUnavailable("down")
        self.start_calls.append(payload)
        if not self.opencode_available:
            return {"error": "runtime_unavailable", "message": "opencode indisponible"}
        h = hashlib.sha256(json.dumps({k: payload[k] for k in sorted(payload)}, sort_keys=True).encode()).hexdigest()
        key = payload["idempotency_key"]
        if key in self.by_key:
            prev_hash, job_id = self.by_key[key]
            if prev_hash != h:
                return {"error": "idempotency_conflict", "message": "x"}
            result = {"job_id": job_id, "state": self.jobs[job_id]["state"], "created": False}
        else:
            self._n += 1
            job_id = f"job-{self._n}"
            self.jobs[job_id] = {"job_id": job_id, "state": "queued", "result_summary": None, "payload": payload}
            self.by_key[key] = (h, job_id)
            result = {"job_id": job_id, "state": "queued", "created": True}
        if self.drop_response_once:
            self.drop_response_once = False
            raise BrokerUnavailable("response lost")
        return result

    def get_job(self, job_id: str) -> dict:
        if self.down:
            raise BrokerUnavailable("down")
        job = self.jobs.get(job_id)
        return dict(job) if job else {"error": "unknown_job", "job_id": job_id}

    def read_output(self, job_id: str) -> str:
        if self.down:
            raise BrokerUnavailable("down")
        return self.outputs.get(job_id, "")

    def cancel_job(self, job_id: str) -> dict:
        if self.down:
            raise BrokerUnavailable("down")
        self.cancel_calls.append(job_id)
        job = self.jobs.get(job_id)
        if job and job["state"] in ("queued", "claimed"):
            job["state"] = "cancelled"
            return {"result": "cancelled"}
        return {"result": "cancel_requested"}

    # pilotage test
    def finish(self, job_id: str, state: str = "completed", output: str = "", summary: str | None = None) -> None:
        self.jobs[job_id]["state"] = state
        self.jobs[job_id]["result_summary"] = summary
        self.outputs[job_id] = output

    def created_jobs(self) -> list[str]:
        return list(self.jobs)


class FakeGate:
    def __init__(self, served_model: str | None = None, answer: str = "GO : réversible") -> None:
        self.served_model = served_model
        self.answer = answer
        self.calls: list[str] = []

    def ask(self, prompt: str) -> GateResult:
        self.calls.append(prompt)
        return GateResult(self.answer, "sess-1", self.served_model)


class MemoryNotifier:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def send(self, subject: str, body: str) -> None:
        self.messages.append((subject, body))

    def last_code(self) -> str:
        body = self.messages[-1][1]
        return next(line.split("=", 1)[1] for line in body.splitlines() if line.startswith("code="))
