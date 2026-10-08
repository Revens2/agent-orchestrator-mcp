"""Chaos : crash du superviseur à chaque frontière de l'outbox + fuzz d'invariants (aucun doublon, aucun faux DONE)."""

import random

import pytest

import orch_autopilot as A
from orch_autopilot.broker import BrokerUnavailable
from orch_autopilot.db import AutopilotDB
from orch_autopilot.policy import Config
from orch_autopilot.supervisor import Supervisor
from tests.autopilot_fakes import AGENT_SID, ARTIFACT, RUNNER_ID, Clock, FakeBroker, RunnerSigner

SIGNER = RunnerSigner()

MARK = "AUTOPILOT_STEP_OK"


class Crash(BaseException):
    """Mort brutale du processus (non rattrapée par le code applicatif)."""


class CrashingBroker(FakeBroker):
    def __init__(self) -> None:
        super().__init__()
        self.crash_at: str | None = None

    def _maybe(self, point: str) -> None:
        if self.crash_at == point:
            self.crash_at = None
            raise Crash(point)

    def start_job(self, payload):
        self._maybe("before_apply")
        result = super().start_job(payload)
        self._maybe("after_apply")
        return result

    def get_job(self, job_id):
        self._maybe("reconcile")
        return super().get_job(job_id)

    def read_output(self, job_id):
        self._maybe("read_output")
        return super().read_output(job_id)


def make(path, clock, broker, seed=0):
    if broker.signer is None:
        broker.signer = SIGNER
    broker.clock = clock
    return Supervisor(AutopilotDB(path, clock=clock), broker, Config(runner_id=RUNNER_ID, runner_pubkey=broker.signer.pubkey,
                      expected_agent_sid=AGENT_SID, allowed_kinds=frozenset({"read", "local_write", "decision"}), local_write_needs_approval=False, backoff_base_s=5, backoff_max_s=30, lost_grace_s=30),
                      clock=clock, rng=random.Random(seed))


def spec(kind):
    return {"title": "c", "goal": "chaos", "steps": [
        {"key": "s1", "kind": kind, "workspace_id": "e2e", "prompt": "p",
         "expect": {"markers": [MARK], **({"artifacts": [ARTIFACT]} if kind == "local_write" else {})}}]}


@pytest.mark.parametrize("point", ["before_apply", "after_apply", "reconcile", "read_output"])
@pytest.mark.parametrize("kind", ["read", "local_write"])
def test_crash_at_each_boundary_no_duplicate_no_false_done(tmp_path, point, kind):
    clock, broker = Clock(), CrashingBroker()
    sup = make(tmp_path / "a.db", clock, broker)
    oid = sup.submit(spec(kind))["objective_id"]
    if point in ("reconcile", "read_output"):
        sup.tick()
        broker.finish(broker.created_jobs()[0], output=MARK)
    broker.crash_at = point
    with pytest.raises(Crash):
        for _ in range(3):
            sup.tick()
    sup.db.close()
    sup = make(tmp_path / "a.db", clock, broker, seed=1)  # redémarrage (reboot VPS / systemd)
    for _ in range(4):
        clock.advance(60)
        sup.tick()
        for j in broker.created_jobs():
            if broker.jobs[j]["state"] == "queued":
                broker.finish(j, output=MARK)
    assert len(broker.created_jobs()) == 1
    assert sup.objective(oid)["steps"][0]["state"] == A.DONE


@pytest.mark.parametrize("seed", range(25))
def test_fuzz_invariants(tmp_path, seed):
    rng = random.Random(seed)
    clock, broker = Clock(), CrashingBroker()
    path = tmp_path / "a.db"
    sup = make(path, clock, broker, seed)
    kinds = [rng.choice(["read", "local_write"]) for _ in range(4)]
    steps = [{"key": f"s{i}", "kind": kinds[i], "workspace_id": "e2e", "prompt": "p",
              "depends_on": [f"s{i - 1}"] if i and rng.random() < 0.5 else [],
              "expect": {"markers": [MARK], **({"artifacts": [ARTIFACT]} if kinds[i] == "local_write" else {})}}
             for i in range(4)]
    oid = sup.submit({"title": "f", "goal": "fuzz", "steps": steps})["objective_id"]
    honest_done: set[str] = set()  # jobs réellement terminés avec le contrat
    for _ in range(60):
        op = rng.random()
        try:
            if op < 0.08:
                broker.crash_at = rng.choice(["before_apply", "after_apply", "reconcile", "read_output"])
            elif op < 0.14:
                broker.down = not broker.down
            elif op < 0.18:
                broker.drop_response_once = True
            elif op < 0.45:
                live = [j for j, v in broker.jobs.items() if v["state"] in ("queued", "running")]
                if live:
                    j = rng.choice(live)
                    end = rng.choice(["completed", "completed", "failed", "lost", "timeout"])
                    good = end == "completed" and rng.random() < 0.7
                    broker.finish(j, end, output=MARK if good else "rien")
                    if good:
                        honest_done.add(j)
            clock.advance(rng.choice([1, 30, 120, 400]))
            sup.tick()
        except Crash:
            sup.db.close()
            sup = make(path, clock, broker, rng.randrange(1000))
        except BrokerUnavailable:
            pass
    broker.down = False
    keys = [p["idempotency_key"] for p in (v["payload"] for v in broker.jobs.values())]
    assert len(keys) == len(set(keys)), "un job broker par (étape, tentative)"
    assert {v["payload"]["runtime"] for v in broker.jobs.values()} <= {"opencode"}
    for s in sup.objective(oid)["steps"]:
        if s["state"] == A.DONE:
            assert s["job_id"] in honest_done, f"faux DONE {s}"
        jobs_for_step = [v for v in broker.jobs.values() if f"step={s['key']} " in v["payload"]["prompt"]]
        assert len(jobs_for_step) <= s["max_attempts"] + 10
    rows = sup.db.query("SELECT kind, workspace_id FROM steps WHERE state IN ('RUNNING','DISPATCHING') AND kind='local_write'")
    assert len(rows) <= 1, "un seul job d'écriture actif par workspace"


def test_systemd_unit_confines_supervisor():
    """Confinement OS côté VPS : écriture limitée à /srv/autopilot/data, jamais orch.db, réseau local seul."""
    from pathlib import Path

    unit = (Path(__file__).parents[1] / "deploy/autopilot/autopilot.service").read_text(encoding="utf-8")
    lines = dict(line.split("=", 1) for line in unit.splitlines() if "=" in line and not line.startswith("#"))
    assert lines["User"] == "orch-app" and lines["ProtectSystem"] == "strict" and lines["NoNewPrivileges"] == "yes"
    assert lines["ReadWritePaths"] == "/srv/autopilot/data"
    assert lines["IPAddressDeny"] == "any" and lines["IPAddressAllow"] == "localhost"
    assert "/srv/orch/data" not in unit and lines["CapabilityBoundingSet"] == ""
