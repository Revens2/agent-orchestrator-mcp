"""Mode `trusted_personal_runner` : opt-in explicite, preuves honnêtes, défaut isolé inchangé.

Couvre :
- défaut conservateur : local_write sur main-windows-pc toujours refusé sans opt-in ;
- opt-in sans accusé explicite refusé (fail-closed) ;
- mode personnel : local_write autorisé sur main-windows-pc, DONE agent_reported
  (sans enveloppe, runner historique) ou runner_observed_same_user (enveloppe même-SID) ;
- jamais `externally_verified` ;
- faux DONE (marqueur sans contrat) toujours refusé, même en mode personnel ;
- mode isolé inchangé (blocage legacy + exigence pubkey/SID).
"""

from __future__ import annotations

import random

import pytest

import orch_autopilot as A
from orch_autopilot.db import AutopilotDB
from orch_autopilot.policy import PERSONAL_ACK, Config, PolicyError
from orch_autopilot.supervisor import AutopilotError, Supervisor
from tests.autopilot_fakes import AGENT_SID, ARTIFACT, Clock, FakeBroker, FakeGate, MemoryNotifier, RunnerSigner

MARK = "AUTOPILOT_STEP_OK"
SAME_SID = "S-1-5-21-1111111111-2222222222-3333333333-1009"


def _write(key="w1"):
    return {"key": key, "kind": "local_write", "workspace_id": "e2e", "prompt": "écris",
            "expect": {"markers": [MARK], "artifacts": [ARTIFACT] }}


def _personal_cfg(**kw):
    base = dict(trust_mode="trusted_personal_runner", personal_ack=PERSONAL_ACK,
                runner_id="main-windows-pc",
                allowed_kinds=frozenset({"read", "local_write", "decision"}),
                local_write_needs_approval=False)
    base.update(kw)
    return Config(**base)


def _sup(tmp_path, cfg, broker=None):
    clock = Clock()
    broker = broker or FakeBroker()
    sup = Supervisor(AutopilotDB(tmp_path / "p.db", clock=clock), broker, cfg, gate=FakeGate(),
                     notifier=MemoryNotifier(), clock=clock, rng=random.Random(1))
    return sup, broker, clock


def test_default_isolated_still_blocks_legacy_local_write():
    with pytest.raises(PolicyError) as exc:
        Config(runner_id="main-windows-pc",
               allowed_kinds=frozenset({"read", "local_write", "decision"}))
    assert exc.value.code == "local_write_requires_isolated_runner"


def test_isolated_still_requires_pubkey_and_sid():
    with pytest.raises(PolicyError) as exc:
        Config(runner_id="autopilot-win-01",
               allowed_kinds=frozenset({"read", "local_write"}))
    assert exc.value.code == "local_write_requires_runner_evidence"


def test_personal_requires_explicit_ack():
    with pytest.raises(PolicyError) as exc:
        Config(trust_mode="trusted_personal_runner", runner_id="main-windows-pc",
               allowed_kinds=frozenset({"read", "local_write"}))
    assert exc.value.code == "personal_ack_required"
    with pytest.raises(PolicyError):
        Config(trust_mode="trusted_personal_runner", personal_ack="oui",
               runner_id="main-windows-pc", allowed_kinds=frozenset({"read", "local_write"}))


def test_personal_allows_legacy_without_pubkey():
    cfg = _personal_cfg()
    assert cfg.is_personal is True
    assert cfg.runner_id == "main-windows-pc"


def test_personal_rejects_unknown_trust_mode():
    with pytest.raises(PolicyError):
        Config(trust_mode="tout-permis")


def test_personal_local_write_agent_reported_reaches_done_without_envelope(tmp_path):
    cfg = _personal_cfg()
    sup, broker, _ = _sup(tmp_path, cfg)
    assert broker.signer is None  # runner historique : aucune enveloppe
    oid = sup.submit({"title": "t", "goal": "g", "steps": [_write()]})["objective_id"]
    sup.tick()
    st = sup.objective(oid)["steps"][0]
    broker.finish(st["job_id"], output=MARK)
    sup.tick()
    done = sup.objective(oid)["steps"][0]
    assert done["state"] == A.DONE
    assert done["evidence"]["evidence_level"] == "agent_reported"
    assert done["evidence"]["evidence_level"] != "externally_verified"


def test_personal_fake_done_without_marker_never_done(tmp_path):
    cfg = _personal_cfg()
    sup, broker, _ = _sup(tmp_path, cfg)
    oid = sup.submit({"title": "t", "goal": "g", "steps": [_write()]})["objective_id"]
    sup.tick()
    st = sup.objective(oid)["steps"][0]
    broker.finish(st["job_id"], output="terminé, tout est OK (sans marqueur)")
    sup.tick()
    done = sup.objective(oid)["steps"][0]
    assert done["state"] != A.DONE


def test_personal_same_user_envelope_is_runner_observed_same_user(tmp_path):
    signer = RunnerSigner()
    broker = FakeBroker()
    broker.signer, broker.clock = signer, Clock()
    cfg = _personal_cfg(runner_pubkey=signer.pubkey)
    sup, broker, clock = _sup(tmp_path, cfg, broker)
    broker.clock = clock
    oid = sup.submit({"title": "t", "goal": "g", "steps": [_write()]})["objective_id"]
    sup.tick()
    st = sup.objective(oid)["steps"][0]
    job = broker.jobs[st["job_id"]]
    payload = signer.payload(job)
    # même compte Windows : agent et runner partagent le SID, sondes non refusées (honnête)
    payload["attestation"] = {**payload["attestation"], "ok": False, "agent_sid": SAME_SID,
                              "agent_process_sid": SAME_SID, "runner_sid": SAME_SID,
                              "agent_is_admin": True, "agent_elevated": False,
                              "probes": [{"probe": "read_runner_token", "denied": False}]}
    broker.finish(st["job_id"], output=MARK, envelope=signer.envelope(payload))
    sup.tick()
    done = sup.objective(oid)["steps"][0]
    assert done["state"] == A.DONE
    assert done["evidence"]["evidence_level"] == "runner_observed_same_user"


def _same_user_payload(signer, job):
    payload = signer.payload(job)
    payload["attestation"] = {**payload["attestation"], "ok": False, "agent_sid": SAME_SID,
                              "agent_process_sid": SAME_SID, "runner_sid": SAME_SID,
                              "agent_is_admin": True, "agent_elevated": False,
                              "probes": [{"probe": "read_runner_token", "denied": False}]}
    return payload


def test_personal_envelope_other_job_replayed_rejected(tmp_path):
    signer = RunnerSigner()
    broker = FakeBroker()
    broker.clock = Clock()
    cfg = _personal_cfg(runner_pubkey=signer.pubkey)
    sup, broker, clock = _sup(tmp_path, cfg, broker)
    broker.clock = clock
    oid1 = sup.submit({"title": "t", "goal": "g", "steps": [_write("w1")]})["objective_id"]
    sup.tick()
    jid1 = sup.objective(oid1)["steps"][0]["job_id"]
    old = signer.envelope(_same_user_payload(signer, broker.jobs[jid1]))
    broker.finish(jid1, output=MARK, envelope=signer.envelope(_same_user_payload(signer, broker.jobs[jid1])))
    sup.tick()
    assert sup.objective(oid1)["steps"][0]["state"] == A.DONE
    oid2 = sup.submit({"title": "t", "goal": "g", "steps": [_write("w2")]})["objective_id"]
    sup.tick()
    jid2 = sup.objective(oid2)["steps"][0]["job_id"]
    broker.finish(jid2, output=MARK, envelope=old)
    sup.tick()
    st2 = sup.objective(oid2)["steps"][0]
    assert st2["state"] == A.NEEDS_AUDIT and "job_id_mismatch" in st2["last_error"]


def test_personal_local_write_approval_gate_by_default(tmp_path):
    cfg = _personal_cfg(local_write_needs_approval=True)
    broker = FakeBroker()
    sup, broker, _ = _sup(tmp_path, cfg, broker)
    sup.notifier = MemoryNotifier()
    oid = sup.submit({"title": "t", "goal": "g", "steps": [_write()]})["objective_id"]
    sup.tick()
    assert sup.objective(oid)["steps"][0]["state"] == A.NEEDS_APPROVAL
    assert broker.start_calls == []
