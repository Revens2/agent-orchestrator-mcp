"""Runner isolé : manifeste réel du workspace, attestation, enveloppe acceptée/refusée par le superviseur.

Les tests sous un VRAI second compte Windows (CreateProcessWithLogonW) ne tournent que si
AUTOPILOT_TEST_AGENT_USER / AUTOPILOT_TEST_AGENT_CRED (fichier DPAPI) sont définis : voir RUNBOOK (T1/T7).
"""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

import pytest

from orch_protocol import evidence as EV
from orch_runner import isolation as I

windows = pytest.mark.skipif(os.name != "nt", reason="Windows uniquement")

RUNNER = {"sid": "S-1-5-21-1-2-3-1009", "is_admin": False, "elevated": False}
PROBE = {"sid": "S-1-5-21-1-2-3-1010", "is_admin": False, "elevated": False, "integrity_rid": 8192,
         "probes": [{"probe": "read", "path": r"C:\Users\Juliann\.orch-runner\runner.token", "denied": True,
                     "why": "access_denied"}]}


PROMPT = "[autopilot objective=o step=w attempt=1] tâche"


def job(mode="workspace_write"):
    return {"job_id": "j-1", "fencing": 3, "workspace_id": "e2e", "mode": mode, "prompt": PROMPT}


def payload_for(before, after, mode="workspace_write", att=None):
    return I.build_payload(runner_id="autopilot-win-01", job=job(mode), exit_code=0, started_at=1000.0,
                           finished_at=1001.0, tree_killed=True, att=att or I.attestation(RUNNER, PROBE, PROBE["sid"]),
                           before=before, after=after)


def binding(**kw):
    base = {"runner_id": "autopilot-win-01", "job_id": "j-1", "workspace_id": "e2e", "mode": "workspace_write",
            "exit_code": 0, "expected_agent_sid": PROBE["sid"], "not_before": 1000.0,
            "prompt_sha256": hashlib.sha256(PROMPT.encode("utf-8")).hexdigest()}
    base.update(kw)
    return EV.Binding(**base)


# ------------------------------------------------------------------ manifeste réel
@windows
def test_real_scan_diff_accepts_declared_artifact_only(tmp_path):
    (tmp_path / "keep.txt").write_text("a")
    before = I.scan_workspace(str(tmp_path))
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "index.html").write_text("<h1>landing</h1>")
    after = I.scan_workspace(str(tmp_path))
    exp = EV.Expectation(kind="local_write", artifacts=("out/index.html",))
    assert EV.check_payload(payload_for(before, after), exp, binding()) == []


@windows
def test_real_scan_detects_out_of_scope_and_git_hook(tmp_path):
    (tmp_path / ".git" / "hooks").mkdir(parents=True)
    before = I.scan_workspace(str(tmp_path))
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "index.html").write_text("x")
    (tmp_path / ".git" / "hooks" / "pre-commit").write_text("evil")
    (tmp_path / "keep.txt").write_text("changed")
    after = I.scan_workspace(str(tmp_path))
    v = EV.check_payload(payload_for(before, after), EV.Expectation("local_write", ("out/index.html",)), binding())
    assert "out_of_scope_change" in v and "sensitive_path_changed" in v


@windows
def test_real_scan_flags_junction_without_following_it(tmp_path):
    target = tmp_path.parent / (tmp_path.name + "-outside")
    target.mkdir()
    (target / "secret.txt").write_text("hors workspace")
    subprocess.run(["cmd", "/c", "mklink", "/J", str(tmp_path / "link"), str(target)], check=True, capture_output=True)
    scan = I.scan_workspace(str(tmp_path))
    assert scan.reparse_points == ["link"] and not any("secret" in p for p in scan.files)
    v = EV.check_payload(payload_for(scan, scan), EV.Expectation("read"), binding(mode="read_only"))
    assert "reparse_point" in v


@windows
def test_real_scan_flags_alternate_data_stream(tmp_path):
    (tmp_path / "a.txt").write_text("x")
    before = I.scan_workspace(str(tmp_path))
    with open(str(tmp_path / "a.txt") + ":hidden", "w") as fh:
        fh.write("payload")
    after = I.scan_workspace(str(tmp_path))
    assert after.alternate_streams and after.ads_checked
    v = EV.check_payload(payload_for(before, after, mode="read_only"), EV.Expectation("read"), binding(mode="read_only"))
    assert "alternate_data_stream" in v


def test_scan_caps_fail_closed(tmp_path):
    for i in range(5):
        (tmp_path / f"f{i}.txt").write_text("x" * 10)
    scan = I.scan_workspace(str(tmp_path), max_files=3)
    assert scan.complete is False


# ------------------------------------------------------------------ attestation
@pytest.mark.parametrize("runner,probe,proc_sid,ok", [
    (RUNNER, PROBE, None, False),                                            # jeton agent illisible
    (RUNNER, PROBE, PROBE["sid"], True),
    (RUNNER, PROBE, RUNNER["sid"], False),                                    # agent lancé sous A
    ({**RUNNER, "is_admin": True}, PROBE, None, False),                      # runner admin
    (RUNNER, {**PROBE, "is_admin": True}, None, False),                      # agent admin
    (RUNNER, {**PROBE, "sid": RUNNER["sid"]}, RUNNER["sid"], False),         # même compte
    (RUNNER, {**PROBE, "probes": [{**PROBE["probes"][0], "denied": False}]}, None, False),
    (RUNNER, {**PROBE, "probes": []}, None, False),
])
def test_attestation_rules(runner, probe, proc_sid, ok):
    assert I.attestation(runner, probe, proc_sid)["ok"] is ok


def test_signed_envelope_roundtrip_through_supervisor_rules(tmp_path):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

    key = Ed25519PrivateKey.generate()
    pub = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw).hex()
    empty = I.Scan({}, [], [], True, True)
    after = I.Scan({"out/x.txt": {"sha256": "ab" * 32, "size": 3}}, [], [], True, True)
    line = EV.encode_envelope(payload_for(empty, after), key.sign)
    assert len(line) < EV.MAX_ENVELOPE_CHARS
    assert line[len(EV.PREFIX):] == line[len(EV.PREFIX):].lower()  # hex minuscule : aucun motif de redact
    decoded = EV.decode_envelope(line + "\nrésumé agent", EV.ed25519_verifier(pub))
    assert EV.check_payload(decoded, EV.Expectation("local_write", ("out/x.txt",)), binding()) == []


def test_envelope_survives_broker_redaction_and_clip():
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    import orch_protocol as P
    from orch_protocol.redact import redact

    key = Ed25519PrivateKey.generate()
    probes = [{"probe": "read", "path": rf"C:\Users\Juliann\secret{i}\file.bin", "denied": True,
               "why": "access_denied"} for i in range(12)]
    att = I.attestation(RUNNER, {**PROBE, "probes": probes}, PROBE["sid"])
    many = I.Scan({f"out/f{i}.txt": {"sha256": "cd" * 32, "size": 9} for i in range(EV.MAX_CHANGED)}, [], [], True, True)
    line = EV.encode_envelope(payload_for(I.Scan({}, [], [], True, True), many, att=att), key.sign)
    stored = P.clip(redact(line + "\n" + "token: " + "z" * 40 + "\n" + "y" * 20_000), P.MAX_SUMMARY_CHARS)
    assert stored.split("\n", 1)[0] == line


# ------------------------------------------------------------------ vrai compte agent (opt-in)
AGENT_USER = os.environ.get("AUTOPILOT_TEST_AGENT_USER")
AGENT_CRED = os.environ.get("AUTOPILOT_TEST_AGENT_CRED")


@windows
@pytest.mark.skipif(not (AGENT_USER and AGENT_CRED), reason="compte agent de test non configuré")
def test_real_agent_account_probe_denied_by_os():
    from orch_runner.policy import load_secret

    conf = I.IsolationConf(
        agent_user=AGENT_USER, credential_file=Path(AGENT_CRED), signing_key_file=Path("unused"),
        probe_python=os.environ.get("AUTOPILOT_TEST_PROBE_PYTHON", r"C:\Program Files\Python311\python.exe"),
        deny_read=[str(Path.home() / ".orch-runner" / "runner.toml")], deny_write=[str(Path.home())],
        agent_profile=rf"C:\Users\{AGENT_USER}")
    probe = I.run_probe(conf, load_secret(conf.credential_file).decode())
    att = I.attestation(I.self_identity() | {"is_admin": False}, probe, None)
    assert probe["sid"] != I.self_identity()["sid"]
    assert all(p["denied"] and p["why"] == "access_denied" for p in probe["probes"]), probe["probes"]
    assert probe["is_admin"] is False and att["agent_sid"] == probe["sid"]


def test_more_changes_than_cap_marks_manifest_incomplete():
    files = {f"out/f{i}.txt": {"sha256": "cd" * 32, "size": 9} for i in range(EV.MAX_CHANGED + 1)}
    p = payload_for(I.Scan({}, [], [], True, True), I.Scan(files, [], [], True, True))
    assert p["manifest"]["complete"] is False
    exp = EV.Expectation("local_write", ("out/f0.txt",), ("out/*",))
    assert "manifest_incomplete" in EV.check_payload(p, exp, binding())


def test_zlib_bomb_rejected():
    import zlib

    bomb = zlib.compress(b"{" + b" " * (EV.MAX_PAYLOAD_BYTES * 4) + b"}", 9).hex()
    with pytest.raises(EV.EvidenceError) as exc:
        EV.decode_envelope(f"{EV.PREFIX}{bomb}.{'00' * 64}", lambda sig, data: None)
    assert exc.value.code == "evidence_malformed"


def test_prompt_of_other_attempt_rejected():
    p = payload_for(I.Scan({}, [], [], True, True), I.Scan({"out/x": {"sha256": "ab" * 32, "size": 1}}, [], [], True, True))
    other = hashlib.sha256(PROMPT.replace("attempt=1", "attempt=2").encode()).hexdigest()
    v = EV.check_payload(p, EV.Expectation("local_write", ("out/x",)), binding(prompt_sha256=other))
    assert "prompt_mismatch" in v


def test_unverified_agent_process_identity_rejected():
    att = I.attestation(RUNNER, PROBE, PROBE["sid"])
    att["agent_process_sid"] = None
    p = payload_for(I.Scan({}, [], [], True, True), I.Scan({"out/x": {"sha256": "ab" * 32, "size": 1}}, [], [], True, True),
                    att=att)
    assert "agent_process_identity_unverified" in EV.check_payload(p, EV.Expectation("local_write", ("out/x",)), binding())


@windows
def test_real_scan_flags_hardlink_and_trailing_dot(tmp_path):
    outside = tmp_path.parent / (tmp_path.name + "-hl.txt")
    outside.write_text("hors workspace")
    os.link(outside, tmp_path / "linked.txt")
    (tmp_path / "ok.txt").write_text("x")
    scan = I.scan_workspace(str(tmp_path))
    assert "linked.txt" in scan.suspicious and "ok.txt" not in scan.suspicious
    p = payload_for(scan, scan, mode="read_only")
    assert "suspicious_entry" in EV.check_payload(p, EV.Expectation("read"), binding(mode="read_only"))
