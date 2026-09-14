"""Poller Hermes v2 (deploy/hermes-poller/poller_v2.py) : supervision réelle
d'un sous-processus (Hermes simulé par un script Python), capture de sortie,
télémétrie à plat, cancel, resume, et contrat HTTP réel avec le broker.

Régression 852fa6bc : process terminé mais zombie (os.kill(pid,0) OK) ->
suivi infini, pid/session/output jamais remontés, timeout broker."""
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

import orch_protocol as P
from orch_mcp.runner_api import RunnerAuth, build_routes, parse_tokens
from orch_mcp.store import Store as BrokerStore

ROOT = Path(__file__).resolve().parents[1]
SID = "20260914_180948_8cb5f0"
FAKE_HERMES = (
    "import os,sys,time\n"
    "p=sys.stdin.read()\n"
    "print('session_id: " + SID + "',flush=True)\n"
    "time.sleep(float(os.environ.get('FAKE_SLEEP','0')))\n"
    "leak=os.environ.get('FAKE_LEAK')\n"
    "if leak=='always' or (leak=='once' and 'PAS ete execute' not in p):\n"
    "    print('<atem:function_calls>\\n<atem:invoke name=\"default.terminal\">\\n'\n"
    "          '</atem:invoke>\\n</atem:function_calls>',flush=True)\n"
    "else:\n"
    "    print('PONG_OK' if 'PONG_OK' in p or 'PAS ete execute' in p else 'NOPE',flush=True)\n"
    "sys.exit(int(os.environ.get('FAKE_EXIT','0')))\n"
)


@pytest.fixture
def pv2(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "deploy" / "hermes-poller"))
    spec = importlib.util.spec_from_file_location("poller_v2", ROOT / "deploy" / "hermes-poller" / "poller_v2.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "LOG_DIR", str(tmp_path / "runs"))
    monkeypatch.setattr(mod, "PROMPT_DIR", str(tmp_path / "prompts"))
    monkeypatch.setattr(mod, "PROC_POLL_S", 0.1)
    monkeypatch.setattr(mod, "HERMES_ARGV", [sys.executable, "-c", FAKE_HERMES])
    monkeypatch.setattr(mod, "DURABLE_LAUNCH", False)
    return mod


class _Poller:
    def __init__(self, store):
        self.store = store


def _claimed(mod, tmp_path, job_id="852fa6bc-41f5-46b0-9804-7d91224da42b", prompt="Réponds uniquement PONG_OK"):
    st = mod.Store(str(tmp_path / "poller.db"))
    st.upsert_job(job_id, 1, 1, "claimed", prompt_path=st.store_prompt(job_id, prompt))
    return st, job_id


def _run(sup, timeout=20):
    t = threading.Thread(target=sup.run, daemon=True)
    t.start()
    t.join(timeout)
    assert not t.is_alive(), "supervision bloquée (régression zombie/timeout)"


def _outbox(st):
    return [(i["kind"], json.loads(i["payload"])) for i in st.outbox_peek(100)]


def test_completed_captures_output_session_and_flat_telemetry(pv2, tmp_path):
    st, jid = _claimed(pv2, tmp_path)
    t0 = time.time()
    _run(pv2.Supervisor(_Poller(st), jid))
    assert time.time() - t0 < 10
    items = _outbox(st)
    trans = [p for k, p in items if k == "transition"]
    assert [p["to"] for p in trans] == ["starting", "running", "completed"]
    running, done = trans[1], trans[2]
    assert isinstance(running["pid"], int) and running["proc_alive"] is True
    assert "telemetry" not in running  # le broker ignore une télémétrie imbriquée
    assert done["exit_code"] == 0 and done["runtime_session_id"] == SID
    assert done["result_summary"] == "PONG_OK" and done["proc_alive"] is False
    output = "".join(p["output"] for k, p in items if k == "event")
    assert "PONG_OK" in output
    assert st.get_job(jid)["local_state"] == "done" and st.get_job(jid)["session_id"] == SID


def test_nonzero_exit_is_failed_with_output(pv2, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_EXIT", "3")
    st, jid = _claimed(pv2, tmp_path)
    _run(pv2.Supervisor(_Poller(st), jid))
    last = _outbox(st)[-1][1]
    assert last["to"] == "failed" and last["exit_code"] == 3 and "PONG_OK" in last["result_summary"]


def test_output_streamed_while_running(pv2, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_SLEEP", "2")
    st, jid = _claimed(pv2, tmp_path)
    sup = pv2.Supervisor(_Poller(st), jid)
    t = threading.Thread(target=sup.run, daemon=True)
    t.start()
    end = time.time() + 5
    seen = False
    while time.time() < end and not seen:
        seen = any(k == "event" and SID in p["output"] for k, p in _outbox(st))
        time.sleep(0.1)
    assert seen, "session/sortie non publiées pendant l'exécution"
    assert t.is_alive()
    t.join(10)
    events = [p for k, p in _outbox(st) if k == "event"]
    assert len({json.dumps(e, sort_keys=True) for e in events}) == len(events)


@pytest.mark.skipif(os.name != "posix", reason="zombie / killpg POSIX")
def test_proc_alive_false_for_zombie(pv2):
    p = subprocess.Popen([sys.executable, "-c", "pass"])
    time.sleep(1)  # terminé, non reaped -> zombie
    try:
        assert pv2.proc_alive(p.pid) is False
    finally:
        p.wait()


@pytest.mark.skipif(os.name != "posix", reason="killpg POSIX")
def test_cancel_kills_and_reports(pv2, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_SLEEP", "60")
    st, jid = _claimed(pv2, tmp_path)
    sup = pv2.Supervisor(_Poller(st), jid)
    t = threading.Thread(target=sup.run, daemon=True)
    t.start()
    time.sleep(1.5)
    sup.cancel_ev.set()
    t.join(15)
    assert not t.is_alive()
    last = _outbox(st)[-1][1]
    assert last["to"] == "cancelled" and last["runtime_session_id"] == SID
    assert not pv2.proc_alive(st.get_job(jid)["pid"])


def test_abandon_publishes_no_terminal(pv2, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_SLEEP", "3")
    st, jid = _claimed(pv2, tmp_path)
    sup = pv2.Supervisor(_Poller(st), jid)
    t = threading.Thread(target=sup.run, daemon=True)
    t.start()
    time.sleep(1)
    sup.stop_ev.set()
    t.join(10)
    assert [p["to"] for k, p in _outbox(st) if k == "transition"] == ["starting", "running"]


def test_recovery_without_process_evidence_never_respawns(pv2, tmp_path, monkeypatch):
    """Session ID seule ne prouve ni vie ni mort : aucune relance aveugle.
    Sans processus hôte ni receipt, recovery -> terminal `lost` déterministe,
    exit code non inventé, aucune relance automatique."""
    spawned = []
    real = subprocess.Popen

    def spy(argv, **kw):
        spawned.append(argv)
        return real(argv, **kw)

    monkeypatch.setattr(pv2.subprocess, "Popen", spy)
    st, jid = _claimed(pv2, tmp_path)
    st.set_job(jid, local_state="recovery", session_id=SID)
    _run(pv2.Supervisor(_Poller(st), jid))
    assert spawned == []
    trans = [p for k, p in _outbox(st) if k == "transition"]
    assert [p["to"] for p in trans] == ["lost"]
    assert trans[0]["src"] == "running"
    assert "exit_code" not in trans[0]
    assert st.get_job(jid)["local_state"] == "done"


def _spy_argvs(pv2, monkeypatch):
    argvs = []
    real = subprocess.Popen

    def spy(argv, **kw):
        argvs.append(argv)
        return real(argv, **kw)

    monkeypatch.setattr(pv2.subprocess, "Popen", spy)
    return argvs


def test_text_tool_call_resumes_session_then_completes(pv2, tmp_path, monkeypatch):
    """Régression a730531e : appel outil sérialisé en texte + exit 0 -> faux completed."""
    monkeypatch.setenv("FAKE_LEAK", "once")
    argvs = _spy_argvs(pv2, monkeypatch)
    st, jid = _claimed(pv2, tmp_path)
    _run(pv2.Supervisor(_Poller(st), jid))
    assert len(argvs) == 2 and argvs[1][-2:] == ["--resume", SID]
    trans = [p for k, p in _outbox(st) if k == "transition"]
    assert [p["to"] for p in trans] == ["starting", "running", "completed"]
    assert trans[-1]["result_summary"].endswith("PONG_OK")


def test_persistent_text_tool_call_fails_after_bounded_retries(pv2, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_LEAK", "always")
    argvs = _spy_argvs(pv2, monkeypatch)
    st, jid = _claimed(pv2, tmp_path)
    _run(pv2.Supervisor(_Poller(st), jid))
    assert len(argvs) == 1 + pv2.LEAK_RETRIES
    last = [p for k, p in _outbox(st) if k == "transition"][-1]
    assert last["to"] == "failed" and "texte" in last["error"]
    assert st.get_job(jid)["local_state"] == "done"


def test_heartbeat_held_never_invents_liveness(pv2, tmp_path):
    """Sans superviseur vivant, un pid local ne prouve rien : held minimal.
    Seul un snapshot supervisé frais porte proc_alive ; un job done avec
    outbox restante reste held pour livraison terminale bornée."""
    st, jid = _claimed(pv2, tmp_path)
    st.set_job(jid, pid=os.getpid(), local_state="running")
    poller = pv2.Poller.__new__(pv2.Poller)
    poller.store = st
    poller.supervisors = {}
    assert poller.held_payload() == [{"job_id": jid, "fencing": 1}]


def test_held_carries_fresh_supervisor_snapshot(pv2, tmp_path):
    st, jid = _claimed(pv2, tmp_path)
    st.set_job(jid, pid=os.getpid(), local_state="running")
    poller = pv2.Poller.__new__(pv2.Poller)
    poller.store = st

    class _Sup:
        snapshot = {"pid": os.getpid(), "proc_alive": True}
        last_tick = time.monotonic()
        def is_alive(self):
            return True

    sup = _Sup()
    poller.supervisors = {jid: sup}
    # Frais : superviseur vivant.
    held = poller.held_payload()
    assert held[0]["proc_alive"] is True and held[0]["supervisor_alive"] is True

    # Périmé : tick trop ancien -> supervisor_alive False (télémétrie non fraîche).
    sup.last_tick = time.monotonic() - (pv2.SUPERVISOR_FRESH_S + 1)
    held = poller.held_payload()
    assert held[0]["supervisor_alive"] is False


def test_terminal_outbox_survives_restart_without_respawn(pv2, tmp_path, monkeypatch):
    """Résultat persistant après restart : un terminal déjà en outbox avant
    crash appartient à la publication, jamais à une nouvelle exécution."""
    monkeypatch.setattr(pv2.subprocess, "Popen", lambda *a, **k: (_ for _ in ()).throw(AssertionError("respawn interdit")))
    st, jid = _claimed(pv2, tmp_path)
    st.set_job(jid, local_state="running", session_id=SID)
    st.enqueue(jid, "transition", f"tr-{jid}-completed",
               {"src": "running", "to": "completed", "exit_code": 0, "result_summary": "PONG_OK"})
    poller = pv2.Poller.__new__(pv2.Poller)
    poller.store = st
    poller.supervisors = {}
    poller.ensure_supervisor(jid)
    assert jid not in poller.supervisors
    assert st.get_job(jid)["local_state"] == "done"
    assert [p["to"] for k, p in _outbox(st) if k == "transition"] == ["completed"]


def test_e2e_contract_with_real_broker_api(pv2, tmp_path):
    """Poller (outbox) -> routes HTTP runner réelles -> store broker réel."""
    token = "h" * 48
    broker = BrokerStore(tmp_path / "broker.db")
    digests = parse_tokens(f"hermes-vps:{hashlib.sha256(token.encode()).hexdigest()}")
    client = TestClient(Starlette(routes=build_routes(broker, RunnerAuth(digests, ["10.0.0.0/8"]))))

    class HttpBroker(pv2.Broker):
        def post(self, path, body):
            r = client.post(f"/runner/v1/{path}", json={"protocol_version": P.PROTOCOL_VERSION, **body},
                            headers={"authorization": f"Bearer {token}", "x-real-ip": "10.0.0.2"})
            assert r.status_code == 200, r.text
            return r.json()

    poller = pv2.Poller.__new__(pv2.Poller)
    poller.store = pv2.Store(str(tmp_path / "poller.db"))
    poller.broker = HttpBroker(token)
    poller.supervisors = {}
    poller.epoch = poller.broker.hello(pv2.INFO, [])["epoch"]
    job, _ = broker.create_job("hermes-vps", "hermes", "vps-etude", "Réponds uniquement PONG_OK", "read_only")
    claimed = poller.broker.claim(poller.epoch, 1, wait_s=0)["jobs"]
    assert [j["job_id"] for j in claimed] == [job["job_id"]]
    jid, fencing = claimed[0]["job_id"], claimed[0]["fencing"]
    poller.store.upsert_job(jid, fencing, poller.epoch, "claimed",
                            prompt_path=poller.store.store_prompt(jid, claimed[0]["prompt"]))
    _run(pv2.Supervisor(poller, jid))
    poller.flush_outbox()
    assert poller.store.outbox_depth() == 0
    row = broker._db.execute(
        "SELECT state, exit_code, result_summary, runtime_session_id, proc_pid, output_chars FROM jobs WHERE id=?", (jid,)
    ).fetchone()
    assert row["state"] == "completed" and row["exit_code"] == 0
    assert row["result_summary"] == "PONG_OK" and row["runtime_session_id"] == SID
    assert row["proc_pid"] and row["output_chars"] > 0
