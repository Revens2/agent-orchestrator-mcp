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
    assert len(argvs) == 2  # generation initiale + 1 relance bornee
    if pv2.DURABLE_LAUNCH:
        descs = sorted(Path(pv2.LOG_DIR).glob("*.launch.json"))
        assert len(descs) == 2
        resume_argv = json.loads(descs[1].read_text())["argv"]
    else:
        resume_argv = argvs[1]
    assert resume_argv[-2:] == ["--resume", SID]
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


def _claimed_ws(mod, tmp_path, job_id, workspace="vps-etude", mode="read_only",
                prompt="Réponds uniquement PONG_OK"):
    st = mod.Store(str(tmp_path / "poller.db"))
    st.upsert_job(job_id, 1, 1, "claimed", prompt_path=st.store_prompt(job_id, prompt),
                  workspace_id=workspace, mode=mode)
    return st, job_id


def test_argv_pins_dedicated_profile(pv2, monkeypatch):
    """L'argv docker ancre le profil dedie du slot avant `chat`."""
    monkeypatch.setattr(pv2, "HERMES_ARGV",
                        ["docker", "exec", "-i", "hermes", "hermes", "chat",
                         "--query-file", "-", "-Q"])
    argv = pv2.hermes_argv_for_slot("orch-slot-03")
    assert argv[argv.index("hermes", 4) + 1:argv.index("chat")] == ["-p", "orch-slot-03"]
    assert pv2.profile_home_for_slot("orch-slot-03") == "/opt/data/profiles/orch-slot-03"
    assert pv2.profile_home_for_slot(None) == "/opt/data"


def test_two_concurrent_jobs_get_distinct_slots(pv2, tmp_path):
    """Deux jobs simultanes : slots distincts, logs separes, slots liberes."""
    st, a = _claimed_ws(pv2, tmp_path, "job-A-isole", mode="read_only")
    st.upsert_job("job-B-isole", 1, 1, "claimed",
                  prompt_path=st.store_prompt("job-B-isole", "Réponds uniquement PONG_OK"),
                  workspace_id="vps-etude", mode="read_only")
    ta = threading.Thread(target=pv2.Supervisor(_Poller(st), a).run, daemon=True)
    tb = threading.Thread(target=pv2.Supervisor(_Poller(st), "job-B-isole").run, daemon=True)
    ta.start()
    tb.start()
    ta.join(20)
    tb.join(20)
    assert not ta.is_alive() and not tb.is_alive()
    assert st.get_job(a)["local_state"] == "done"
    assert st.get_job("job-B-isole")["local_state"] == "done"
    # Slots liberes seulement a la fin (release on done) : plus rien d'alloue.
    assert st.slot_map() == {}
    assert len(st.free_slots()) == len(pv2.PROFILE_POOL)
    logs = sorted(os.listdir(pv2.LOG_DIR))
    assert any(x.startswith("job-A-is") for x in logs)
    assert any(x.startswith("job-B-is") for x in logs)


def test_workspace_write_serialized_same_workspace(pv2, tmp_path, monkeypatch):
    """Deux writes meme workspace : jamais ensemble, le second attend en claimed."""
    monkeypatch.setenv("FAKE_SLEEP", "3")
    st, a = _claimed_ws(pv2, tmp_path, "job-W1", mode="workspace_write")
    st.upsert_job("job-W2", 1, 1, "claimed",
                  prompt_path=st.store_prompt("job-W2", "Réponds uniquement PONG_OK"),
                  workspace_id="vps-etude", mode="workspace_write")
    poller = pv2.Poller.__new__(pv2.Poller)
    poller.store = st
    poller.supervisors = {}
    ta = threading.Thread(target=pv2.Supervisor(poller, a).run, daemon=True)
    ta.start()
    time.sleep(1.5)  # A tourne (slot pris)
    assert st.get_slot(a) is not None
    ok_b, why_b = st.gate("job-W2")
    assert not ok_b and why_b.startswith("workspace_write-verrouille")
    poller.ensure_supervisor("job-W2")  # ne doit rien spawner
    assert "job-W2" not in poller.supervisors
    assert st.get_job("job-W2").get("pid") is None
    assert st.get_job("job-W2")["local_state"] == "claimed"  # lease tenu par held
    ta.join(15)
    assert st.get_job(a)["local_state"] == "done"
    ok_b, _ = st.gate("job-W2")
    assert ok_b  # verrou leve : le second peut partir
    _run(pv2.Supervisor(poller, "job-W2"))
    assert st.get_job("job-W2")["local_state"] == "done"


def test_read_only_same_workspace_stays_concurrent(pv2, tmp_path, monkeypatch):
    """Deux read_only meme workspace : concurrence autorisee."""
    monkeypatch.setenv("FAKE_SLEEP", "2")
    st, a = _claimed_ws(pv2, tmp_path, "job-R1", mode="read_only")
    st.upsert_job("job-R2", 1, 1, "claimed",
                  prompt_path=st.store_prompt("job-R2", "Réponds uniquement PONG_OK"),
                  workspace_id="vps-etude", mode="read_only")
    sa = pv2.Supervisor(_Poller(st), a)
    sb = pv2.Supervisor(_Poller(st), "job-R2")
    ta = threading.Thread(target=sa.run, daemon=True)
    tb = threading.Thread(target=sb.run, daemon=True)
    ta.start()
    time.sleep(1.0)
    tb.start()
    time.sleep(0.5)
    # Les deux tiennent un slot DIFFERENT en meme temps.
    assert st.get_slot(a) is not None and st.get_slot("job-R2") is not None
    assert st.get_slot(a) != st.get_slot("job-R2")
    ta.join(15)
    tb.join(15)
    assert st.get_job(a)["local_state"] == "done"
    assert st.get_job("job-R2")["local_state"] == "done"


def test_cancel_one_leaves_other_running(pv2, tmp_path, monkeypatch):
    """Cancel d'un job : l'autre poursuit et termine normalement."""
    monkeypatch.setenv("FAKE_SLEEP", "60")
    st, a = _claimed_ws(pv2, tmp_path, "job-C1", mode="read_only")
    st.upsert_job("job-C2", 1, 1, "claimed",
                  prompt_path=st.store_prompt("job-C2", "Réponds uniquement PONG_OK"),
                  workspace_id="vps-etude", mode="read_only")
    sa = pv2.Supervisor(_Poller(st), a)
    sb = pv2.Supervisor(_Poller(st), "job-C2")
    ta = threading.Thread(target=sa.run, daemon=True)
    tb = threading.Thread(target=sb.run, daemon=True)
    ta.start()
    tb.start()
    time.sleep(1.5)
    sa.cancel_ev.set()  # cancel broker -> seulement A
    ta.join(15)
    assert not ta.is_alive() and tb.is_alive()
    assert st.get_job(a)["local_state"] == "done"
    assert st.get_slot(a) is None  # slot de A libere
    assert st.get_slot("job-C2") is not None  # B garde le sien
    sb.cancel_ev.set()
    tb.join(15)
    assert st.get_job("job-C2")["local_state"] == "done"
    assert st.slot_map() == {}


def test_restart_keeps_slot_no_double_spawn(pv2, tmp_path, monkeypatch):
    """Restart/recovery : meme slot, pas de second spawn (generation inchangee)."""
    monkeypatch.setenv("FAKE_SLEEP", "4")
    st, jid = _claimed_ws(pv2, tmp_path, "job-restart-1", mode="read_only")
    sup = pv2.Supervisor(_Poller(st), jid)
    t = threading.Thread(target=sup.run, daemon=True)
    t.start()
    time.sleep(1.5)
    slot_before = st.get_slot(jid)
    assert slot_before is not None
    gen_before = st.get_meta("launch:" + jid)
    # Simule un restart du poller : nouveau handle Store sur la meme DB.
    st2 = pv2.Store(str(tmp_path / "poller.db"))
    assert st2.get_slot(jid) == slot_before  # allocation conservee
    assert st2.gate(jid) == (True, "slot-conserve")
    sup2 = pv2.Supervisor(_Poller(st2), jid)
    t2 = threading.Thread(target=sup2.run, daemon=True)
    t2.start()  # recovery : reattache, ne respawn pas
    t.join(15)
    t2.join(15)
    assert st2.get_meta("launch:" + jid) == gen_before  # aucun double spawn
    done = [p for k, p in _outbox(st2) if k == "transition" and p.get("to") == "completed"]
    assert len(done) == 1  # terminal idempotent malgre deux observateurs
    assert st2.get_job(jid)["local_state"] == "done"


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


def test_e2e_two_jobs_real_broker_isolated_slots(pv2, tmp_path):
    """Matrice E2E : 2 jobs claimés ensemble -> slots distincts, 2 completed."""
    token = "k" * 48
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
    for i in range(2):
        broker.create_job("hermes-vps", "hermes", "vps-etude",
                          "Réponds uniquement PONG_OK %d" % i, "read_only")
    claimed = poller.broker.claim(poller.epoch, 2, wait_s=0)["jobs"]
    assert len(claimed) == 2
    for job in claimed:
        poller.store.upsert_job(job["job_id"], job["fencing"], poller.epoch, "claimed",
                                prompt_path=poller.store.store_prompt(job["job_id"], job["prompt"]),
                                workspace_id=job.get("workspace_id"),
                                mode=job.get("mode") or "read_only")
    threads = []
    for job in claimed:
        t = threading.Thread(target=pv2.Supervisor(poller, job["job_id"]).run, daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join(20)
        assert not t.is_alive()
    slots = [poller.store.get_slot(j["job_id"]) for j in claimed]
    # Slots liberes apres done ; pendant l'execution ils etaient distincts
    # (prouve par test_read_only_same_workspace_stays_concurrent).
    poller.flush_outbox()
    assert poller.store.outbox_depth() == 0
    rows = broker._db.execute("SELECT id, state FROM jobs").fetchall()
    assert sorted(r[1] for r in rows) == ["completed", "completed"]
    assert poller.store.slot_map() == {}
    assert slots == [None, None]  # releases post-done, pas de fuite
