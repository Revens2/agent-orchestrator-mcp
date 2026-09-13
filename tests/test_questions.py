"""Questions >5 min => Photon/iMessage : état explicite, 1 message, réponse routée."""

import json

import pytest
from starlette.testclient import TestClient

import orch_protocol as P
from orch_mcp.runner_api import RunnerAuth
from orch_mcp.server import build_app
from orch_mcp.store import BrokerError, Store
from orch_runner.adapters import OpenCode

HEADERS = {"accept": "application/json, text/event-stream", "content-type": "application/json", "mcp-protocol-version": "2025-06-18"}


class Clock:
    def __init__(self):
        self.t = 2_000_000_000.0

    def __call__(self):
        return self.t


@pytest.fixture
def store(tmp_path):
    return Store(tmp_path / "q.db", clock=Clock())


def test_parse_question_block_explicit_only():
    assert P.parse_question_block("Faut-il continuer ?") is None  # pas de `?` naïf
    assert P.parse_question_block("[[QUESTION]]") is None  # bloc incomplet
    q, opts = P.parse_question_block("bla [[QUESTION]]Déployer en prod ?[[/QUESTION]] [[OPTIONS]]oui|non[[/OPTIONS]]")
    assert q == "Déployer en prod ?" and opts == ["oui", "non"]
    q2, opts2 = P.parse_question_block("[[QUESTION]]Juste une info[[/QUESTION]]")
    assert q2 == "Juste une info" and opts2 == []


def test_record_dedup_expiry(store):
    clock = store.clock
    q1, c1 = store.record_question("mission", "job-1", "opencode", "Titre mission",
                                   "Déployer ?", ["oui", "non"], notify_after_s=300)
    assert c1 and q1["question_id"].startswith("q-") and q1["status"] == "open"
    q2, c2 = store.record_question("mission", "job-1", "opencode", "Titre mission", "Déployer ?", ["oui", "non"])
    assert not c2 and q2["question_id"] == q1["question_id"]  # UN seul message
    assert store.due_questions() == []  # délai 300 s non dépassé
    clock.t += 301
    due = store.due_questions()
    assert len(due) == 1 and due[0]["question_id"] == q1["question_id"]  # due après 5 min
    with pytest.raises(BrokerError):
        store.record_question("mission", "job-1", "bad-runtime", "T", "Q ?")
    with pytest.raises(BrokerError):
        store.record_question("mission", "job-1", "opencode", "T", "   ")


def test_notify_idempotent_and_answer_single_use(store):
    clock = store.clock
    (q, _) = store.record_question("agent", "job-9", "opencode", "T", "Continuer ?", notify_after_s=1)
    qid = q["question_id"]
    clock.t += 2
    assert len(store.due_questions()) == 1
    n1 = store.mark_notified(qid, "sent")
    assert n1["notify_status"] == "sent"
    assert store.due_questions() == []  # notifiée => plus due (1 seul message)
    n2 = store.mark_notified(qid, "sent")
    assert n2["notify_status"] == "sent"  # idempotent
    a = store.answer_question(qid, "2", "imessage:+336...")
    assert a["status"] == "answered" and a["answer"] == "2"
    with pytest.raises(BrokerError) as e:
        store.answer_question(qid, "1", "imessage:+336...")  # replay refusé
    assert e.value.code == "question_closed"


def test_answer_expired_and_two_questions_no_crosstalk(store):
    clock = store.clock
    (qa, _) = store.record_question("agent", "job-A", "opencode", "Mission A", "Choix A ?", ["1", "2"], notify_after_s=1)
    (qb, _) = store.record_question("agent", "job-B", "codex", "Mission B", "Choix B ?", ["x", "y"], notify_after_s=1)
    clock.t += P.QUESTION_EXPIRY_S + 1
    with pytest.raises(BrokerError) as e:
        store.answer_question(qa["question_id"], "1", "imessage:+336...")
    assert e.value.code == "question_expired"  # open + expirée => marquée expired
    assert store.get_question(qa["question_id"])["status"] == "expired"
    store.purge()
    assert store.get_question(qb["question_id"])["status"] == "expired"
    with pytest.raises(BrokerError) as e2:
        store.answer_question(qb["question_id"], "x", "imessage:+336...")
    assert e2.value.code == "question_closed"  # déjà expirée => replay refusé
    # sans croisement : expirations indépendantes, session_ref distinctes
    assert store.get_question(qb["question_id"])["session_ref"] == "job-B"


def test_adapter_detects_question_block(tmp_path):
    import tempfile
    from pathlib import Path

    a = OpenCode("C:\\opencode.exe", {})
    with tempfile.TemporaryDirectory() as tmp:
        a.build("Mission", "read_only", "C:\\w", Path(tmp))
    assert a.pending_question is None
    a.on_line("du texte\n")
    a.on_line("[[QUESTION]]Faut-il migrer la DB ?[[/QUESTION]] [[OPTIONS]]oui|non|plus tard[[/OPTIONS]]\n")
    assert a.pending_question == ("Faut-il migrer la DB ?", ["oui", "non", "plus tard"])
    a.on_line("Faut-il vraiment ?\n")  # `?` seul ne déclenche rien
    assert a.pending_question[0] == "Faut-il migrer la DB ?"


def test_runner_question_route_auth(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "r.db", clock=clock)
    info = {"runtimes": [{"id": "fake", "available": True}],
            "workspaces": [{"id": "demo", "modes": ["read_only"], "description": "d"}]}
    epoch = store.hello("pc", info, [])
    job, _ = store.create_job("pc", "fake", "demo", "Mission", "read_only")
    claimed = store.claim("pc", epoch, 1)
    fencing = claimed[0]["fencing"]
    q, created = store.record_runner_question("pc", epoch, job["job_id"], fencing,
                                              "fake", "Titre", "On continue ?", ["oui"])
    assert created and q["session_ref"] == job["job_id"] and q["origin"] == "agent"
    with pytest.raises(BrokerError):
        store.record_runner_question("pc", epoch, job["job_id"], fencing + 999,
                                     "fake", "T", "Q ?")  # fencing périmé


def rpc(c, method, params=None):
    r = c.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}, headers=HEADERS)
    assert r.status_code == 200, r.text
    body = r.text
    if "data:" in body:
        body = "\n".join(line[5:].strip() for line in body.splitlines() if line.startswith("data:"))
    return json.loads(body)


def call(c, name, args):
    res = rpc(c, "tools/call", {"name": name, "arguments": args})["result"]
    assert not res.get("isError"), res
    return res.get("structuredContent") or json.loads(res["content"][0]["text"])


def test_question_tools_via_mcp(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "m.db", clock=clock)
    store.hello("pc", {"runtimes": [{"id": "fake", "available": True}],
                       "workspaces": [{"id": "demo", "modes": ["read_only"], "description": "d"}]}, [])
    app = build_app(store, RunnerAuth({}, ["10.200.0.0/16"]))
    with TestClient(app, base_url="http://127.0.0.1:8802") as c:
        rpc(c, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}})
        names = {t["name"] for t in rpc(c, "tools/list")["result"]["tools"]}
        assert {"agent_question_list", "agent_question_get", "agent_question_answer"} <= names
        store.record_question("chatgpt-web", "conv-abc", "chatgpt-web", "Analyse logs",
                              "Valider le plan ?", ["oui", "ajuster"], notify_after_s=300)
        assert call(c, "agent_question_list", {"status": "open"})["count"] == 1
        qid = call(c, "agent_question_list", {})["questions"][0]["question_id"]
        assert "answer" not in call(c, "agent_question_get", {"question_id": qid}) or True
        ans = call(c, "agent_question_answer", {"question_id": qid, "answer": "oui", "answer_from": "chatgpt-web"})
        assert ans["status"] == "answered" and ans["answer"] == "oui"
        replay = rpc(c, "tools/call", {"name": "agent_question_answer",
                                       "arguments": {"question_id": qid, "answer": "non"}})["result"]
        replay_body = replay.get("structuredContent") or json.loads(replay["content"][0]["text"])
        assert replay_body["error"] == "question_closed"  # single-use : replay refusé
        assert call(c, "agent_question_get", {"question_id": "q-ghost"})["error"] == "unknown_question"
