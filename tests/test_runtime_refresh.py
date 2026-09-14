"""Refresh borné des runtimes indisponibles : probe mocké, horloge injectée, aucun réseau."""

import sys
from pathlib import Path

from orch_runner import runner as R
from orch_runner.policy import Config, RuntimeConf, Workspace


class FakeClient:
    def __init__(self):
        self.hellos = []

    def post(self, path, body):
        if path == "hello":
            self.hellos.append(body["info"])
            return {"epoch": len(self.hellos)}
        return {}


def make(tmp_path: Path, probes: dict):
    ws = tmp_path / "ws"
    ws.mkdir()
    cfg = Config(
        runner_id="pc", broker_url="http://10.0.0.1", token_file=tmp_path / "t", max_parallel=2,
        workspaces={"e2e": Workspace("e2e", str(ws), ["read_only"], "")},
        runtimes={"fake": RuntimeConf("fake", True, sys.executable), "opencode": RuntimeConf("opencode", True, sys.executable)},
        home=tmp_path, runtime_refresh_min_s=60, runtime_refresh_max_s=300,
    )
    client = FakeClient()
    runner = R.Runner(cfg, client)
    calls = {k: 0 for k in probes}

    def probe(rt_id):
        calls[rt_id] += 1
        seq = probes[rt_id]
        ok = seq[min(calls[rt_id] - 1, len(seq) - 1)]
        return {"id": rt_id, "available": ok, **({} if ok else {"reason": "runtime_not_ready"})}

    runner._probe = probe
    return runner, client, calls


def avail(info, rt_id):
    return next(r["available"] for r in info["runtimes"] if r["id"] == rt_id)


def test_initial_failure_then_recovery_updates_broker(tmp_path):
    runner, client, calls = make(tmp_path, {"fake": [True], "opencode": [False, False, True]})
    runner.hello()
    assert avail(client.hellos[-1], "opencode") is False
    t0 = runner._retry["opencode"]["next"] - 60

    assert runner.refresh_runtimes(now=t0 + 30) is False  # pas encore échu : aucun probe
    assert calls["opencode"] == 1
    assert runner.refresh_runtimes(now=t0 + 60) is False  # 2e échec -> backoff doublé
    assert runner._retry["opencode"]["delay"] == 120
    assert runner.refresh_runtimes(now=t0 + 179) is False and calls["opencode"] == 2
    assert runner.refresh_runtimes(now=t0 + 180) is True
    assert "opencode" not in runner._retry and runner._resync.is_set()

    runner.hello()
    assert avail(client.hellos[-1], "opencode") is True
    assert avail(client.hellos[-1], "fake") is True
    assert calls["fake"] == 1  # runtime disponible jamais re-sondé


def test_backoff_is_capped(tmp_path):
    runner, _, _ = make(tmp_path, {"fake": [True], "opencode": [False]})
    runner.hello()
    for _ in range(10):
        runner.refresh_runtimes(now=runner._retry["opencode"]["next"])
    assert runner._retry["opencode"]["delay"] == 300


def test_heartbeat_hook_never_probes_when_all_available(tmp_path):
    runner, _, calls = make(tmp_path, {"fake": [True], "opencode": [True]})
    runner.hello()
    for _ in range(5):
        runner._maybe_refresh_runtimes()
    assert runner._refresh_thread is None and calls == {"fake": 1, "opencode": 1}
    assert not runner._resync.is_set()


def test_heartbeat_hook_runs_due_refresh_in_background(tmp_path):
    runner, client, calls = make(tmp_path, {"fake": [True], "opencode": [False, True]})
    runner.hello()
    runner._retry["opencode"]["next"] = 0  # échu
    runner._maybe_refresh_runtimes()
    runner._refresh_thread.join(timeout=5)
    assert calls["opencode"] == 2 and runner._resync.is_set()
    runner.hello()
    assert avail(client.hellos[-1], "opencode") is True


def test_probe_exception_is_treated_as_unavailable(tmp_path):
    runner, _, _ = make(tmp_path, {"fake": [True], "opencode": [False]})
    runner.hello()

    def boom(rt_id):
        raise RuntimeError("x")

    runner._probe = boom
    assert runner.refresh_runtimes(now=runner._retry["opencode"]["next"]) is False
    assert runner._retry["opencode"]["delay"] == 120
