"""Démon runner : connexion SORTANTE vers le broker (NetBird), aucun port entrant.

Boucles :
- claim (long-poll 25 s) tant qu'il reste des slots ;
- heartbeat (5 s) : présence, prolongation des bails, ordres cancel/abandon ;
- un thread par job : validations locales -> starting -> processus suspendu dans
  un Job Object -> running -> sortie bornée -> transition finale.
Réconciliation : au (re)hello le runner déclare les jobs qu'il exécute réellement ;
le broker marque `lost` ceux qu'il ne détient plus. Aucun prompt n'est relancé.
"""

from __future__ import annotations

import json
import logging
import shutil
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import httpx

import orch_protocol as P
from orch_protocol.redact import redact
from orch_runner import winproc
from orch_runner.adapters import ADAPTERS, Adapter
from orch_runner.policy import Config, PolicyError, child_env, load_token, resolve_workspace

VERSION = "0.1.0"
log = logging.getLogger("orch.runner")

ABORT_CODES = {"stale_fencing", "state_conflict", "unknown_job", "invalid_transition"}
RESYNC_CODES = {"superseded", "unknown_runner"}


class NetworkError(Exception):
    pass


class RemoteError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message = status, code, message


class BrokerClient:
    def __init__(self, base_url: str, token: str, transport: httpx.BaseTransport | None = None) -> None:
        self.http = httpx.Client(
            base_url=base_url,
            headers={"authorization": f"Bearer {token}", "user-agent": f"orch-runner/{VERSION}"},
            timeout=httpx.Timeout(10.0, read=P.CLAIM_POLL_S + 15),
            transport=transport,
            trust_env=False,  # jamais de proxy système entre le PC et le broker
        )

    def post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            r = self.http.post(f"/runner/v1/{path}", json={"protocol_version": P.PROTOCOL_VERSION, **body})
        except httpx.HTTPError as exc:
            raise NetworkError(type(exc).__name__) from exc
        try:
            data = r.json()
        except ValueError:
            data = {}
        if r.status_code >= 400:
            raise RemoteError(r.status_code, str(data.get("error", "http_error")), str(data.get("message", r.text[:200])))
        return data


class JobWorker(threading.Thread):
    def __init__(self, runner: Runner, job: dict[str, Any]) -> None:
        super().__init__(name=f"job-{job['job_id'][:8]}", daemon=True)
        self.runner = runner
        self.job = job
        self.job_id: str = job["job_id"]
        self.fencing: int = int(job["fencing"])
        self.cancel_event = threading.Event()
        self.abandon_event = threading.Event()
        self.state = P.CLAIMED
        self.proc: winproc.JobProcess | None = None
        self._out_lock = threading.Lock()
        self._out: list[str] = []
        self._activity: str | None = None
        self._sent_chars = 0
        self._stderr_tail = ""
        self.session_id: str | None = None
        self.done = threading.Event()

    # ------------------------------------------------------------- broker I/O
    def _call(self, path: str, body: dict[str, Any], deadline_s: float = 600) -> dict[str, Any] | None:
        """Retry réseau borné. Retourne None si le job doit être abandonné."""
        end = time.monotonic() + deadline_s
        delay = 1.0
        while not self.abandon_event.is_set():
            try:
                return self.runner.client.post(
                    path, {"epoch": self.runner.epoch, "job_id": self.job_id, "fencing": self.fencing, **body}
                )
            except RemoteError as exc:
                if exc.code in RESYNC_CODES:
                    self.runner.request_resync()
                elif exc.code in ABORT_CODES:
                    log.warning("job_abort job_id=%s path=%s code=%s", self.job_id, path, exc.code)
                    return None
                elif exc.status < 500:
                    log.error("job_remote_error job_id=%s path=%s %s", self.job_id, path, exc)
                    return None
            except NetworkError as exc:
                log.info("job_network_retry job_id=%s path=%s err=%s", self.job_id, path, exc)
            if time.monotonic() > end:
                return None
            time.sleep(delay)
            delay = min(delay * 2, 10)
        return None

    def _transition(self, dst: str, **fields: Any) -> bool:
        res = self._call("transition", {"from": self.state, "to": dst, "runtime_session_id": self.session_id, **fields})
        if res is None:
            return False
        log.info("job_state job_id=%s %s->%s", self.job_id, self.state, dst)
        self.state = dst
        return True

    def _flush(self, final: bool = False) -> None:
        with self._out_lock:
            text = "".join(self._out)
            self._out.clear()
            activity, self._activity = self._activity, None
        while text or activity or final:
            chunk, text = text[: P.MAX_CHUNK_CHARS], text[P.MAX_CHUNK_CHARS:]
            res = self._call(
                "event",
                {"event_id": uuid.uuid4().hex, "activity": activity, "output": chunk or None, "runtime_session_id": self.session_id},
                deadline_s=60 if not final else 120,
            )
            activity, final = None, False
            if res and res.get("cancel"):
                self.cancel_event.set()
            if res is None:
                return

    def _collect(self, stream, adapter: Adapter, is_stderr: bool) -> None:
        for raw in iter(lambda: stream.readline(1 << 16), b""):
            line = raw.decode("utf-8", errors="replace")
            if is_stderr:
                self._stderr_tail = (self._stderr_tail + line)[-4000:]
                text, activity = "[stderr] " + line, None
            else:
                try:
                    text, activity = adapter.on_line(line)
                except Exception:  # noqa: BLE001 - un parseur ne doit jamais tuer la collecte
                    text, activity = line, None
                self.session_id = adapter.session_id or self.session_id
            with self._out_lock:
                if text and self._sent_chars < P.MAX_OUTPUT_CHARS_PER_JOB:
                    self._out.append(text)
                    self._sent_chars += len(text)
                if activity:
                    self._activity = activity

    # --------------------------------------------------------------- lifecycle
    def run(self) -> None:
        try:
            self._run()
        except Exception:
            log.exception("job_crash job_id=%s", self.job_id)
            if self.proc:
                self.proc.kill_tree()
            if self.state in (P.CLAIMED, P.STARTING, P.RUNNING):
                self._transition(P.FAILED, error="erreur interne du runner")
        finally:
            if self.proc:
                self.proc.close()
            self.done.set()
            self.runner.job_finished(self)

    def _refuse(self, code: str, message: str) -> None:
        log.warning("job_refused job_id=%s code=%s", self.job_id, code)
        self._transition(P.FAILED, error=f"{code}: {message}")

    def _run(self) -> None:
        cfg = self.runner.config
        job = self.job
        if self.cancel_event.is_set():
            self._transition(P.CANCELLED, error="annulé avant lancement")
            return
        # --- validations locales (le broker n'est pas cru sur parole)
        rt = cfg.runtimes.get(job["runtime"])
        if rt is None or not rt.enabled or job["runtime"] not in ADAPTERS:
            return self._refuse("runtime_denied", "runtime non autorisé localement")
        ws = cfg.workspaces.get(job["workspace_id"])
        if ws is None:
            return self._refuse("workspace_denied", "workspace absent de l'allowlist locale")
        adapter = ADAPTERS[job["runtime"]](rt.exe, rt.extra)
        if job["mode"] not in ws.modes or job["mode"] not in adapter.modes:
            return self._refuse("mode_denied", "mode non autorisé pour ce workspace/runtime")
        prompt = job.get("prompt")
        if not isinstance(prompt, str) or not prompt or "\x00" in prompt or len(prompt) > P.MAX_PROMPT_CHARS:
            return self._refuse("invalid_prompt", "prompt invalide")
        try:
            cwd = resolve_workspace(ws)
        except PolicyError as exc:
            return self._refuse(exc.code, exc.message)

        if not self._transition(P.STARTING):
            return
        tmpdir = cfg.home / "jobs" / self.job_id
        tmpdir.mkdir(parents=True, exist_ok=True)
        try:
            launch = adapter.build(prompt, job["mode"], cwd, tmpdir)
            self.proc = winproc.JobProcess(launch.argv, cwd, child_env(self.job_id, launch.env_extra))
        except (winproc.LaunchError, ValueError, OSError) as exc:
            shutil.rmtree(tmpdir, ignore_errors=True)
            return self._transition(P.FAILED, error=f"launch_failed: {exc}") and None
        log.info("job_spawned job_id=%s runtime=%s workspace=%s pid=%s", self.job_id, job["runtime"], job["workspace_id"], self.proc.pid)

        if self.cancel_event.is_set() or self.abandon_event.is_set():
            self.proc.kill_tree()  # jamais repris : le processus n'a pas exécuté une instruction
            self._transition(P.CANCELLED, error="annulé pendant le démarrage (processus jamais repris)")
            shutil.rmtree(tmpdir, ignore_errors=True)
            return
        if not self._transition(P.RUNNING):
            self.proc.kill_tree()
            return
        self.proc.resume()

        def feed() -> None:
            try:
                if launch.stdin_text is not None:
                    self.proc.proc.stdin.write(launch.stdin_text.encode("utf-8"))
                self.proc.proc.stdin.close()
            except OSError:
                pass

        readers = [
            threading.Thread(target=feed, daemon=True),
            threading.Thread(target=self._collect, args=(self.proc.proc.stdout, adapter, False), daemon=True),
            threading.Thread(target=self._collect, args=(self.proc.proc.stderr, adapter, True), daemon=True),
        ]
        for t in readers:
            t.start()

        started = time.monotonic()
        reason = None
        last_flush = 0.0
        while self.proc.proc.poll() is None:
            if self.abandon_event.is_set():
                self.proc.kill_tree()
                log.warning("job_abandoned job_id=%s (broker ne reconnaît plus le job)", self.job_id)
                shutil.rmtree(tmpdir, ignore_errors=True)
                return
            if self.cancel_event.is_set() and reason is None:
                reason = P.CANCELLED
                self.proc.kill_tree(1)
            elif time.monotonic() - started > int(job["timeout_s"]) and reason is None:
                reason = P.TIMEOUT
                self.proc.kill_tree(1)
            if time.monotonic() - last_flush >= 1.0:
                self._flush()
                last_flush = time.monotonic()
            time.sleep(0.2)
        exit_code = self.proc.proc.returncode
        for t in readers[1:]:
            t.join(timeout=10)
        self._flush()
        outcome = adapter.finish(exit_code, tmpdir)
        self.session_id = adapter.session_id or self.session_id
        self.proc.close()  # tue d'éventuels descendants restants
        shutil.rmtree(tmpdir, ignore_errors=True)
        summary = outcome.summary
        if reason == P.CANCELLED:
            self._transition(P.CANCELLED, exit_code=exit_code, result_summary=summary, error="annulé : arbre de processus terminé")
        elif reason == P.TIMEOUT:
            self._transition(P.TIMEOUT, exit_code=exit_code, result_summary=summary, error=f"timeout {job['timeout_s']} s : arbre terminé")
        elif outcome.ok and exit_code == 0:
            self._transition(P.COMPLETED, exit_code=0, result_summary=summary)
        else:
            err = outcome.error or f"exit code {exit_code}"
            if self._stderr_tail:
                err += " | stderr: " + redact(self._stderr_tail[-800:])
            self._transition(P.FAILED, exit_code=exit_code, result_summary=summary, error=err)
        log.info("job_finished job_id=%s state=%s exit=%s duration_s=%.1f", self.job_id, self.state, exit_code, time.monotonic() - started)


class Runner:
    def __init__(self, config: Config, client: BrokerClient | None = None) -> None:
        self.config = config
        self.client = client or BrokerClient(config.broker_url, load_token(config.token_file))
        self.epoch = -1
        self.workers: dict[str, JobWorker] = {}
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self._resync = threading.Event()
        self.last_ok = time.monotonic()
        self.connected = False
        self._info_cache: dict[str, Any] | None = None

    # ---------------------------------------------------------------- session
    def info(self) -> dict[str, Any]:
        runtimes = []
        for rt_id, rt in self.config.runtimes.items():
            if rt.enabled and rt_id in ADAPTERS:
                runtimes.append(ADAPTERS[rt_id](rt.exe, rt.extra).probe())
        workspaces = []
        for ws in self.config.workspaces.values():
            try:
                resolve_workspace(ws)
            except PolicyError as exc:
                log.warning("workspace_invalid id=%s code=%s", ws.id, exc.code)
                continue
            workspaces.append({"id": ws.id, "modes": ws.modes, "description": ws.description})
        return {"version": VERSION, "max_parallel": self.config.max_parallel, "runtimes": runtimes, "workspaces": workspaces}

    def held(self) -> list[dict[str, Any]]:
        with self.lock:
            return [{"job_id": w.job_id, "fencing": w.fencing} for w in self.workers.values() if not w.done.is_set()]

    def hello(self) -> None:
        if self._info_cache is None:
            self._info_cache = self.info()
        res = self.client.post("hello", {"info": self._info_cache, "held": self.held()})
        self.epoch = int(res["epoch"])
        self.last_ok = time.monotonic()
        if not self.connected:
            log.info("runner_connected epoch=%s", self.epoch)
        self.connected = True
        self._resync.clear()

    def request_resync(self) -> None:
        self._resync.set()

    def job_finished(self, worker: JobWorker) -> None:
        with self.lock:
            self.workers.pop(worker.job_id, None)

    # ------------------------------------------------------------------ loops
    def heartbeat_loop(self) -> None:
        while not self.stop_event.wait(P.HEARTBEAT_S):
            try:
                if self._resync.is_set():
                    self.hello()
                    continue
                res = self.client.post("heartbeat", {"epoch": self.epoch, "held": self.held()})
                self.last_ok = time.monotonic()
                with self.lock:
                    for job_id in res.get("cancel", []):
                        if job_id in self.workers:
                            log.info("job_cancel_received job_id=%s", job_id)
                            self.workers[job_id].cancel_event.set()
                    for job_id in res.get("abandon", []):
                        if job_id in self.workers:
                            self.workers[job_id].abandon_event.set()
            except RemoteError as exc:
                if exc.code in RESYNC_CODES:
                    self._resync.set()
                else:
                    log.error("heartbeat_error %s", exc)
            except NetworkError as exc:
                if self.connected:
                    log.warning("runner_disconnected err=%s", exc)
                self.connected = False
                self._resync.set()
            offline = time.monotonic() - self.last_ok
            if offline > self.config.offline_kill_s:
                with self.lock:
                    for w in self.workers.values():
                        if not w.abandon_event.is_set():
                            log.warning("job_offline_abandon job_id=%s offline_s=%.0f", w.job_id, offline)
                            w.abandon_event.set()
            self._write_status()

    def claim_loop(self) -> None:
        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                if self.epoch < 0 or self._resync.is_set():
                    self.hello()
                with self.lock:
                    free = self.config.max_parallel - len(self.workers)
                if free <= 0:
                    self.stop_event.wait(1.0)
                    continue
                res = self.client.post("claim", {"epoch": self.epoch, "free_slots": free, "wait_s": P.CLAIM_POLL_S})
                self.last_ok = time.monotonic()
                backoff = 1.0
                for job in res.get("jobs", []):
                    worker = JobWorker(self, job)
                    with self.lock:
                        self.workers[worker.job_id] = worker
                    log.info("job_claimed job_id=%s runtime=%s workspace=%s", worker.job_id, job["runtime"], job["workspace_id"])
                    worker.start()
            except RemoteError as exc:
                if exc.code in RESYNC_CODES:
                    self._resync.set()
                else:
                    log.error("claim_error %s", exc)
                self.stop_event.wait(backoff)
                backoff = min(backoff * 2, 60)
            except NetworkError as exc:
                if self.connected:
                    log.warning("runner_disconnected err=%s", exc)
                self.connected = False
                self._resync.set()
                self.stop_event.wait(backoff)
                backoff = min(backoff * 2, 60)

    def _write_status(self) -> None:
        status = {
            "runner_id": self.config.runner_id,
            "connected": self.connected,
            "epoch": self.epoch,
            "seconds_since_broker_ok": round(time.monotonic() - self.last_ok, 1),
            "active_jobs": [h["job_id"] for h in self.held()],
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
        path = Path(self.config.home) / "status.json"
        tmp = path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(status, indent=1), encoding="utf-8")
            tmp.replace(path)
        except OSError:
            pass

    def run(self) -> None:
        # Au démarrage aucun job n'est détenu : les dossiers temporaires restants viennent
        # d'un runner tué (les processus sont morts avec leur Job Object).
        jobs_dir = Path(self.config.home) / "jobs"
        if jobs_dir.is_dir():
            for leftover in jobs_dir.iterdir():
                shutil.rmtree(leftover, ignore_errors=True)
        self._info_cache = self.info()
        hb = threading.Thread(target=self.heartbeat_loop, name="heartbeat", daemon=True)
        hb.start()
        try:
            self.claim_loop()
        finally:
            self.stop_event.set()
            with self.lock:
                for w in self.workers.values():
                    w.abandon_event.set()

    def stop(self) -> None:
        self.stop_event.set()
