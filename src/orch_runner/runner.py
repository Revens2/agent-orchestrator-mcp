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
import subprocess
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

VERSION = "0.2.0"
log = logging.getLogger("orch.runner")


def _best_effort(fn):
    """Exécute fn ; None en cas d'échec (télémétrie : absent plutôt qu'inventé)."""
    try:
        return fn()
    except Exception:  # noqa: BLE001 - télémétrie best-effort
        return None


def _git_snapshot(path: str) -> dict[str, Any] | None:
    """Snapshot git borné d'un workspace (branch/HEAD/dirty), ou None si non
    observable (pas un dépôt, git absent, timeout). Jamais de secret : les trois
    champs seuls, sorties bornées, échec silencieux -> None."""
    import os as _os

    def _run(args: list[str]) -> str | None:
        try:
            out = subprocess.run(
                ["git", *args], cwd=path, capture_output=True, text=True, timeout=10,
                creationflags=0x08000000 if _os.name == "nt" else 0,
                encoding="utf-8", errors="replace",
            )
        except Exception:  # noqa: BLE001 - snapshot best-effort : échec silencieux -> None
            return None
        if out.returncode != 0:
            return None
        return (out.stdout or "").strip()[:4_000]

    branch = _run(["branch", "--show-current"])
    if branch is None:
        return None  # pas un dépôt git (ou git indisponible) : on omet le workspace
    head = _run(["rev-parse", "HEAD"])
    porcelain = _run(["status", "--porcelain"])
    return {
        "branch": (branch or None),
        "head": (head[:40] if head else None),
        "dirty": bool(porcelain) if porcelain is not None else None,
    }

ABORT_CODES = {"stale_fencing", "state_conflict", "unknown_job", "invalid_transition"}
RESYNC_CODES = {"superseded", "unknown_runner"}

# Commande locale de reprise par runtime (affichée à ChatGPT Web avec l'ID).
_RESUME_HINTS = {
    "claude-code": "claude --resume {sid}",
    "agy": "agy --conversation {sid}",
    "opencode": "opencode run --session {sid}",
    "codex": "codex resume {sid}",
}


def with_session_header(runtime: str, session_id: str | None, summary: str | None) -> str | None:
    """Préfixe le résumé final par l'ID de session runtime (première ligne).

    ChatGPT Web ne voit que le texte du résultat, pas le champ
    `runtime_session_id` déjà envoyé au broker : sans cet en-tête, l'ID
    permettant de reprendre la conversation en local est perdu pour
    l'utilisateur. L'en-tête est en tête car le broker clippe le résumé
    à MAX_SUMMARY_CHARS (la fin peut être coupée, jamais le début).
    """
    if not session_id:
        return summary
    hint = _RESUME_HINTS.get(runtime, "")
    header = f"[session {runtime}: {session_id}" + (f" | reprise: {hint.format(sid=session_id)}]" if hint else "]")
    if not summary:
        return header
    return f"{header}\n---\n{summary}"


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
        self._tool: str | None = None  # outil courant observé ("tool: X" des adapters), sinon None
        self.proc_started_at: float | None = None  # time.time() à la création du processus
        self._sent_chars = 0
        self._stderr_tail = ""
        self.session_id: str | None = None
        self.done = threading.Event()
        self._question_sent = False

    # ------------------------------------------------------------- broker I/O
    def _telemetry(self) -> dict[str, Any]:
        """Snapshot best-effort joint aux transitions et events (clés absentes =
        non observé, jamais inventé ; le broker normalise et horodate). Seules
        les valeurs observées sont renvoyées : le broker conserve le dernier
        connu (âge lisible via telemetry_age_s)."""
        snap: dict[str, Any] = {}
        if self.proc is not None:
            alive = _best_effort(lambda: self.proc.proc.poll() is None)
            if alive is not None:
                snap["proc_alive"] = alive
            pid = _best_effort(lambda: self.proc.pid)
            if pid is not None:
                snap["pid"] = pid
            if self.proc_started_at is not None:
                snap["proc_started_at"] = self.proc_started_at
            children = _best_effort(self.proc.active_processes)
            if children is not None:
                snap["child_procs"] = children
        if self._tool:
            snap["tool"] = self._tool
        return snap

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
        res = self._call("transition", {"from": self.state, "to": dst, "runtime_session_id": self.session_id, **self._telemetry(), **fields})
        if res is None:
            return False
        log.info("job_state job_id=%s %s->%s", self.job_id, self.state, dst)
        self.state = dst
        return True

    def _send_question(self, adapter: Adapter) -> None:
        """Remonte UNE question explicite [[QUESTION]] vers le broker (idempotent
        côté broker par fingerprint : un seul enregistrement, un seul message)."""
        pending = getattr(adapter, "pending_question", None)
        if not pending or self._question_sent:
            return
        self._question_sent = True
        question, options = pending
        res = self._call(
            "question",
            {
                "runtime": self.job.get("runtime", ""),
                "title": P.display_title(self.job.get("prompt"), fallback=f"job {self.job_id[:8]}"),
                "question": question,
                "options": options,
            },
            deadline_s=60,
        )
        log.info("job_question job_id=%s sent=%s", self.job_id, bool(res and res.get("question")))

    def _flush(self, final: bool = False) -> None:
        with self._out_lock:
            text = "".join(self._out)
            self._out.clear()
            activity, self._activity = self._activity, None
        tele = self._telemetry()
        while text or activity or final:
            chunk, text = text[: P.MAX_CHUNK_CHARS], text[P.MAX_CHUNK_CHARS:]
            res = self._call(
                "event",
                {"event_id": uuid.uuid4().hex, "activity": activity, "output": chunk or None, "runtime_session_id": self.session_id, **tele},
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
                    if activity.startswith("tool:"):
                        self._tool = redact(activity[5:].strip())[:120] or None

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
        adapter = ADAPTERS[job["runtime"]](rt.exe, rt.extra, cfg.permission_policy)
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
            self.proc_started_at = time.time()
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
                self._send_question(adapter)
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
        # L'ID de session part en tête du résumé : c'est le seul canal que
        # ChatGPT Web voit (le champ runtime_session_id ne lui est pas relayé).
        summary = with_session_header(job["runtime"], self.session_id, outcome.summary)
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
        # runtime_id -> {"next": monotonic, "delay": s} : seuls les runtimes en échec y figurent.
        self._retry: dict[str, dict[str, float]] = {}
        self._refresh_thread: threading.Thread | None = None

    # ---------------------------------------------------------------- session
    def _probe(self, rt_id: str) -> dict[str, Any]:
        rt = self.config.runtimes[rt_id]
        return ADAPTERS[rt_id](rt.exe, rt.extra, self.config.permission_policy).probe()

    def _schedule_retry(self, rt_id: str, now: float) -> None:
        prev = self._retry.get(rt_id)
        lo, hi = self.config.runtime_refresh_min_s, max(self.config.runtime_refresh_min_s, self.config.runtime_refresh_max_s)
        delay = float(lo) if prev is None else min(prev["delay"] * 2, float(hi))
        self._retry[rt_id] = {"next": now + delay, "delay": delay}

    def refresh_runtimes(self, now: float | None = None) -> bool:
        """Re-probe les runtimes indisponibles dont l'échéance est passée. Si l'un
        redevient disponible, met à jour l'info annoncée et force un hello. Les
        runtimes disponibles ne sont jamais re-sondés. Retourne True si l'info a changé."""
        now = time.monotonic() if now is None else now
        with self.lock:
            due = [rt_id for rt_id, st in self._retry.items() if st["next"] <= now]
        recovered: dict[str, dict[str, Any]] = {}
        for rt_id in due:
            try:
                entry = self._probe(rt_id)
            except Exception:  # noqa: BLE001 - un probe ne doit jamais tuer la boucle
                entry = {"available": False}
            with self.lock:
                if entry.get("available"):
                    self._retry.pop(rt_id, None)
                    recovered[rt_id] = entry
                else:
                    self._schedule_retry(rt_id, now)
                    log.info("runtime_still_unavailable id=%s next_in_s=%.0f", rt_id, self._retry[rt_id]["delay"])
        if not recovered:
            return False
        with self.lock:
            if self._info_cache is not None:
                runtimes = [recovered.get(r.get("id"), r) for r in self._info_cache.get("runtimes", [])]
                self._info_cache = {**self._info_cache, "runtimes": runtimes}
        for rt_id in recovered:
            log.info("runtime_recovered id=%s", rt_id)
        self._resync.set()  # le hello suivant annonce la nouvelle info au broker
        return True

    def _maybe_refresh_runtimes(self) -> None:
        """Appelé par le heartbeat : ne sonde rien lui-même ; lance au plus un
        thread de refresh, seulement si un runtime en échec est arrivé à échéance."""
        now = time.monotonic()
        with self.lock:
            if not any(st["next"] <= now for st in self._retry.values()):
                return
            if self._refresh_thread is not None and self._refresh_thread.is_alive():
                return
            self._refresh_thread = threading.Thread(target=self.refresh_runtimes, name="runtime-refresh", daemon=True)
            self._refresh_thread.start()

    def info(self) -> dict[str, Any]:
        runtimes = []
        now = time.monotonic()
        for rt_id, rt in self.config.runtimes.items():
            if rt.enabled and rt_id in ADAPTERS:
                entry = self._probe(rt_id)
                runtimes.append(entry)
                with self.lock:
                    if entry.get("available"):
                        self._retry.pop(rt_id, None)
                    else:
                        self._schedule_retry(rt_id, now)
        workspaces = []
        env: dict[str, Any] = {}
        for ws in self.config.workspaces.values():
            try:
                path = resolve_workspace(ws)
            except PolicyError as exc:
                log.warning("workspace_invalid id=%s code=%s", ws.id, exc.code)
                continue
            workspaces.append({"id": ws.id, "modes": ws.modes, "description": ws.description})
            snap = _git_snapshot(path)
            if snap is not None:
                env[ws.id] = snap
        return {"version": VERSION, "max_parallel": self.config.max_parallel, "runtimes": runtimes, "workspaces": workspaces, "env": env}

    def held(self) -> list[dict[str, Any]]:
        """Jobs réellement détenus + télémétrie d'exécution (pid, vivant, enfants,
        outil courant). Champs absents = non observés ; le broker les stocke NULL."""
        with self.lock:
            out = []
            for w in self.workers.values():
                if w.done.is_set():
                    continue
                entry: dict[str, Any] = {"job_id": w.job_id, "fencing": w.fencing}
                if w.proc is not None:
                    # Snapshot unique (_telemetry) : seules les valeurs observées
                    # sont envoyées (le broker conserve le dernier connu).
                    entry.update(w._telemetry())
                out.append(entry)
            return out

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
            self._maybe_refresh_runtimes()
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
