"""Démon runner : connexion SORTANTE vers le broker (NetBird), aucun port entrant.

Boucles :
- claim (long-poll 25 s) tant qu'il reste des slots ;
- heartbeat (5 s) : présence, prolongation des bails, ordres cancel/abandon ;
- un thread par job : validations locales -> starting -> processus suspendu dans
  un Job Object -> running -> sortie bornée -> transition finale.
Réconciliation : au (re)hello le runner déclare les jobs qu'il exécute réellement
PLUS son journal local de reprise (`recovering:true`) ; le broker parque (`suspended`,
grâce 4 h) au lieu de marquer `lost` aussitôt. Aucun prompt workspace_write n'est
relancé aveuglément : reprise de session du runtime si possible, sinon parking
explicite (bail entretenu, décision humaine). Pas de mise à mort sur perte réseau :
le processus local continue, la sortie est bufferisée et flushée au retour.
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
from orch_runner import recovery, winproc
from orch_runner.adapters import ADAPTERS, Adapter
from orch_runner.policy import Config, PolicyError, child_env, load_token, resolve_workspace

VERSION = "0.3.0"
log = logging.getLogger("orch.runner")


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
    def __init__(self, runner: Runner, job: dict[str, Any], initial_state: str = P.CLAIMED,
                 resume_launch: Any | None = None, resumed: bool = False) -> None:
        super().__init__(name=f"job-{job['job_id'][:8]}", daemon=True)
        self.runner = runner
        self.job = job
        self.job_id: str = job["job_id"]
        self.fencing: int = int(job["fencing"])
        self.cancel_event = threading.Event()
        self.abandon_event = threading.Event()
        self.state = initial_state
        self._resume_launch = resume_launch  # Launch déjà construit (reprise), ou None
        self._resumed = resumed  # True si le processus continue un job déjà running côté broker
        self.proc: winproc.JobProcess | None = None
        self._out_lock = threading.Lock()
        self._out: list[str] = []
        self._activity: str | None = None
        self._tool: str | None = None  # outil courant observé ("tool: X" des adapters), sinon None
        self.proc_started_at: float | None = None  # time.time() à la création du processus
        self._sent_chars = 0
        self._last_recovery_save = 0.0
        self._stderr_tail = ""
        self.last_remote_code: str | None = None
        self.session_id: str | None = None
        if isinstance(job.get("runtime_session_id"), str):
            self.session_id = job["runtime_session_id"] or None
        self.done = threading.Event()

    # ------------------------------------------------------------- journal local
    def _save_recovery(self, phase: str, force: bool = False) -> None:
        """Persiste le journal avant tout point de non-retour. Best-effort."""
        now_t = time.monotonic()
        if not force and now_t - self._last_recovery_save < 5.0:
            return
        self._last_recovery_save = now_t
        try:
            recovery.save(
                self.runner.config.home,
                recovery.build_record(
                    job=self.job,
                    phase=phase,
                    prompt=self.job.get("prompt") or "",
                    session_id=self.session_id,
                    pid=self.proc.pid if self.proc is not None else None,
                    proc_started_at=self.proc_started_at,
                    sent_chars=self._sent_chars,
                ),
            )
        except Exception:  # le journal ne doit jamais tuer le job
            log.exception("job_recovery_save_failed job_id=%s", self.job_id)

    def _clear_recovery(self) -> None:
        try:
            recovery.clear(self.runner.config.home, self.job_id)
        except Exception:  # noqa: BLE001, S110 - best-effort silencieux
            pass

    # ------------------------------------------------------------- broker I/O
    def _call(self, path: str, body: dict[str, Any], deadline_s: float = 600) -> dict[str, Any] | None:
        """Retry réseau borné. Retourne None si le job doit être abandonné."""
        end = time.monotonic() + deadline_s
        delay = 1.0
        self.last_remote_code = None
        while not self.abandon_event.is_set():
            try:
                return self.runner.client.post(
                    path, {"epoch": self.runner.epoch, "job_id": self.job_id, "fencing": self.fencing, **body}
                )
            except RemoteError as exc:
                self.last_remote_code = exc.code
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
            if self.last_remote_code in ABORT_CODES:
                # Broker faisant autorité (fencing périmé, job inconnu/terminal, conflit) :
                # le journal avec ce fencing est sans valeur, on le solde pour éviter
                # une reprise fantôme en boucle.
                log.warning("job_recovery_stale job_id=%s code=%s (journal soldé)", self.job_id, self.last_remote_code)
                self._clear_recovery()
            return False
        log.info("job_state job_id=%s %s->%s", self.job_id, self.state, dst)
        self.state = dst
        return True

    def _flush(self, final: bool = False) -> None:
        with self._out_lock:
            text = "".join(self._out)
            self._out.clear()
            activity, self._activity = self._activity, None
        # Journal : la session/runtime_session_id est la clé de reprise future.
        if self.session_id:
            self._save_recovery(self.state if self.state in (P.CLAIMED, P.STARTING, P.RUNNING) else P.RUNNING)
        pending_activity: str | None = activity
        while text or pending_activity or final:
            chunk, text = text[: P.MAX_CHUNK_CHARS], text[P.MAX_CHUNK_CHARS:]
            res = self._call(
                "event",
                {"event_id": uuid.uuid4().hex, "activity": pending_activity, "output": chunk or None, "runtime_session_id": self.session_id},
                deadline_s=60 if not final else 120,
            )
            pending_activity, final = None, False
            if res and res.get("cancel"):
                self.cancel_event.set()
            if res is None:
                # Réseau coupé : on ne perd rien, on refile le reste au prochain flush.
                # Borné : au-delà du plafond broker, on tronque comme à l'envoi normal.
                with self._out_lock:
                    rest = (chunk or "") + text
                    if rest:
                        room = P.MAX_OUTPUT_CHARS_PER_JOB - self._sent_chars
                        if room > 0:
                            self._out.insert(0, rest[:room][: P.MAX_CHUNK_CHARS * 4])
                    # NOTE : l'activité non envoyée est volontairement abandonnée
                    # (éphémère) ; la sortie, elle, est préservée.
                log.info("job_flush_deferred job_id=%s (réseau coupé, sortie bufferisée)", self.job_id)
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
                try:
                    self.proc.proc.wait(timeout=15)  # récolte le zombie (libère le PID)
                except Exception:  # noqa: BLE001, S110 - best-effort : close tue de toute façon les survivants
                    pass
                self.proc.close()
            if self.state in P.TERMINAL:
                self._clear_recovery()  # job soldé côté broker : le journal a rempli son rôle
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

        if self.state == P.CLAIMED:
            self._save_recovery(P.CLAIMED, force=True)  # persister AVANT tout point de non-retour
            if not self._transition(P.STARTING):
                return
        if self.state == P.STARTING:
            self._save_recovery(P.STARTING, force=True)
            tmpdir = cfg.home / "jobs" / self.job_id
            tmpdir.mkdir(parents=True, exist_ok=True)
            try:
                launch = self._resume_launch or adapter.build(prompt, job["mode"], cwd, tmpdir)
                self.proc = winproc.JobProcess(launch.argv, cwd, child_env(self.job_id, launch.env_extra))
                self.proc_started_at = time.time()
            except (winproc.LaunchError, ValueError, OSError) as exc:
                shutil.rmtree(tmpdir, ignore_errors=True)
                return self._transition(P.FAILED, error=f"launch_failed: {exc}") and None
            self._save_recovery(P.STARTING, force=True)
            log.info("job_spawned job_id=%s runtime=%s workspace=%s pid=%s%s", self.job_id, job["runtime"],
                     job["workspace_id"], self.proc.pid, " (reprise)" if self._resume_launch else "")
            if self._resumed:
                self._announce_resume()
            if self.cancel_event.is_set() or self.abandon_event.is_set():
                self.proc.kill_tree()  # jamais repris : le processus n'a pas exécuté une instruction
                self._transition(P.CANCELLED, error="annulé pendant le démarrage (processus jamais repris)")
                shutil.rmtree(tmpdir, ignore_errors=True)
                return
            if not self._transition(P.RUNNING):
                self.proc.kill_tree()
                return
            self._save_recovery(P.RUNNING, force=True)
            return self._supervise(adapter, launch, tmpdir)
        if self.state == P.RUNNING:
            # Reprise d'un job déjà running côté broker : pas de transition d'attache,
            # le nouveau processus continue la même exécution (même fencing, nouvel epoch).
            if self._resume_launch is None:
                log.error("job_resume_missing job_id=%s (aucun lancement de reprise)", self.job_id)
                return
            tmpdir = cfg.home / "jobs" / self.job_id
            tmpdir.mkdir(parents=True, exist_ok=True)
            try:
                launch = self._resume_launch
                self.proc = winproc.JobProcess(launch.argv, cwd, child_env(self.job_id, launch.env_extra))
                self.proc_started_at = time.time()
            except (winproc.LaunchError, ValueError, OSError) as exc:
                return self._transition(P.FAILED, error=f"resume_failed: {exc}") and None
            self._save_recovery(P.RUNNING, force=True)
            log.info("job_resumed job_id=%s runtime=%s pid=%s", self.job_id, job["runtime"], self.proc.pid)
            self._announce_resume()
            if self.cancel_event.is_set() or self.abandon_event.is_set():
                self.proc.kill_tree()
                self._transition(P.CANCELLED, error="annulé pendant la reprise (processus jamais repris)")
                shutil.rmtree(tmpdir, ignore_errors=True)
                return
            self.proc.resume()
            return self._supervise(adapter, launch, tmpdir, already_running=True)
        log.error("job_bad_state job_id=%s state=%s", self.job_id, self.state)

    def _announce_resume(self) -> None:
        """Borne de reprise explicite dans le journal broker (pas de duplication silencieuse)."""
        self.runner.bump_resume_count(self.job_id)
        self._call(
            "event",
            {"event_id": uuid.uuid4().hex,
             "activity": f"resume_attempt #{self.runner.resume_count(self.job_id)} (reprise après coupure, même fencing)",
             "runtime_session_id": self.session_id},
            deadline_s=120,
        )

    def _supervise(self, adapter: Adapter, launch: Any, tmpdir: Path, already_running: bool = False) -> None:
        job = self.job
        if not already_running:
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
        self.parked: dict[str, dict[str, Any]] = {}  # job_id -> record (suspended, sans processus, bail entretenu)
        self._resume_counts: dict[str, int] = {}
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self._resync = threading.Event()
        self.last_ok = time.monotonic()
        self.connected = False
        self._info_cache: dict[str, Any] | None = None

    def bump_resume_count(self, job_id: str) -> None:
        with self.lock:
            self._resume_counts[job_id] = self._resume_counts.get(job_id, 0) + 1

    def resume_count(self, job_id: str) -> int:
        with self.lock:
            return self._resume_counts.get(job_id, 0)

    # ---------------------------------------------------------------- session
    def info(self) -> dict[str, Any]:
        runtimes = []
        for rt_id, rt in self.config.runtimes.items():
            if rt.enabled and rt_id in ADAPTERS:
                runtimes.append(ADAPTERS[rt_id](rt.exe, rt.extra, self.config.permission_policy).probe())
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
        outil courant) + parkings de reprise (`suspended:true`, sans processus).
        Champs absents = non observés ; le broker les stocke NULL."""
        with self.lock:
            out = []
            for w in self.workers.values():
                if w.done.is_set():
                    continue
                entry: dict[str, Any] = {"job_id": w.job_id, "fencing": w.fencing}
                if w.proc is not None:
                    try:
                        alive = w.proc.proc.poll() is None
                    except Exception:  # noqa: BLE001 - télémétrie best-effort : inconnu plutôt qu'inventé
                        alive = None
                    try:
                        children = w.proc.active_processes()
                    except Exception:  # noqa: BLE001 - télémétrie best-effort : inconnu plutôt qu'inventé
                        children = None
                    entry.update(
                        {
                            "pid": w.proc.pid,
                            "proc_alive": alive,
                            "proc_started_at": w.proc_started_at,
                            "child_procs": children,
                            "tool": w._tool,
                        }
                    )
                out.append(entry)
            for job_id, rec in self.parked.items():
                if job_id in self.workers:
                    continue
                out.append(
                    {
                        "job_id": job_id,
                        "fencing": int(rec.get("fencing", -1)),
                        "suspended": True,
                        "recovering": True,
                        "recovery_detail": str(rec.get("recovery_detail") or "parqué : reprise explicite requise")[:500],
                    }
                )
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
            try:
                if self._resync.is_set():
                    self.hello()
                    if self.stop_event.is_set():
                        break
                    continue
                res = self.client.post("heartbeat", {"epoch": self.epoch, "held": self.held()})
                self.last_ok = time.monotonic()
                if not self.connected:
                    log.info("runner_reconnected epoch=%s", self.epoch)
                self.connected = True
                with self.lock:
                    for job_id in res.get("cancel", []):
                        if job_id in self.workers:
                            log.info("job_cancel_received job_id=%s", job_id)
                            self.workers[job_id].cancel_event.set()
                    for job_id in res.get("abandon", []):
                        if job_id in self.workers:
                            self.workers[job_id].abandon_event.set()
                            recovery.clear(self.config.home, job_id)  # broker faisant autorité
                        if job_id in self.parked:
                            # Terminal côté broker (cancelled/lost/failed...) : le parking est soldé.
                            log.info("job_parked_abandoned job_id=%s (terminal côté broker)", job_id)
                            self.parked.pop(job_id, None)
                            recovery.clear(self.config.home, job_id)
            except RemoteError as exc:
                if exc.code in RESYNC_CODES:
                    self._resync.set()
                else:
                    log.error("heartbeat_error %s", exc)
            except NetworkError as exc:
                if self.connected:
                    log.warning("runner_disconnected err=%s (processus conservés, sortie bufferisée)", exc)
                self.connected = False
                self._resync.set()
            offline = time.monotonic() - self.last_ok
            if offline > self.config.offline_kill_s:
                # Changement assumé (reprise PC) : PLUS de mise à mort sur perte réseau.
                # Les processus continuent localement ; la sortie est bufferisée et flushée
                # au retour. `offline_kill_s` devient un seuil d'alerte, pas d'abandon.
                log.warning("runner_offline_s=%.0f (jobs conservés, aucune mise à mort)", offline)
            self._write_status()

    def claim_loop(self) -> None:
        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                if self.epoch < 0 or self._resync.is_set():
                    self.hello()
                    if self.stop_event.is_set():
                        break  # arrêté pendant le hello : ne plus toucher au broker (epoch)
                with self.lock:
                    free = self.config.max_parallel - len(self.workers) - len(self.parked)
                if free <= 0:
                    self.stop_event.wait(1.0)
                    continue
                res = self.client.post("claim", {"epoch": self.epoch, "free_slots": free, "wait_s": P.CLAIM_POLL_S})
                if self.stop_event.is_set():
                    break  # long-poll revenu après stop : ne pas spawner, ne pas bump l'epoch
                self.last_ok = time.monotonic()
                backoff = 1.0
                for job in res.get("jobs", []):
                    worker = JobWorker(self, job)
                    worker._save_recovery(P.CLAIMED, force=True)  # persister avant le premier thread
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
        held = self.held()
        with self.lock:
            parked_ids = sorted(self.parked)
        status = {
            "runner_id": self.config.runner_id,
            "connected": self.connected,
            "epoch": self.epoch,
            "seconds_since_broker_ok": round(time.monotonic() - self.last_ok, 1),
            "active_jobs": [h["job_id"] for h in held],
            "parked_jobs": parked_ids,
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
        # Les dossiers temporaires restants viennent d'un runner tué (les processus sont
        # morts avec leur Job Object). Le journal `recovery/` est PRÉSERVÉ : c'est lui
        # qui permet la reprise au (re)hello.
        jobs_dir = Path(self.config.home) / "jobs"
        if jobs_dir.is_dir():
            for leftover in jobs_dir.iterdir():
                shutil.rmtree(leftover, ignore_errors=True)
        pending = recovery.load_all(self.config.home)
        if pending:
            log.info("recovery_pending count=%s", len(pending))
        self._info_cache = self.info()
        # Premier hello AVEC le journal : empêche le `lost` immédiat, rattache au nouvel epoch.
        if pending:
            with self.lock:
                for rec in pending:
                    self.parked[str(rec["job_id"])] = rec
            try:
                self.hello()
            except (NetworkError, RemoteError, OSError) as exc:
                log.warning("recovery_hello_deferred err=%s", exc)
        hb = threading.Thread(target=self.heartbeat_loop, name="heartbeat", daemon=True)
        hb.start()
        if pending:
            threading.Thread(target=self._reconcile_all, args=(pending,), name="recovery", daemon=True).start()
        try:
            self.claim_loop()
        finally:
            self.stop_event.set()
            with self.lock:
                for w in self.workers.values():
                    w.abandon_event.set()

    # ---------------------------------------------------------- reprise locale
    def _reconcile_all(self, records: list[dict[str, Any]]) -> None:
        # Laisse au hello/heartbeat le temps de rattacher (nouvel epoch) avant de spawner.
        for _ in range(60):
            if self.stop_event.is_set():
                return
            if self.epoch >= 0 and self.connected:
                break
            time.sleep(1.0)
        for rec in records:
            if self.stop_event.is_set():
                return
            try:
                self._recover_one(rec)
            except Exception:  # une reprise ne doit jamais tuer les autres
                log.exception("recovery_failed job_id=%s", rec.get("job_id"))

    def _recover_one(self, rec: dict[str, Any]) -> None:
        job_id = str(rec.get("job_id"))
        with self.lock:
            if job_id in self.workers:
                return  # déjà repris (double hello)
        phase = str(rec.get("phase") or "running")
        runtime = str(rec.get("runtime") or "")
        mode = str(rec.get("mode") or "")
        ws_id = str(rec.get("workspace_id") or "")
        prompt = rec.get("prompt") if isinstance(rec.get("prompt"), str) else ""
        session_id = rec.get("runtime_session_id") or rec.get("session_id")
        cfg = self.config
        rt = cfg.runtimes.get(runtime)
        ws = cfg.workspaces.get(ws_id)
        if rt is None or not rt.enabled or runtime not in ADAPTERS or ws is None:
            return self._park_one(rec, "runtime ou workspace plus autorisé localement : parking, décision humaine requise")
        adapter = ADAPTERS[runtime](rt.exe, rt.extra, cfg.permission_policy)
        if mode not in ws.modes or mode not in adapter.modes:
            return self._park_one(rec, f"mode {mode} plus autorisé : parking, décision humaine requise")
        try:
            cwd = resolve_workspace(ws)
        except PolicyError as exc:
            return self._park_one(rec, f"workspace invalide ({exc.code}) : parking, décision humaine requise")
        # Ancien processus survivant ? Après un reboot : jamais (KILL_ON_JOB_CLOSE).
        # Vérification anti-recyclage de PID : purement diagnostique. On ne bloque la
        # reprise que si elle serait dangereuse (workspace_write sans session : parking
        # de toute façon). Les chemins sûrs (session resume, fake, read_only) peuvent
        # reprendre même si un orphelin agonise encore : l'ancien processus est sourd
        # au nouveau fencing/epoch et ne recevra plus rien du broker.
        pid = rec.get("pid")
        orphan = isinstance(pid, int) and winproc.is_same_process(pid, rec.get("proc_started_at"))
        if orphan:
            log.warning("recovery_pid_maybe_alive job_id=%s pid=%s (reprise quand même si chemin sûr)", job_id, pid)
        tmpdir = cfg.home / "jobs" / job_id
        # Politique de reprise : session du runtime d'abord, ré-exécution fraîche
        # seulement si sûre (fake ou read_only), sinon parking explicite.
        resume_launch = None
        fresh_ok = (runtime == "fake") or (mode == "read_only")
        if session_id and isinstance(session_id, str):
            try:
                resume_launch = adapter.resume(session_id, mode, cwd, tmpdir)
            except Exception:  # noqa: BLE001 - un adapter ne doit jamais tuer la reprise
                resume_launch = None
        if resume_launch is not None:
            log.info("job_resume_session job_id=%s runtime=%s", job_id, runtime)
        elif phase == "claimed" and prompt and fresh_ok:
            resume_launch = None  # chemin frais normal ci-dessous
        elif phase in ("starting", "running", "suspended") and prompt and fresh_ok and runtime == "fake":
            try:
                resume_launch = adapter.build(prompt, mode, cwd, tmpdir)
                log.info("job_resume_rebuild job_id=%s runtime=fake (fixture idempotente)", job_id)
            except (ValueError, OSError) as exc:
                return self._park_one(rec, f"reconstruction impossible ({exc}) : parking")
        else:
            detail = (
                f"runtime {runtime} sans session resumable (session {'absente' if not session_id else 'non supportee'}) "
                f"en mode {mode} : re-execution aveugle refusee, parking en attente de decision humaine"
            )
            return self._park_one(rec, detail)
        job = {
            "job_id": job_id,
            "fencing": int(rec.get("fencing", -1)),
            "runtime": runtime,
            "workspace_id": ws_id,
            "mode": mode,
            "prompt": prompt,
            "timeout_s": rec.get("timeout_s") or P.DEFAULT_TIMEOUT_S,
            "runtime_session_id": session_id,
        }
        if phase == "claimed":
            worker = JobWorker(self, job, initial_state=P.CLAIMED)
        elif phase == "starting":
            worker = JobWorker(self, job, initial_state=P.STARTING, resume_launch=resume_launch)
        else:
            worker = JobWorker(self, job, initial_state=P.RUNNING, resume_launch=resume_launch, resumed=True)
            worker.session_id = session_id if isinstance(session_id, str) else None
        with self.lock:
            if job_id in self.workers:
                return
            self.workers[job_id] = worker
            self.parked.pop(job_id, None)
        log.info("job_recovery_start job_id=%s phase=%s", job_id, phase)
        worker.start()

    def _park_one(self, rec: dict[str, Any], detail: str) -> None:
        """Parking explicite : état récupérable, bail entretenu, jamais `lost` immédiat."""
        job_id = str(rec.get("job_id"))
        rec = {**rec, "phase": "suspended", "recovery_state": P.SUSPENDED, "recovery_detail": detail[:500]}
        try:
            recovery.save(self.config.home, rec)
        except Exception:  # noqa: BLE001, S110 - parking mémoire quand même (bail entretenu)
            pass
        with self.lock:
            self.parked[job_id] = rec
        log.warning("job_suspended job_id=%s %s", job_id, detail)

    def stop(self) -> None:
        self.stop_event.set()
