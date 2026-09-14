"""Journal local durable du runner : survit au reboot / à la perte réseau.

Un fichier par job non terminal : `<home>/recovery/<job_id>.json`, écriture atomique
(tmp + os.replace). Contenu minimal avant spawn : identité job, fencing, runtime,
mode, workspace, prompt + hash, timeout, phase, session/runtime_session_id quand
disponible, PID + create time, statut de recovery, curseurs/output nécessaires.
Lecture tolérante : fichier partiellement écrit ou corrompu => ignoré et renommé
`.corrupt-<ts>` (le boot ne doit jamais échouer à cause du journal).
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any


def recovery_dir(home: Path | str) -> Path:
    return Path(home) / "recovery"


def record_path(home: Path | str, job_id: str) -> Path:
    safe = "".join(c for c in job_id if c.isalnum() or c in ("-", "_"))[:64] or "job"
    return recovery_dir(home) / f"{safe}.json"


def prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.encode("utf-8", errors="replace")).hexdigest()


def build_record(
    *,
    job: dict[str, Any],
    phase: str,
    prompt: str = "",
    session_id: str | None = None,
    pid: int | None = None,
    proc_started_at: float | None = None,
    sent_chars: int = 0,
    recovery_state: str | None = None,
    recovery_detail: str | None = None,
) -> dict[str, Any]:
    return {
        "v": 1,
        "job_id": job.get("job_id"),
        "fencing": int(job.get("fencing", -1)),
        "runtime": job.get("runtime"),
        "workspace_id": job.get("workspace_id"),
        "mode": job.get("mode"),
        "timeout_s": job.get("timeout_s"),
        "prompt": prompt,
        "prompt_sha256": prompt_hash(prompt) if prompt else None,
        "phase": phase,  # claimed | starting | running | suspended
        "runtime_session_id": session_id,
        "session_id": session_id,  # alias lisible
        "pid": pid,
        "proc_started_at": proc_started_at,
        "sent_chars": int(sent_chars),
        "recovery_state": recovery_state,
        "recovery_detail": recovery_detail[:500] if recovery_detail else None,
        "updated_at": time.time(),
    }


def save(home: Path | str, record: dict[str, Any]) -> None:
    """Écriture atomique : tmp + replace. Ne lève jamais (log appelant)."""
    d = recovery_dir(home)
    d.mkdir(parents=True, exist_ok=True)
    job_id = str(record.get("job_id") or "job")
    path = record_path(home, job_id)
    tmp = path.with_suffix(".tmp")
    record = {**record, "updated_at": time.time()}
    tmp.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def load_all(home: Path | str) -> list[dict[str, Any]]:
    """Charge les records non terminaux. Corrompus => renommés, jamais d'exception."""
    d = recovery_dir(home)
    if not d.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(d.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001 - journal partiellement écrit : quarantaine, pas de crash
            try:
                path.rename(path.with_suffix(f".corrupt-{int(time.time())}"))
            except OSError:
                pass
            continue
        if not isinstance(data, dict) or not data.get("job_id"):
            try:
                path.rename(path.with_suffix(f".corrupt-{int(time.time())}"))
            except OSError:
                pass
            continue
        try:
            data["fencing"] = int(data.get("fencing", -1))
        except (TypeError, ValueError):
            continue
        out.append(data)
    return out


def clear(home: Path | str, job_id: str) -> None:
    try:
        record_path(home, job_id).unlink(missing_ok=True)
    except OSError:
        pass
