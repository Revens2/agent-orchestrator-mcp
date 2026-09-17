"""Notifications de fin (Telegram) : outbox SQLite + dispatcher reaper.

Enqueue : atomique dans `Store.transition()` sur `running -> completed`
acceptée pour les runners allowlistés (défaut `main-windows-pc,pc-fixe`,
`ORCH_NOTIFY_RUNNERS`) — `INSERT OR IGNORE` sur (job_id, kind), aucun appel
externe dans la transaction/route.

Dispatch : `dispatch_due()` lit les dues (`pending`, `next_attempt_at <= now`),
construit un message STRICTEMENT whitelisté (runner/runtime/workspace/titre/
durée/job8/état mission + rappel `completed ≠ mission validée` si mission),
puis appelle un sender configurable (défaut `/usr/local/bin/send_telegram.sh`,
`ORCH_NOTIFY_SENDER`) avec timeout borné (`ORCH_NOTIFY_TIMEOUT_S`, défaut 15 s).
Succès => `sent` ; échec => retry exponentiel borné (60s, 120s, … plafonné 1h,
`ORCH_NOTIFY_MAX_ATTEMPTS`, défaut 8) puis `failed`. L'outbox persiste au
restart (reprise automatique au prochain reaper).

Ne jamais envoyer : prompt/output/result_summary/error/secrets. Le titre vient
de `display_title` (fallback si secret apparent) ; aucun champ libre.
"""

from __future__ import annotations

import logging
import os
import subprocess
from collections.abc import Callable
from typing import Any

import orch_protocol as P

log = logging.getLogger("orch.notifications")

SenderFn = Callable[[str], tuple[bool, str]]


def parse_runner_filter(raw: str | None) -> frozenset[str]:
    """`ORCH_NOTIFY_RUNNERS` (csv) -> set. Vide => défaut protocolaire."""
    if raw is None:
        return frozenset(P.NOTIFY_RUNNERS_DEFAULT)
    parts = frozenset(s.strip() for s in raw.split(",") if s.strip())
    return parts or frozenset(P.NOTIFY_RUNNERS_DEFAULT)


def runner_filter_from_env() -> frozenset[str]:
    return parse_runner_filter(os.environ.get("ORCH_NOTIFY_RUNNERS"))


def sender_from_env() -> str:
    return os.environ.get("ORCH_NOTIFY_SENDER", P.NOTIFY_SENDER_DEFAULT).strip() or P.NOTIFY_SENDER_DEFAULT


def timeout_from_env() -> float:
    try:
        return max(1.0, min(120.0, float(os.environ.get("ORCH_NOTIFY_TIMEOUT_S", P.NOTIFY_TIMEOUT_S))))
    except (TypeError, ValueError):
        return float(P.NOTIFY_TIMEOUT_S)


def max_attempts_from_env() -> int:
    try:
        return max(1, min(32, int(os.environ.get("ORCH_NOTIFY_MAX_ATTEMPTS", P.NOTIFY_MAX_ATTEMPTS))))
    except (TypeError, ValueError):
        return int(P.NOTIFY_MAX_ATTEMPTS)


def build_completion_message(job: dict[str, Any], mission_state: str | None = None) -> str:
    """Message Telegram strictement whitelisté : runner/runtime/workspace/titre/
    durée/job8/état mission. Aucun prompt/output/result_summary/error/secret.

    `job` = vue `get_job` (ou équivalent) ; seuls les champs allowlistés sont
    lus. `mission_state` = état mission concernée ou None (pas de ligne mission).
    """
    runner = str(job.get("runner_id") or "?")[:64]
    runtime = str(job.get("runtime") or "?")[:32]
    workspace = str(job.get("workspace_id") or "?")[:64]
    title = P.display_title(job.get("display_title") or job.get("prompt") or "", fallback="job")
    title = P.clip(title, P.MAX_TITLE_CHARS) or "job"
    duration = job.get("duration_s")
    dur_txt = f"{float(duration):.1f}s" if isinstance(duration, (int, float)) else "?"
    job8 = str(job.get("job_id") or "?")[:8]
    lines = [
        "✅ job terminé (completed)",
        f"runner: {runner}",
        f"runtime: {runtime}",
        f"workspace: {workspace}",
        f"titre: {title}",
        f"durée: {dur_txt}",
        f"job: {job8}",
    ]
    if mission_state:
        lines.append(f"mission: {mission_state} (completed ≠ mission validée)")
    return "\n".join(lines)


def run_sender_process(sender: str, message: str, timeout_s: float) -> tuple[bool, str]:
    """Appelle le sender externe : `[sender, message]`, timeout borné.

    Retourne (ok, error). Toute exception/timeout/retour != 0 => retryable.
    Le message ne contient que des champs whitelistés (jamais de secret)."""
    try:
        proc = subprocess.run(
            [sender, message],
            capture_output=True,
            text=True,
            timeout=max(1.0, float(timeout_s)),
        )
    except FileNotFoundError:
        return False, f"sender introuvable: {sender}"
    except subprocess.TimeoutExpired:
        return False, f"sender timeout après {timeout_s}s"
    except OSError as exc:
        return False, f"sender OSError: {exc}"
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()[:300]
        return False, f"sender exit {proc.returncode}: {err}"
    return True, ""


def dispatch_due(
    store,
    sender: str | SenderFn | None = None,
    timeout_s: float | None = None,
    now: float | None = None,
    limit: int = 20,
    max_attempts: int | None = None,
) -> dict[str, int]:
    """Envoie les notifications dues (reaper orch-mcp). Reprise après restart
    incluse (l'outbox est en SQLite). Retourne des stats bornées."""
    from orch_mcp.store import Store  # import local : évite un cycle

    assert isinstance(store, Store)
    now_v = store.clock() if now is None else float(now)
    sender_v: str | SenderFn = sender if sender is not None else sender_from_env()
    timeout_v = timeout_from_env() if timeout_s is None else max(1.0, min(120.0, float(timeout_s)))
    max_att = max_attempts_from_env() if max_attempts is None else max(1, int(max_attempts))
    stats = {"due": 0, "sent": 0, "retried": 0, "failed": 0, "skipped": 0}
    dues = store.due_completion_notifications(now_v, limit)
    stats["due"] = len(dues)
    for due in dues:
        job_id = due["job_id"]
        # Filtre runner défensif (l'enqueue filtre déjà ; le dispatcher ne fait
        # jamais confiance à une ligne orpheline).
        if due.get("runner_id") not in store.notify_runners:
            stats["skipped"] += 1
            continue
        job = store.get_job(job_id, tail_chars=0)
        if job is None or job.get("state") != P.COMPLETED:
            # Job purgé ou plus completed (ne devrait pas arriver : completed
            # immuable) => échec définitif honnête, pas de retry infini.
            store.mark_completion_error(job_id, "job introuvable ou non completed", max_attempts=max_att)
            stats["failed"] += 1
            continue
        mission = store.mission_for_job(job_id)
        msg = build_completion_message(job, mission["state"] if mission else None)
        try:
            if callable(sender_v):
                ok, err = sender_v(msg)
            else:
                ok, err = run_sender_process(str(sender_v), msg, timeout_v)
        except Exception as exc:  # noqa: BLE001 - un sender ne doit jamais tuer le reaper
            ok, err = False, f"sender exception: {exc!r}"[:300]
        if ok:
            store.mark_completion_sent(job_id)
            stats["sent"] += 1
        else:
            after = store.mark_completion_error(job_id, err or "sender_failed", max_attempts=max_att)
            if after is not None and after.get("status") == P.NOTIFY_STATUS_FAILED:
                stats["failed"] += 1
            else:
                stats["retried"] += 1
    if stats["due"]:
        log.info("completion_dispatch %s", stats)
    return stats
