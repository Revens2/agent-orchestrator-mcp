"""Contrat partagé broker ↔ runner (version 1).

Tout ce qui traverse la frontière VPS ↔ PC est défini ici : noms d'états,
transitions autorisées, bornes et validation des identifiants. Le broker et le
runner importent ce module ; aucune des deux parties n'invente sa propre règle.
"""

from __future__ import annotations

import re

PROTOCOL_VERSION = 1

# --- états ---------------------------------------------------------------
QUEUED = "queued"          # accepté, en attente d'un runner
CLAIMED = "claimed"        # un runner détient le bail, rien n'est lancé
STARTING = "starting"      # validations locales + création du processus
RUNNING = "running"        # processus agent actif
COMPLETED = "completed"    # exit 0, sortie reçue
FAILED = "failed"          # exit != 0 ou refus local (workspace, runtime...)
TIMEOUT = "timeout"        # durée maximale dépassée, arbre tué
CANCELLED = "cancelled"    # annulation confirmée
LOST = "lost"              # bail expiré après le point de non-retour : issue inconnue

TERMINAL = frozenset({COMPLETED, FAILED, TIMEOUT, CANCELLED, LOST})
ACTIVE = frozenset({CLAIMED, STARTING, RUNNING})
ALL_STATES = frozenset({QUEUED}) | ACTIVE | TERMINAL

# Transitions valides. Tout le reste est refusé (409 côté broker).
# claimed -> queued : uniquement par le reaper (bail expiré avant tout lancement).
TRANSITIONS: dict[str, frozenset[str]] = {
    QUEUED: frozenset({CLAIMED, CANCELLED}),
    CLAIMED: frozenset({STARTING, CANCELLED, FAILED, QUEUED}),
    STARTING: frozenset({RUNNING, FAILED, CANCELLED, LOST}),
    RUNNING: frozenset({COMPLETED, FAILED, TIMEOUT, CANCELLED, LOST}),
}
for _t in TERMINAL:
    TRANSITIONS[_t] = frozenset()

# Transitions qu'un runner a le droit de demander (le reste appartient au broker).
RUNNER_TRANSITIONS: frozenset[tuple[str, str]] = frozenset(
    {
        (CLAIMED, STARTING),
        (CLAIMED, FAILED),
        (CLAIMED, CANCELLED),
        (STARTING, RUNNING),
        (STARTING, FAILED),
        (STARTING, CANCELLED),
        (RUNNING, COMPLETED),
        (RUNNING, FAILED),
        (RUNNING, TIMEOUT),
        (RUNNING, CANCELLED),
    }
)


def transition_allowed(src: str, dst: str) -> bool:
    return dst in TRANSITIONS.get(src, frozenset())


# --- runtimes / modes ----------------------------------------------------
RUNTIMES = ("claude-code", "codex", "agy", "opencode", "fake")
MODES = ("read_only", "workspace_write")
# politique locale du runner (jamais fournie par le broker/MCP)
UNATTENDED, GUARDED = "unattended", "guarded"
PERMISSION_POLICIES = (UNATTENDED, GUARDED)

# --- bornes --------------------------------------------------------------
MAX_PROMPT_CHARS = 100_000
MAX_SUMMARY_CHARS = 8_000
MAX_ERROR_CHARS = 2_000
MAX_ACTIVITY_CHARS = 300
MAX_CHUNK_CHARS = 16_000
MAX_OUTPUT_CHARS_PER_JOB = 2_000_000
MAX_OUTPUT_PAGE = 20_000
DEFAULT_TIMEOUT_S = 3_600
MAX_TIMEOUT_S = 4 * 3_600
LEASE_S = 60
HEARTBEAT_S = 5
ONLINE_WINDOW_S = 30
CLAIM_POLL_S = 25

# --- reprise sur panne PC (v1, compatible : métadonnées + événements, pas de nouvel état)
# Un job starting/running dont le bail expire n'est plus marqué `lost` aussitôt :
# le broker le parque (`recovery_state='suspended'`, bail prolongé de RECOVERY_GRACE_S)
# puis ne le déclare `lost` qu'à la seconde expiration. Le runner déclare au (re)hello
# les jobs de son journal local (`recovering:true`) pour empêcher le `lost` immédiat
# et les rattacher au nouvel epoch (même fencing). Aucune ré-exécution aveugle :
# reprise de session du runtime si possible, sinon parking explicite `suspended`
# (bail entretenu par heartbeat, décision humaine via cancel/retry).
RECOVERY_GRACE_S = 4 * 3_600
RECOVERING = "recovering"
SUSPENDED = "suspended"
RECOVERY_STATES = frozenset({RECOVERING, SUSPENDED})

# --- supervision riche (v1, champs optionnels : protocole inchangé) ------------
# Santé d'exécution calculée par le broker (job_get.execution_health).
HEALTHY = "healthy"                        # activité/output récents
IDLE = "idle"                              # en attente (queued/claimed ou démarrage récent)
SUSPECTED_STALL = "suspected_stall"        # processus vivant mais sans activité/output depuis un seuil
STALLED = "stalled"                        # silence prolongé, processus vivant : intervention humaine requise
RUNNER_DISCONNECTED = "runner_disconnected"  # runner hors ligne (heartbeat trop vieux)
PROCESS_DEAD = "process_dead"              # processus non vivant alors que le job est actif
EXECUTION_HEALTH = frozenset({HEALTHY, IDLE, SUSPECTED_STALL, STALLED, RUNNER_DISCONNECTED, PROCESS_DEAD})

# Seuils de détection de stall (processus vivant + silence d'activité/output).
# La détection EMET un événement (notify) ; elle ne relance ni n'annule jamais seule.
STALL_SUSPECT_S = 600
STALL_S = 1_800

# Journal d'événements structuré borné (job_events, pas de transcript).
EV_JOB_CLAIMED = "job_claimed"
EV_RUNTIME_SPAWNED = "runtime_spawned"
EV_PROCESS_RUNNING = "process_running"
EV_OUTPUT_PROGRESS = "output_progress"
EV_ACTIVITY = "activity"
EV_PROCESS_EXIT = "process_exit"
EV_RUNNER_DISCONNECT = "runner_disconnect"
EV_LEASE_EXPIRED = "lease_expired"
EV_REQUEUED = "requeued"
EV_CANCEL_REQUESTED = "cancel_requested"
EV_TIMEOUT_MARKED = "timeout_marked"
EV_SUSPECTED_STALL = "suspected_stall"
EV_STALLED = "stalled"
EV_RUNNER_RECOVERING = "runner_recovering"
EV_RESUME_ATTEMPT = "resume_attempt"
EV_RESUME_FAILED = "resume_failed"
EV_JOB_SUSPENDED = "job_suspended"
EVENT_KINDS = frozenset({
    EV_JOB_CLAIMED, EV_RUNTIME_SPAWNED, EV_PROCESS_RUNNING, EV_OUTPUT_PROGRESS,
    EV_ACTIVITY, EV_PROCESS_EXIT, EV_RUNNER_DISCONNECT, EV_LEASE_EXPIRED,
    EV_REQUEUED, EV_CANCEL_REQUESTED, EV_TIMEOUT_MARKED, EV_SUSPECTED_STALL, EV_STALLED,
    EV_RUNNER_RECOVERING, EV_RESUME_ATTEMPT, EV_RESUME_FAILED, EV_JOB_SUSPENDED,
})
MAX_EVENTS_PER_JOB = 500
OUTPUT_PROGRESS_STEP_CHARS = 65_536

# Missions au-dessus des jobs : objectif + critères d'acceptation + tentatives.
MISSION_EXECUTING = "executing"
MISSION_NEEDS_VALIDATION = "needs_validation"
MISSION_VALIDATED = "validated"
MISSION_INCOMPLETE = "incomplete"
MISSION_BLOCKED = "blocked"
MISSION_FAILED = "failed"
MISSION_STATES = frozenset({
    MISSION_EXECUTING, MISSION_NEEDS_VALIDATION, MISSION_VALIDATED,
    MISSION_INCOMPLETE, MISSION_BLOCKED, MISSION_FAILED,
})
MAX_MISSION_ATTEMPTS = 5
MAX_OBJECTIVE_CHARS = 4_000
MAX_CRITERIA = 20
MAX_CRITERION_CHARS = 500

# Attente long-poll côté MCP (agent_job_wait) : bornes sûres.
WAIT_DEFAULT_S = 25
WAIT_MAX_S = 60

_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


def valid_id(value: object) -> bool:
    """Identifiant opaque sûr : minuscules, chiffres, `-`, `_`, 1..64 car."""
    return isinstance(value, str) and bool(_ID_RE.fullmatch(value))


def clip(text: str | None, limit: int) -> str | None:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[: limit - 15] + "…[tronqué]"
