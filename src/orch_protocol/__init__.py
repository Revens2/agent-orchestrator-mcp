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
# Reprise sur session corrompue : session abandonnée (jamais réutilisée),
# nouvelle session propre via mission_retry, handoff minimal (pas de transcript).
EV_SESSION_CORRUPTED = "session_corrupted"
EV_SESSION_RECREATED = "session_recreated"
EVENT_KINDS = frozenset({
    EV_JOB_CLAIMED, EV_RUNTIME_SPAWNED, EV_PROCESS_RUNNING, EV_OUTPUT_PROGRESS,
    EV_ACTIVITY, EV_PROCESS_EXIT, EV_RUNNER_DISCONNECT, EV_LEASE_EXPIRED,
    EV_REQUEUED, EV_CANCEL_REQUESTED, EV_TIMEOUT_MARKED, EV_SUSPECTED_STALL, EV_STALLED,
    EV_SESSION_CORRUPTED, EV_SESSION_RECREATED,
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

# --- session corrompue (reprise OpenCode : nouvelle session + handoff) -------
# Signatures EXACTES (sous-chaînes littérales, jamais un simple mot "error").
# `failed to load plugin` est observé en production (opencode.log) ; les autres
# sont les formes exactes connues des corruptions session/config/DB. Tout texte
# ne contenant aucune de ces formes n'est PAS une corruption.
SESSION_CORRUPTION_SIGNS = (
    "failed to load plugin",
    "failed to load session",
    "database disk image is malformed",
    "SQLITE_CORRUPT",
    "Unexpected token",
    "Unexpected end of JSON input",
    "bad decrypt",
    "wrong final block length",
    "Failed to decrypt",
)
SESSION_CORRUPTED_PREFIX = "session_corrupted:"
# Anti-boucle : au-delà de N corruptions consécutives sur la même mission,
# mission_retry refuse (intervention humaine requise).
MAX_CONSECUTIVE_CORRUPTIONS = 2


def match_corruption(text: object) -> str | None:
    """Première signature exacte trouvée, sinon None (jamais de match naïf)."""
    if not isinstance(text, str):
        return None
    for sign in SESSION_CORRUPTION_SIGNS:
        if sign in text:
            return sign
    return None
# display_title dérive un titre court et stable du premier objectif réel.
# Stable par construction (fonction pure du texte source : pas de renommage
# en boucle), sans migration (calculé à la lecture, identités/source_hash
# ConvIA intacts). Runtimes sans mécanisme natif (claude/codex/agy en
# headless) : seul ce display_title broker existe (fallback documenté).
# --- questions en attente (Photon/iMessage : débat explicite, pas de `?`) --
# waiting_for_user est un ÉTAT (table pending_questions), jamais un regex sur
# `?`. Notification après QUESTION_NOTIFY_AFTER_S sans réponse (défaut 300 s,
# configurable pour E2E accélérée) ; UN message corrélé et dédupliqué ;
# réponse single-use routée à la session émettrice via correlation_id court.
QUESTION_NOTIFY_AFTER_S = 300
QUESTION_EXPIRY_S = 3_600
MAX_QUESTION_CHARS = 2_000
MAX_QUESTION_OPTIONS = 6
MAX_QUESTION_OPTION_CHARS = 200
MAX_ANSWER_CHARS = 500
Q_OPEN, Q_ANSWERED, Q_EXPIRED = "open", "answered", "expired"
QUESTION_STATES = (Q_OPEN, Q_ANSWERED, Q_EXPIRED)
Q_NOTIFY_PENDING, Q_NOTIFY_SENT, Q_NOTIFY_DEFERRED, Q_NOTIFY_FAILED = (
    "pending", "sent", "deferred", "failed",
)
QUESTION_NOTIFY_STATES = (Q_NOTIFY_PENDING, Q_NOTIFY_SENT, Q_NOTIFY_DEFERRED, Q_NOTIFY_FAILED)
# Convention explicite émise par l'agent dans sa sortie (parsée par le runner) :
# [[QUESTION]]texte[[/QUESTION]] [[OPTIONS]]choix1|choix2[[/OPTIONS]]
QUESTION_OPEN_TAG = "[[QUESTION]]"
QUESTION_CLOSE_TAG = "[[/QUESTION]]"
QUESTION_OPTIONS_TAG = "[[OPTIONS]]"
QUESTION_OPTIONS_CLOSE = "[[/OPTIONS]]"


def parse_question_block(text: object) -> tuple[str, list[str]] | None:
    """Extrait le premier bloc explicite [[QUESTION]]...[[/QUESTION]] (+
    [[OPTIONS]]a|b[[/OPTIONS]] optionnel). Retourne (question, options) ou
    None. Borné (4000 car. max scannés) ; aucun regex fragile, aucun match sur
    un simple `?`."""
    if not isinstance(text, str):
        return None
    buf = text[-4000:]
    start = buf.find(QUESTION_OPEN_TAG)
    if start < 0:
        return None
    end = buf.find(QUESTION_CLOSE_TAG, start)
    if end < 0:
        return None
    question = buf[start + len(QUESTION_OPEN_TAG):end].strip()
    if not question:
        return None
    options: list[str] = []
    rest = buf[end + len(QUESTION_CLOSE_TAG):end + len(QUESTION_CLOSE_TAG) + 600]
    ostart = rest.find(QUESTION_OPTIONS_TAG)
    if ostart >= 0:
        oend = rest.find(QUESTION_OPTIONS_CLOSE, ostart)
        if oend >= 0:
            for part in rest[ostart + len(QUESTION_OPTIONS_TAG):oend].split("|"):
                part = part.strip()
                if part:
                    options.append(part[:MAX_QUESTION_OPTION_CHARS])
                if len(options) >= MAX_QUESTION_OPTIONS:
                    break
    return question[:MAX_QUESTION_CHARS], options

# --- titres d'affichage (conversations lisibles, jamais de renommage) --------
# display_title dérive un titre court et stable du premier objectif réel.
# Stable par construction (fonction pure du texte source : pas de renommage
# en boucle), sans migration (calculé à la lecture, identités/source_hash
# ConvIA intacts). Runtimes sans mécanisme natif (claude/codex/agy en
# headless) : seul ce display_title broker existe (fallback documenté).
MAX_TITLE_WORDS = 10
MAX_TITLE_CHARS = 90

def display_title(text: object, fallback: str = "session sans titre") -> str:
    """Titre court (5-10 mots, ≤90 car.) depuis la première ligne utile.

    - première ligne non vide (marqueurs `>`, `-`, `*`, `#` initiaux retirés) ;
    - mots coupés à MAX_TITLE_WORDS, fin bornée à MAX_TITLE_CHARS ;
    - si le texte porte un secret apparent (redact le modifierait) ou est
      vide/absent : fallback (jamais de secret ni d'ID bruyant dans un titre).
    """
    if not isinstance(text, str):
        return fallback
    first = ""
    for line in text.splitlines():
        line = " ".join(line.split())
        line = line.lstrip(">#-*• \t")
        if line:
            first = line
            break
    if not first:
        return fallback
    from orch_protocol.redact import redact  # import local : évite un cycle

    if redact(first) != first:
        return fallback
    words = first.split()
    short = " ".join(words[:MAX_TITLE_WORDS])
    if len(short) > MAX_TITLE_CHARS:
        short = short[: MAX_TITLE_CHARS - 1].rstrip() + "…"
    return short or fallback

# --- alertes infra (observabilité Telegram unifiée : [ETUDE]/[NEXUS]) --------
# Persistance normalisée des alertes sortantes (jamais l'historique Telegram
# comme source de vérité). Écriture réservée aux ingesteurs locaux du VPS
# (CLI loopback) ; le MCP n'expose que la lecture (infra_alert_list/get).
ALERT_ETUDE = "etude"
ALERT_NEXUS = "nexus"
ALERT_SOURCES = (ALERT_ETUDE, ALERT_NEXUS)
ALERT_INFO, ALERT_WARNING, ALERT_CRITICAL = "info", "warning", "critical"
ALERT_SEVERITIES = (ALERT_INFO, ALERT_WARNING, ALERT_CRITICAL)
ALERT_ACTIVE, ALERT_ACKED, ALERT_RESOLVED = "active", "acked", "resolved"
ALERT_STATES = (ALERT_ACTIVE, ALERT_ACKED, ALERT_RESOLVED)
MAX_ALERT_TITLE_CHARS = 500
MAX_ALERT_DETAIL_CHARS = 4_000
MAX_ALERT_SERVICE_CHARS = 128
# Même empreinte revue dans la fenêtre => occurrences+1, pas de nouvelle ligne
# (anti-spam : une erreur répétée ne crée pas d'alerte, cf. dedup Telegram).
ALERT_DEDUP_WINDOW_S = 3_600
MAX_ALERTS = 5_000
ALERT_RETENTION_S = 90 * 86_400
MAX_ALERT_LIST = 100

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
