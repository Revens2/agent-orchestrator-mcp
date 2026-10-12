"""Contrat partagé broker ↔ runner (version 1).

Tout ce qui traverse la frontière VPS ↔ PC est défini ici : noms d'états,
transitions autorisées, bornes et validation des identifiants. Le broker et le
runner importent ce module ; aucune des deux parties n'invente sa propre règle.
"""

from __future__ import annotations

import math
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
        (STARTING, LOST),  # durable runner recovery: exit outcome unavailable
        (RUNNING, COMPLETED),
        (RUNNING, FAILED),
        (RUNNING, TIMEOUT),
        (RUNNING, CANCELLED),
        (RUNNING, LOST),
    }
)


def transition_allowed(src: str, dst: str) -> bool:
    return dst in TRANSITIONS.get(src, frozenset())


# --- runtimes / modes ----------------------------------------------------
RUNTIMES = ("claude-code", "codex", "agy", "opencode", "fake", "hermes", "claude-desktop")
MODES = ("read_only", "workspace_write")
# Modes supportés par runtime (avant intersection avec la config du workspace).
# `claude-desktop` pilote l'application Claude Desktop (MSIX) via UIA : le prompt
# est transmis à l'UI, aucun confinement du processus au workspace n'est démontrable
# (l'app partage l'historique/le profil de l'utilisateur). `read_only` = réponse
# texte seule. `workspace_write` = VOIE PATCH CONFINÉE : le Desktop propose des
# diffs unifiés, le RUNNER les applique bornés au workspace (desktop_patch.py) ;
# le Desktop n'a aucun accès disque direct, aucune commande n'est exécutée.
RUNTIME_MODES: dict[str, tuple[str, ...]] = {
    "claude-desktop": ("read_only", "workspace_write"),
}
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
# Final output / durable recovery may finish after the process exits. This
# deadline starts at the FIRST negative observation, never at the next ping.
PROCESS_RECOVERY_S = 60
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
# Pause-quota (mission) : détection fournisseur, pause durable, réveil, reprise.
EV_PROVIDER_QUOTA_DETECTED = "provider_quota_detected"
EV_MISSION_PAUSED = "mission_paused"
EV_QUOTA_RETRY_SCHEDULED = "quota_retry_scheduled"
EV_MISSION_RESUMED = "mission_resumed"
EV_RESUME_FAILED = "resume_failed"
EV_PAUSE_CANCELLED = "pause_cancelled"
EVENT_KINDS = frozenset({
    EV_JOB_CLAIMED, EV_RUNTIME_SPAWNED, EV_PROCESS_RUNNING, EV_OUTPUT_PROGRESS,
    EV_ACTIVITY, EV_PROCESS_EXIT, EV_RUNNER_DISCONNECT, EV_LEASE_EXPIRED,
    EV_REQUEUED, EV_CANCEL_REQUESTED, EV_TIMEOUT_MARKED, EV_SUSPECTED_STALL, EV_STALLED,
    EV_SESSION_CORRUPTED, EV_SESSION_RECREATED,
    EV_PROVIDER_QUOTA_DETECTED, EV_MISSION_PAUSED, EV_QUOTA_RETRY_SCHEDULED,
    EV_MISSION_RESUMED, EV_RESUME_FAILED, EV_PAUSE_CANCELLED,
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
# Pause durable : le fournisseur du modèle est indisponible (quota/forfait
# épuisé, rate limit). La tentative est close (job `failed` préfixé
# PROVIDER_UNAVAILABLE_PREFIX) ; la mission attend puis repart sur un NOUVEAU
# job lié. Ni terminal, ni réussie, ni validable.
MISSION_WAITING_FOR_QUOTA = "waiting_for_quota"
# Annulation explicite de la mission (y compris pendant une pause) : terminal.
MISSION_CANCELLED = "cancelled"
MISSION_STATES = frozenset({
    MISSION_EXECUTING, MISSION_NEEDS_VALIDATION, MISSION_VALIDATED,
    MISSION_INCOMPLETE, MISSION_BLOCKED, MISSION_FAILED,
    MISSION_WAITING_FOR_QUOTA, MISSION_CANCELLED,
})
MISSION_FINAL = frozenset({MISSION_VALIDATED, MISSION_FAILED, MISSION_CANCELLED})
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


# --- fournisseur indisponible (pause-quota de mission) -----------------------
# Causes DISTINCTES : seules quota/rate_limit mettent la mission en pause
# durable ; auth/modèle/réseau/inconnu restent des échecs ordinaires (classe
# visible dans `error`). Signatures EXACTES issues des erreurs fournisseurs
# documentées ; un texte libre ambigu n'est JAMAIS promu en quota.
PROVIDER_QUOTA = "quota_exhausted"
PROVIDER_RATE_LIMIT = "rate_limited"
PROVIDER_AUTH = "auth_required"
PROVIDER_MODEL_UNAVAILABLE = "model_unavailable"
PROVIDER_CLASSES = frozenset({PROVIDER_QUOTA, PROVIDER_RATE_LIMIT, PROVIDER_AUTH, PROVIDER_MODEL_UNAVAILABLE})
PAUSE_CLASSES = frozenset({PROVIDER_QUOTA, PROVIDER_RATE_LIMIT})
PROVIDER_UNAVAILABLE_PREFIX = "provider_unavailable:"
PROVIDER_ERROR_PREFIX = "provider_error:"

# Sources : opencode provider/error.ts (parseStreamError), retry.ts (Zen
# FreeUsageLimitError/GoUsageLimitError), fixture Codex `usage_limit_reached` ;
# codes d'erreur API publics OpenAI (`insufficient_quota`) / Anthropic.
QUOTA_SIGNS = (
    "insufficient_quota",                        # code OpenAI (body/stream)
    "Quota exceeded. Check your plan and billing details",  # opencode parseStreamError
    "You exceeded your current quota",           # message OpenAI APICallError
    "Your credit balance is too low",            # Anthropic, crédits épuisés
    "usage_limit_reached",                       # Codex/ChatGPT (type d'erreur)
    "The usage limit has been reached",          # Codex/ChatGPT (message)
    "GoUsageLimitError",                         # opencode Zen Go (body)
    "usage limit` reached",                      # opencode Zen Go (message réécrit)
    "FreeUsageLimitError",                       # opencode Zen free tier (body)
    "Free usage exceeded",                       # opencode Zen free tier (message)
)
RATE_LIMIT_SIGNS = (
    "rate_limit_error",                          # type d'erreur Anthropic
    "rate_limit_exceeded",                       # code OpenAI
)
AUTH_SIGNS = (
    "ProviderAuthError",                         # nom d'erreur opencode
    "invalid_api_key",
    "authentication_error",
)
MODEL_UNAVAILABLE_SIGNS = (
    "ProviderModelNotFoundError",                # nom d'erreur opencode
    "model_not_found",
)

# Politique de réveil : backoff borné (base par cause, x2 par pause, plafond),
# retry_after fournisseur respecté s'il est fourni (borné), durée totale de
# pause et nombre de pauses consécutives plafonnés (=> incomplete, humain).
PAUSE_BACKOFF_BASE_S = {PROVIDER_QUOTA: 900, PROVIDER_RATE_LIMIT: 120}
PAUSE_BACKOFF_MAX_S = 6 * 3_600
PAUSE_MIN_DELAY_S = 60
MAX_PAUSE_TOTAL_S = 7 * 86_400
MAX_CONSECUTIVE_PAUSES = 12
PAUSE_JITTER_RATIO = 0.1


def classify_provider_error(text: object, status_code: object = None) -> str | None:
    """Classe fournisseur depuis une erreur EXACTE, sinon None.

    Ordre : quota avant rate limit (un 429 `insufficient_quota` est un quota).
    `status_code` n'est pris en compte que s'il vient d'une erreur fournisseur
    typée (APIError) : 429 seul => rate_limited, 401/403 seul => auth_required.
    Jamais de déduction depuis un mot isolé ("limit", "error", "429" en texte).
    """
    s = text if isinstance(text, str) else ""
    for signs, cls in (
        (QUOTA_SIGNS, PROVIDER_QUOTA),
        (RATE_LIMIT_SIGNS, PROVIDER_RATE_LIMIT),
        (AUTH_SIGNS, PROVIDER_AUTH),
        (MODEL_UNAVAILABLE_SIGNS, PROVIDER_MODEL_UNAVAILABLE),
    ):
        if any(sign in s for sign in signs):
            return cls
    if isinstance(status_code, int) and not isinstance(status_code, bool):
        if status_code == 429:
            return PROVIDER_RATE_LIMIT
        if status_code in (401, 403):
            return PROVIDER_AUTH
    return None


_RESET_IN_RE = re.compile(r"reset in\s+((?:\d+\s*(?:days?|hours?|minutes?|seconds?)[\s,]*(?:and\s+)?)+)", re.IGNORECASE)
_RESET_UNIT_RE = re.compile(r"(\d+)\s*(day|hour|minute|second)", re.IGNORECASE)
_RESET_UNIT_S = {"day": 86_400, "hour": 3_600, "minute": 60, "second": 1}


def parse_reset_in(text: object) -> float | None:
    """Délai annoncé « It will reset in 5 hours 23 minutes » (opencode Zen Go),
    en secondes ; None si absent. Ne lit jamais une date libre."""
    if not isinstance(text, str):
        return None
    m = _RESET_IN_RE.search(text)
    if m is None:
        return None
    total = sum(int(n) * _RESET_UNIT_S[u.lower()] for n, u in _RESET_UNIT_RE.findall(m.group(1)))
    return float(total) if total > 0 else None


def finite_delay(value: object) -> float | None:
    """Délai fournisseur exploitable : nombre fini > 0, plafonné à MAX_PAUSE_TOTAL_S."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        return None
    return float(min(value, MAX_PAUSE_TOTAL_S))


def format_provider_error(cls: str, message: str | None, retry_after_s: float | None = None) -> str:
    """`error` d'un job clos pour cause fournisseur (contrat runner -> broker).

    `provider_unavailable:<classe>[ retry_after_s=N] | msg` pour une cause de
    pause, `provider_error:<classe> | msg` sinon. Délai RELATIF (pas d'horloge
    PC transmise : le broker l'ancre sur la sienne)."""
    if cls in PAUSE_CLASSES:
        head = PROVIDER_UNAVAILABLE_PREFIX + cls
        retry = finite_delay(retry_after_s)
        if retry is not None:
            head += f" retry_after_s={int(retry)}"
    else:
        head = PROVIDER_ERROR_PREFIX + cls
    return head + (f" | {message}" if message else "")


def parse_provider_pause(error: object) -> tuple[str, float | None] | None:
    """(classe, retry_after_s|None) si `error` porte une cause de PAUSE, sinon None."""
    if not isinstance(error, str) or not error.startswith(PROVIDER_UNAVAILABLE_PREFIX):
        return None
    head = error[len(PROVIDER_UNAVAILABLE_PREFIX):].split(" | ", 1)[0].split()
    if not head or head[0] not in PAUSE_CLASSES:
        return None
    retry: float | None = None
    for tok in head[1:]:
        if tok.startswith("retry_after_s="):
            try:
                retry = finite_delay(float(tok.split("=", 1)[1]))
            except ValueError:
                retry = None
    return head[0], retry


def pause_delay_s(cls: str, pause_count: int, retry_after_s: float | None, jitter: float = 0.0) -> float:
    """Délai avant réveil. retry_after fiable => respecté (borné) ; sinon
    backoff exponentiel borné. `jitter` ∈ [0,1] (aléa fourni par l'appelant)."""
    retry = finite_delay(retry_after_s)
    if retry is not None:
        base = retry
    else:
        base = PAUSE_BACKOFF_BASE_S.get(cls, PAUSE_BACKOFF_BASE_S[PROVIDER_QUOTA]) * (2 ** max(0, min(pause_count, 16)))
        base = min(base, PAUSE_BACKOFF_MAX_S)
    base = max(PAUSE_MIN_DELAY_S, min(base, MAX_PAUSE_TOTAL_S))
    return base + base * PAUSE_JITTER_RATIO * max(0.0, min(1.0, jitter))
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
