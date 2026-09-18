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

# --- qui est qui : cartes d'identité des runtimes -------------------------
# Constat : `claude-code` et `claude-desktop` se ressemblent trop pour être
# distingués d'après leur seul identifiant, et un appelant qui confond les deux
# choisit le mauvais outil. Chaque runtime porte donc une carte d'identité
# explicite, exposée telle quelle par agent_runner_list / agent_runner_inspect.
# `distinct_from` nomme la confusion à éviter, dans les deux sens.
RUNTIME_IDENTITY: dict[str, dict[str, object]] = {
    "claude-code": {
        # `native` = le runtime documente ses propres sous-agents ; `unknown` =
        # non vérifié, donc jamais annoncé comme acquis ; `none` = sans objet.
        "subagents": "native",
        "label": "Claude Code (CLI)",
        "vendor": "Anthropic",
        "kind": "cli",
        "interface": "processus headless lancé par le runner (argv figé, aucune UI)",
        "use_when": "travail sur le code d'un workspace : lecture, analyse, écriture de fichiers",
        "distinct_from": "claude-desktop",
        "distinction": (
            "claude-code est la CLI headless : un processus par job, sans fenêtre, "
            "confiné au workspace. claude-desktop est l'APPLICATION DE BUREAU de "
            "l'utilisateur, pilotée par son interface : elle partage son profil et "
            "son historique, et n'écrit jamais directement sur le disque."
        ),
    },
    "claude-desktop": {
        # `native` = le runtime documente ses propres sous-agents ; `unknown` =
        # non vérifié, donc jamais annoncé comme acquis ; `none` = sans objet.
        "subagents": "unknown",
        "label": "Claude Desktop (application de bureau)",
        "vendor": "Anthropic",
        "kind": "desktop_app",
        "interface": "application MSIX pilotée par UI (UIA), accès sérialisé, profil vérifié",
        "use_when": "poser une question à l'app de bureau, ou obtenir des patchs appliqués par le runner",
        "distinct_from": "claude-code",
        "distinction": (
            "claude-desktop pilote la FENÊTRE de l'application de l'utilisateur : une "
            "seule à la fois, profil et historique partagés avec lui, aucun accès "
            "disque ni shell (workspace_write = diffs appliqués par le runner). "
            "claude-code est la CLI headless, sans fenêtre et sans profil partagé."
        ),
    },
    "codex": {
        # `native` = le runtime documente ses propres sous-agents ; `unknown` =
        # non vérifié, donc jamais annoncé comme acquis ; `none` = sans objet.
        "subagents": "unknown",
        "label": "Codex (CLI)",
        "vendor": "OpenAI",
        "kind": "cli",
        "interface": "processus headless lancé par le runner",
        "use_when": "travail sur le code avec les modèles OpenAI",
        "distinct_from": None,
        "distinction": None,
    },
    "agy": {
        # `native` = le runtime documente ses propres sous-agents ; `unknown` =
        # non vérifié, donc jamais annoncé comme acquis ; `none` = sans objet.
        "subagents": "unknown",
        "label": "Antigravity (CLI `agy`)",
        "vendor": "Google",
        "kind": "cli",
        "interface": "processus headless lancé par le runner",
        "use_when": "travail sur le code avec les modèles Google",
        "distinct_from": None,
        "distinction": None,
    },
    "opencode": {
        # `native` = le runtime documente ses propres sous-agents ; `unknown` =
        # non vérifié, donc jamais annoncé comme acquis ; `none` = sans objet.
        "subagents": "native",
        "label": "OpenCode (CLI)",
        "vendor": "SST",
        "kind": "cli",
        "interface": "processus headless `opencode run`, une conversation par session",
        "use_when": "travail sur le code ; seul runtime sachant reprendre une conversation existante",
        "distinct_from": None,
        "distinction": None,
    },
    "hermes": {
        # `native` = le runtime documente ses propres sous-agents ; `unknown` =
        # non vérifié, donc jamais annoncé comme acquis ; `none` = sans objet.
        "subagents": "unknown",
        "label": "Hermes (runner VPS)",
        "vendor": "interne",
        "kind": "service",
        "interface": "poller sur le VPS vps-etude, pas sur un PC personnel",
        "use_when": "travaux exécutés côté serveur, jamais sur une machine de l'utilisateur",
        "distinct_from": None,
        "distinction": None,
    },
    "fake": {
        # `native` = le runtime documente ses propres sous-agents ; `unknown` =
        # non vérifié, donc jamais annoncé comme acquis ; `none` = sans objet.
        "subagents": "none",
        "label": "Agent factice (tests)",
        "vendor": "interne",
        "kind": "test",
        "interface": "processus de test déterministe",
        "use_when": "vérifier la chaîne de bout en bout ; jamais un vrai travail",
        "distinct_from": None,
        "distinction": None,
    },
}


def runtime_identity(runtime: object) -> dict[str, object] | None:
    """Carte d'identité d'un runtime, ou None s'il n'est pas connu (jamais inventée)."""
    if not isinstance(runtime, str):
        return None
    card = RUNTIME_IDENTITY.get(runtime)
    return dict(card) if card else None


# --- début de conversation : skill de départ et sous-agents ----------------
# Deux demandes qui partagent un seul mécanisme : ce que l'agent reçoit AVANT
# la mission, à l'ouverture de sa conversation.
#
# 1. Un skill de départ (ex. « /caveman ultra ») doit être la PREMIÈRE ligne du
#    prompt pour être interprété comme une commande, et seuls les runtimes qui
#    comprennent les commandes `/skill` peuvent l'honorer. Un runtime qui ne les
#    comprend pas recevrait la ligne comme du texte : on ne l'envoie donc pas,
#    plutôt que de polluer sa mission.
# 2. Les sous-agents sont une capacité du runtime lui-même. Le préambule ne la
#    crée pas : il AUTORISE explicitement la délégation, pour les runtimes qui
#    la documentent. Là où elle n'est pas vérifiée (`unknown`), on ne dit rien
#    plutôt que de promettre une capacité qui n'existe peut-être pas.
#
# Rien de tout ceci n'est réinjecté quand on REPREND une conversation : elle a
# déjà commencé, et le skill de départ ne se rejoue pas.
# claude-code seulement : sa CLI interprète une commande `/skill` placée en
# première ligne du prompt. `claude-desktop` passe par l'UI et n'a PAS été
# vérifié sur ce point : on ne l'ajoute pas tant que ce n'est pas constaté.
SLASH_SKILL_RUNTIMES = frozenset({"claude-code"})
DEFAULT_START_SKILL = "/caveman ultra"
MAX_START_SKILL_CHARS = 120
SUBAGENT_PERMISSION = (
    "[orchestrateur] Tu peux déléguer à des sous-agents quand la tâche s'y prête "
    "(exploration large, travaux parallèles indépendants, relecture) : c'est "
    "autorisé et encouragé. Tu restes responsable du résultat final."
)


def supports_slash_skill(runtime: object) -> bool:
    return isinstance(runtime, str) and runtime in SLASH_SKILL_RUNTIMES


def subagent_support(runtime: object) -> str:
    """`native`, `none` ou `unknown` (défaut prudent pour un runtime inconnu)."""
    card = RUNTIME_IDENTITY.get(runtime) if isinstance(runtime, str) else None
    return str(card.get("subagents", "unknown")) if card else "unknown"


def build_session_preamble(
    runtime: object,
    start_skill: str | None = None,
    subagents: bool = True,
    resuming: bool = False,
) -> list[str]:
    """Lignes à placer AVANT la mission, à l'ouverture d'une conversation.

    Retourne une liste vide quand il n'y a rien de légitime à ajouter : reprise
    de conversation, runtime qui ne comprend pas les commandes `/skill`, ou
    capacité sous-agents non vérifiée. On préfère ne rien dire à promettre.
    """
    if resuming:
        return []
    lines: list[str] = []
    skill = (start_skill or "").strip()[:MAX_START_SKILL_CHARS]
    if skill and supports_slash_skill(runtime):
        # PREMIÈRE ligne, seule sur sa ligne : c'est la condition pour qu'une
        # commande `/skill` soit interprétée comme telle et non comme du texte.
        lines.append(skill)
    if subagents and subagent_support(runtime) == "native":
        lines.append(SUBAGENT_PERMISSION)
    return lines


def apply_session_preamble(prompt: str, lines: list[str]) -> str:
    """Prompt préfixé du préambule. Le prompt de l'utilisateur n'est jamais
    modifié ni tronqué : il est seulement précédé."""
    if not lines:
        return prompt
    return "\n\n".join([*lines, prompt])


# --- qui est qui : identité d'une machine runner --------------------------
# Un `runner_id` opaque ne dit pas de QUELLE machine il s'agit. Le runner peut
# déclarer une carte d'identité lisible ; tout champ non déclaré reste absent
# (null), jamais deviné. Aucun chemin, aucun secret : seulement de quoi nommer
# la machine dans une phrase.
MACHINE_FIELDS = ("label", "hostname", "os", "role", "description")
MAX_MACHINE_FIELD_CHARS = 120


def machine_identity(info: object) -> dict[str, object]:
    """Normalise la carte d'identité machine déclarée par un runner."""
    src = info if isinstance(info, dict) else {}
    out: dict[str, object] = {}
    for key in MACHINE_FIELDS:
        value = src.get(key)
        out[key] = str(value)[:MAX_MACHINE_FIELD_CHARS] if isinstance(value, str) and value.strip() else None
    return out


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
WAITING_FOR_HUMAN = "waiting_for_human"    # pause humaine explicite : silence VOULU, jamais une panne
EXECUTION_HEALTH = frozenset({HEALTHY, IDLE, SUSPECTED_STALL, STALLED, RUNNER_DISCONNECTED,
                              PROCESS_DEAD, WAITING_FOR_HUMAN})

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
# Pause humaine explicite (signal de vie) : le silence est VOULU, pas une panne.
EV_PAUSED = "paused"
EV_RESUMED = "resumed"
EV_PAUSE_EXPIRED = "pause_expired"
# Reprise sur session corrompue : session abandonnée (jamais réutilisée),
# nouvelle session propre via mission_retry, handoff minimal (pas de transcript).
EV_SESSION_CORRUPTED = "session_corrupted"
EV_SESSION_RECREATED = "session_recreated"
EVENT_KINDS = frozenset({
    EV_JOB_CLAIMED, EV_RUNTIME_SPAWNED, EV_PROCESS_RUNNING, EV_OUTPUT_PROGRESS,
    EV_ACTIVITY, EV_PROCESS_EXIT, EV_RUNNER_DISCONNECT, EV_LEASE_EXPIRED,
    EV_REQUEUED, EV_CANCEL_REQUESTED, EV_TIMEOUT_MARKED, EV_SUSPECTED_STALL, EV_STALLED,
    EV_SESSION_CORRUPTED, EV_SESSION_RECREATED,
    EV_PAUSED, EV_RESUMED, EV_PAUSE_EXPIRED,
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


# --- pause humaine explicite : « je suis vivant, n'abandonne pas » -----------
# Problème résolu : une attente HUMAINE (changement de clé d'API après quota
# épuisé, login à refaire, pause volontaire) était indistinguable d'une panne.
# Le processus mort faisait démarrer PROCESS_RECOVERY_S -> `lost` en 60 s, et le
# silence faisait tomber le job en `suspected_stall`/`stalled`. Une pause
# EXPLICITE suspend ces trois comptes à rebours (recovery, stall, timeout dur)
# et rend le silence lisible : execution_health = WAITING_FOR_HUMAN.
# Une pause ne falsifie AUCUNE observation : proc_alive/telemetry restent ce
# qu'ils sont ; seule l'horloge des verdicts est suspendue, et elle est bornée.
PAUSE_QUOTA = "quota_exhausted"   # quota/crédit épuisé : nouvelle clé d'API attendue
PAUSE_AUTH = "auth_required"      # session/jeton expiré : login à refaire
PAUSE_MANUAL = "manual"           # pause volontaire déclarée par l'humain
PAUSE_QUESTION = "question"       # question waiting_for_user ouverte
PAUSE_REASONS = (PAUSE_QUOTA, PAUSE_AUTH, PAUSE_MANUAL, PAUSE_QUESTION)
PAUSE_DEFAULT_S = 1_800           # durée attendue par défaut d'une pause
PAUSE_MAX_S = 6 * 3_600           # borne dure : au-delà la pause expire, les comptes repartent
MAX_PAUSE_NOTE_CHARS = 300
# Sources autorisées (traçabilité : qui a déclaré la pause).
PAUSE_SRC_RUNNER = "runner"       # auto-détection sur la sortie de l'agent
PAUSE_SRC_HUMAN = "human"         # CLI locale sur le VPS
PAUSE_SRC_CHATGPT = "chatgpt-web"  # outil MCP agent_job_pause
PAUSE_SOURCES = (PAUSE_SRC_RUNNER, PAUSE_SRC_HUMAN, PAUSE_SRC_CHATGPT)

# Signatures EXACTES (sous-chaînes littérales, jamais un match naïf sur
# « error » ou « limit ») des blocages qui demandent une action humaine.
# Même discipline que SESSION_CORRUPTION_SIGNS : tout texte ne contenant
# aucune de ces formes n'est PAS un blocage humain.
QUOTA_EXHAUSTED_SIGNS = (
    "insufficient_quota",
    "You exceeded your current quota",
    "billing_hard_limit_reached",
    "credit balance is too low",
    "Credit balance too low",
    "usage limit reached",
    "quota exceeded",
    "run out of credits",
)
AUTH_REQUIRED_SIGNS = (
    "invalid_api_key",
    "Incorrect API key provided",
    "authentication_error",
    "401 Unauthorized",
    "OAuth token has expired",
    "auth login",
)


def match_pause_reason(text: object) -> tuple[str, str] | None:
    """(raison, signature exacte) si le texte porte un blocage qui demande une
    action humaine, sinon None. Le quota prime sur l'auth (une clé épuisée
    renvoie souvent aussi une erreur d'authentification)."""
    if not isinstance(text, str):
        return None
    for sign in QUOTA_EXHAUSTED_SIGNS:
        if sign in text:
            return PAUSE_QUOTA, sign
    for sign in AUTH_REQUIRED_SIGNS:
        if sign in text:
            return PAUSE_AUTH, sign
    return None


# --- reprise après changement de clé : il faut un PROCESSUS NEUF ----------
# Constat d'exploitation : les runtimes lisent leurs identifiants au démarrage.
# Une nouvelle clé d'API n'est donc JAMAIS rechargée par le processus en cours —
# il faut l'éteindre et le relancer. Une pause `quota_exhausted`/`auth_required`
# ne se lève donc pas toute seule pour ces runtimes : elle demande une RELANCE.
CREDENTIAL_RELOAD_NEEDS_RESTART = frozenset(
    {"opencode", "claude-code", "codex", "agy", "claude-desktop"}
)
# Runtimes capables de REPRENDRE une conversation existante dans un processus
# neuf, et par quel drapeau. Vérifié sur la CLI du runtime : `opencode run
# --session <id>` (« Session ID to continue »). Un runtime absent de cette table
# ne sait pas reprendre : la relance repart d'une conversation neuve avec un
# handoff court, et on le DIT au lieu de le laisser croire.
RUNTIME_RESUME_FLAG: dict[str, str] = {
    "opencode": "--session",
}


def can_resume_session(runtime: object) -> bool:
    return isinstance(runtime, str) and runtime in RUNTIME_RESUME_FLAG


def needs_restart_for_credentials(runtime: object) -> bool:
    return isinstance(runtime, str) and runtime in CREDENTIAL_RELOAD_NEEDS_RESTART


# Message humain associé à une raison de pause (affiché tel quel à l'utilisateur).
PAUSE_MESSAGES = {
    PAUSE_QUOTA: "quota ou crédit épuisé : l'agent attend une nouvelle clé d'API",
    PAUSE_AUTH: "authentification expirée : l'agent attend une reconnexion",
    PAUSE_MANUAL: "pause volontaire : l'agent attend le feu vert de l'utilisateur",
    PAUSE_QUESTION: "question en attente : l'agent attend une réponse de l'utilisateur",
}
