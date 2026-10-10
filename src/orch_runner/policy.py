"""Politique locale du runner : configuration, allowlists, environnement, jeton.

La configuration machine (chemins réels) vit hors Git :
%USERPROFILE%\\.orch-runner\\runner.toml (ORCH_RUNNER_HOME pour déroger).
Le broker ne reçoit que des identifiants.
"""

from __future__ import annotations

import ctypes
import hashlib
import os
import secrets
import stat
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import orch_protocol as P
from orch_runner import winproc

try:
    from orch_runner import posproc
except ImportError:  # pragma: no cover - vieux Python sans fcntl (Windows)
    posproc = None  # type: ignore[assignment]

# Hors AppData : les apps MSIX (ex. Claude Desktop) virtualisent AppData\Local pour leurs
# processus enfants ; une tâche planifiée ne verrait alors ni config ni jeton.
DEFAULT_HOME = Path(os.environ.get("ORCH_RUNNER_HOME", str(Path.home() / ".orch-runner")))

# Variables transmises aux agents. Tout le reste de l'environnement du runner est retiré.
# Windows (inchangé) :
ENV_ALLOW_WINDOWS = (
    "SYSTEMROOT", "WINDIR", "SYSTEMDRIVE", "COMSPEC", "PATH", "PATHEXT", "TEMP", "TMP",
    "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "APPDATA", "LOCALAPPDATA", "PROGRAMDATA",
    "PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMW6432", "COMMONPROGRAMFILES",
    "COMMONPROGRAMFILES(X86)", "COMPUTERNAME", "USERNAME", "USERDOMAIN", "OS",
    "PROCESSOR_ARCHITECTURE", "NUMBER_OF_PROCESSORS", "LANG", "TZ",
    "CLAUDE_CODE_GIT_BASH_PATH",
)
# POSIX (Linux ARM64) : PATH conservé (les CLIs node `codex`/`opencode`/`claude`
# sont des scripts `#!/usr/bin/env node`), HOME/TMPDIR pour les caches outils.
# Aucune clé d'API n'y figure : tout le reste est retiré comme sur Windows.
ENV_ALLOW_POSIX = (
    "PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "LC_CTYPE", "TZ",
    "TMPDIR", "TEMP", "TMP", "XDG_RUNTIME_DIR", "XDG_CACHE_HOME", "XDG_CONFIG_HOME",
    "TERM",
)
# Compatibilité : nom historique = allowlist de la plateforme courante.
ENV_ALLOW = ENV_ALLOW_WINDOWS if os.name == "nt" else ENV_ALLOW_POSIX


class PolicyError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class Workspace:
    id: str
    path: str
    modes: list[str]
    description: str = ""


@dataclass
class RuntimeConf:
    id: str
    enabled: bool
    exe: str
    extra: dict = field(default_factory=dict)


@dataclass
class Config:
    runner_id: str
    broker_url: str
    token_file: Path
    max_parallel: int
    workspaces: dict[str, Workspace]
    runtimes: dict[str, RuntimeConf]
    home: Path
    offline_kill_s: int = 120
    permission_policy: str = P.UNATTENDED
    # Re-probe des seuls runtimes indisponibles (backoff exponentiel borné).
    runtime_refresh_min_s: int = 60
    runtime_refresh_max_s: int = 900
    # Linux : le groupe de processus n'est pas un Job Object (un descendant
    # setsid/double-fork échappe au kill). Tant que cette isolation forte est
    # impossible, workspace_write est REFUSÉ par défaut (fail-closed) ; passer
    # à true assume explicitement ce risque. Sans effet sous Windows.
    linux_allow_workspace_write: bool = False

    @staticmethod
    def load(path: Path | None = None) -> Config:
        path = path or DEFAULT_HOME / "runner.toml"
        with open(path, "rb") as fh:
            raw = tomllib.load(fh)
        runner_id = raw.get("runner_id", "")
        if not P.valid_id(runner_id):
            raise PolicyError("config", "runner_id invalide")
        url = str(raw.get("broker_url", "")).rstrip("/")
        if not url.startswith(("http://10.", "https://")):
            raise PolicyError("config", "broker_url doit être l'IP overlay (http://10.x) ou https://")
        home = Path(raw.get("home", str(path.parent)))
        policy = raw.get("default_permission_policy", P.UNATTENDED)
        if policy not in P.PERMISSION_POLICIES:
            raise PolicyError("config", f"default_permission_policy invalide : {policy!r}")
        workspaces = {}
        for ws_id, ws in (raw.get("workspaces") or {}).items():
            if not P.valid_id(ws_id):
                raise PolicyError("config", f"workspace id invalide : {ws_id!r}")
            modes = [m for m in ws.get("modes", ["read_only"]) if m in P.MODES]
            workspaces[ws_id] = Workspace(ws_id, str(ws["path"]), modes, str(ws.get("description", ""))[:200])
        runtimes = {}
        for rt_id, rt in (raw.get("runtimes") or {}).items():
            if rt_id not in P.RUNTIMES:
                raise PolicyError("config", f"runtime inconnu : {rt_id!r}")
            runtimes[rt_id] = RuntimeConf(rt_id, bool(rt.get("enabled", False)), str(rt.get("exe", "")),
                                          {k: v for k, v in rt.items() if k not in ("enabled", "exe")})
        return Config(
            runner_id=runner_id,
            broker_url=url,
            token_file=Path(raw.get("token_file", str(home / "runner.token"))),
            max_parallel=max(1, min(int(raw.get("max_parallel", 1)), 8)),
            workspaces=workspaces,
            runtimes=runtimes,
            home=home,
            offline_kill_s=int(raw.get("offline_kill_s", 120)),
            permission_policy=policy,
            runtime_refresh_min_s=max(10, int(raw.get("runtime_refresh_min_s", 60))),
            runtime_refresh_max_s=max(10, int(raw.get("runtime_refresh_max_s", 900))),
            linux_allow_workspace_write=bool(raw.get("linux_allow_workspace_write", False)),
        )


# ------------------------------------------------------------------ workspace
def resolve_workspace(ws: Workspace) -> str:
    """Chemin canonique validé, ou PolicyError. Revalidé juste avant chaque lancement.

    Windows (inchangé) : refus UNC et chemins device, lecteur non fixe, `..`,
    dossier absent, et tout lien symbolique / jonction sur le chemin (realpath
    doit égaler le chemin configuré normalisé).

    POSIX (Linux) : absolu sans `..` ni NUL, hors préfixes système (/proc, /sys,
    /dev, /run, /boot, /etc, /root, racine), dossier existant, realpath strict
    égal au normalisé (aucun symlink/jonction sur le chemin), pas un point de
    montage déguisé (lstat non-lien).
    """
    if os.name == "nt":
        return _resolve_workspace_windows(ws)
    return _resolve_workspace_posix(ws)


def _resolve_workspace_windows(ws: Workspace) -> str:
    raw = ws.path
    if raw.startswith(("\\\\", "//")):
        raise PolicyError("workspace_denied", "chemin UNC/device refusé")
    if not os.path.isabs(raw) or ".." in Path(raw).parts:
        raise PolicyError("workspace_denied", "chemin absolu sans '..' requis")
    norm = os.path.normcase(os.path.normpath(raw))
    if not winproc.is_fixed_drive(norm):
        raise PolicyError("workspace_denied", "lecteur non fixe refusé")
    try:
        real = os.path.realpath(raw, strict=True)
    except OSError:
        raise PolicyError("workspace_denied", "workspace introuvable")
    real = real.removeprefix("\\\\?\\")
    if os.path.normcase(os.path.normpath(real)) != norm:
        raise PolicyError("workspace_denied", "lien symbolique/jonction dans le chemin du workspace")
    st = os.lstat(real)
    if not stat.S_ISDIR(st.st_mode):
        raise PolicyError("workspace_denied", "le workspace n'est pas un dossier")
    if getattr(st, "st_file_attributes", 0) & winproc.FILE_ATTRIBUTE_REPARSE_POINT:
        raise PolicyError("workspace_denied", "workspace = point d'analyse (reparse point)")
    return real


def _resolve_workspace_posix(ws: Workspace) -> str:
    """Validation Linux (fail-closed). Voir `resolve_workspace`."""
    raw = ws.path
    if not isinstance(raw, str) or not raw or "\x00" in raw:
        raise PolicyError("workspace_denied", "chemin invalide")
    if raw.startswith(("\\\\", "//")):
        raise PolicyError("workspace_denied", "chemin UNC/device refusé")
    if not os.path.isabs(raw) or ".." in Path(raw).parts:
        raise PolicyError("workspace_denied", "chemin absolu sans '..' requis")
    norm = os.path.normpath(raw)
    if posproc is not None and not posproc.is_safe_path(norm):
        raise PolicyError("workspace_denied", "chemin système refusé")
    try:
        real = os.path.realpath(raw, strict=True)
    except OSError:
        raise PolicyError("workspace_denied", "workspace introuvable")
    if os.path.normpath(real) != norm:
        raise PolicyError("workspace_denied", "lien symbolique dans le chemin du workspace")
    try:
        st = os.lstat(real)
    except OSError:
        raise PolicyError("workspace_denied", "workspace introuvable")
    if not stat.S_ISDIR(st.st_mode):
        raise PolicyError("workspace_denied", "le workspace n'est pas un dossier")
    if stat.S_ISLNK(st.st_mode):
        raise PolicyError("workspace_denied", "workspace = lien symbolique")
    return real


def child_env(job_id: str, extra: dict[str, str] | None = None) -> dict[str, str]:
    upper = {k.upper(): v for k, v in os.environ.items()}
    env = {k: upper[k] for k in ENV_ALLOW if k in upper}
    env["ORCH_JOB_ID"] = job_id
    env.update(extra or {})
    return env


# ---------------------------------------------------------------------- jeton
def _dpapi(data: bytes, protect: bool) -> bytes:
    class BLOB(ctypes.Structure):
        _fields_ = [("cbData", ctypes.c_ulong), ("pbData", ctypes.POINTER(ctypes.c_char))]

    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32")
    buf = ctypes.create_string_buffer(data, len(data))
    src = BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
    dst = BLOB()
    fn = crypt32.CryptProtectData if protect else crypt32.CryptUnprotectData
    ok = fn(ctypes.byref(src), None, None, None, None, 0x1, ctypes.byref(dst))  # UI_FORBIDDEN
    if not ok:
        raise PolicyError("token", f"DPAPI a échoué ({ctypes.get_last_error()})")
    try:
        return ctypes.string_at(dst.pbData, dst.cbData)
    finally:
        kernel32.LocalFree(dst.pbData)


def generate_token(path: Path) -> str:
    """Génère le jeton sur la machine du runner, le stocke localement.

    Windows (inchangé) : chiffré DPAPI (lié à l'utilisateur Windows).
    POSIX (Linux) : fichier brut mode 0600 (aucun DPAPI sous Linux).
    Retourne UNIQUEMENT son empreinte SHA-256 à déclarer côté VPS : le jeton ne quitte jamais la machine."""
    token = secrets.token_urlsafe(48)
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(_dpapi(token.encode(), True))
        os.replace(tmp, path)
    else:
        tmp = path.with_suffix(".tmp")
        fd = os.open(str(tmp), os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        try:
            os.write(fd, token.encode())
        finally:
            os.close(fd)
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass
        os.replace(tmp, path)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
    return hashlib.sha256(token.encode()).hexdigest()


def load_token(path: Path) -> str:
    if os.name == "nt":
        return _dpapi(path.read_bytes(), False).decode()
    st = os.stat(path)
    if not stat.S_ISREG(st.st_mode):
        raise PolicyError("token", "jeton : fichier régulier requis")
    if st.st_mode & 0o077:
        raise PolicyError("token", "jeton : permissions trop ouvertes (0600 requis)")
    token = path.read_bytes().strip().decode()
    if not token or len(token) < 32 or any(c.isspace() for c in token):
        raise PolicyError("token", "jeton invalide")
    return token
