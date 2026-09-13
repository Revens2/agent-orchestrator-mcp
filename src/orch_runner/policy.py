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

# Hors AppData : les apps MSIX (ex. Claude Desktop) virtualisent AppData\Local pour leurs
# processus enfants ; une tâche planifiée ne verrait alors ni config ni jeton.
DEFAULT_HOME = Path(os.environ.get("ORCH_RUNNER_HOME", str(Path.home() / ".orch-runner")))

# Variables transmises aux agents. Tout le reste de l'environnement du runner est retiré.
ENV_ALLOW = (
    "SYSTEMROOT", "WINDIR", "SYSTEMDRIVE", "COMSPEC", "PATH", "PATHEXT", "TEMP", "TMP",
    "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "APPDATA", "LOCALAPPDATA", "PROGRAMDATA",
    "PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMW6432", "COMMONPROGRAMFILES",
    "COMMONPROGRAMFILES(X86)", "COMPUTERNAME", "USERNAME", "USERDOMAIN", "OS",
    "PROCESSOR_ARCHITECTURE", "NUMBER_OF_PROCESSORS", "LANG", "TZ",
    "CLAUDE_CODE_GIT_BASH_PATH",
)


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
        )


# ------------------------------------------------------------------ workspace
def resolve_workspace(ws: Workspace) -> str:
    """Chemin canonique validé, ou PolicyError. Revalidé juste avant chaque lancement.

    Refus : chemin relatif, UNC / \\\\?\\ / \\\\.\\, lecteur non fixe, `..`, dossier
    absent, et tout lien symbolique / jonction sur le chemin (realpath doit
    égaler le chemin configuré normalisé).
    """
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
    """Génère le jeton sur le PC, le stocke chiffré DPAPI (lié à l'utilisateur Windows).
    Retourne UNIQUEMENT son empreinte SHA-256 à déclarer côté VPS : le jeton ne quitte jamais le PC."""
    token = secrets.token_urlsafe(48)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(_dpapi(token.encode(), True))
    os.replace(tmp, path)
    return hashlib.sha256(token.encode()).hexdigest()


def load_token(path: Path) -> str:
    return _dpapi(path.read_bytes(), False).decode()
