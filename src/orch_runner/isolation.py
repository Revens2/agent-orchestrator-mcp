"""Exécution isolée + preuves `runner_observed` (Autopilot, runner dédié `autopilot-win-01`).

Deux identités Windows :
- compte A (runner) : exécute ce code, détient le jeton broker, le mot de passe de B et la clé de
  signature (DPAPI de A). Lecture seule sur le workspace (hachage).
- compte B (agent)  : standard, sans jeton ni clé. OpenCode est lancé sous B via
  CreateProcessWithLogonW (aucun privilège requis côté A), suspendu, rattaché au Job Object.

Avant chaque job : sondes OS exécutées SOUS B (lecture de secrets, écriture hors workspace, HKLM)
=> chaque refus vient de Windows, pas du prompt. Manifeste du workspace avant/après (arbre tué).
Enveloppe signée en tête du résumé (orch_protocol.evidence) : l'agent ne peut ni la forger ni la rejouer.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import stat
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from orch_protocol import evidence as EV

IS_WINDOWS = os.name == "nt"
FILE_ATTRIBUTE_REPARSE_POINT = 0x400
ADMINS_SID = "S-1-5-32-544"


class IsolationError(Exception):
    pass


@dataclass
class IsolationConf:
    agent_user: str
    credential_file: Path              # mot de passe de B, chiffré DPAPI (compte A)
    signing_key_file: Path             # clé privée Ed25519 (32 octets), chiffrée DPAPI (compte A)
    probe_python: str                  # python.exe lisible par B (installation machine, pas un profil)
    deny_read: list[str] = field(default_factory=list)   # fichiers que B ne doit PAS lire (doivent exister)
    deny_write: list[str] = field(default_factory=list)  # dossiers où B ne doit PAS créer de fichier
    agent_profile: str = ""            # défaut C:\Users\<agent_user>
    max_files: int = 20_000
    max_bytes: int = 512 * 1024 * 1024

    @staticmethod
    def from_toml(raw: dict) -> IsolationConf:
        user = str(raw.get("agent_user", ""))
        if not user or not user.replace("-", "").replace("_", "").isalnum():
            raise IsolationError("isolation.agent_user invalide")
        conf = IsolationConf(
            agent_user=user,
            credential_file=Path(raw["credential_file"]),
            signing_key_file=Path(raw["signing_key_file"]),
            probe_python=str(raw["probe_python"]),
            deny_read=[str(p) for p in raw.get("deny_read", [])],
            deny_write=[str(p) for p in raw.get("deny_write", [])],
            agent_profile=str(raw.get("agent_profile") or rf"C:\Users\{user}"),
            max_files=int(raw.get("max_files", 20_000)),
            max_bytes=int(raw.get("max_bytes", 512 * 1024 * 1024)),
        )
        if not conf.deny_read or not conf.deny_write:
            raise IsolationError("isolation : deny_read et deny_write doivent lister au moins un chemin")
        return conf


# ------------------------------------------------------------------ manifeste
@dataclass
class Scan:
    files: dict[str, dict]
    reparse_points: list[str]
    alternate_streams: list[str]
    ads_checked: bool
    complete: bool


def scan_workspace(root: str, max_files: int = 20_000, max_bytes: int = 512 * 1024 * 1024) -> Scan:
    """Manifeste {chemin_posix: {sha256, size}} SANS suivre les reparse points (signalés). `.git` inclus.

    Plafond dépassé => complete=False (jamais de troncature silencieuse).
    """
    files: dict[str, dict] = {}
    reparse: list[str] = []
    ads: list[str] = []
    total = 0
    complete = True
    ads_ok = IS_WINDOWS
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            entries = list(os.scandir(current))
        except OSError:
            complete = False
            continue
        for entry in entries:
            rel = os.path.relpath(entry.path, root).replace(os.sep, "/")
            try:
                st = entry.stat(follow_symlinks=False)
            except OSError:
                complete = False
                continue
            attrs = getattr(st, "st_file_attributes", 0)
            if entry.is_symlink() or attrs & FILE_ATTRIBUTE_REPARSE_POINT:
                reparse.append(rel)
                continue
            if stat.S_ISDIR(st.st_mode):
                stack.append(entry.path)
                continue
            if not stat.S_ISREG(st.st_mode):
                reparse.append(rel)  # périphérique, socket… : hors contrat
                continue
            if len(files) >= max_files or total + st.st_size > max_bytes:
                return Scan(files, reparse, ads, ads_ok, False)
            try:
                files[rel] = {"sha256": _sha256_file(entry.path), "size": st.st_size}
            except OSError:
                complete = False
                continue
            total += st.st_size
            if IS_WINDOWS:
                streams = _alternate_streams(entry.path)
                if streams is None:
                    ads_ok = False
                elif streams:
                    ads.extend(f"{rel}:{s}" for s in streams)
    return Scan(files, sorted(reparse), sorted(ads), ads_ok, complete)


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _alternate_streams(path: str) -> list[str] | None:
    """Flux NTFS nommés (hors `::$DATA`). None = vérification impossible (fail-closed côté superviseur)."""
    from ctypes import wintypes

    class WIN32_FIND_STREAM_DATA(ctypes.Structure):
        _fields_ = [("StreamSize", ctypes.c_longlong), ("cStreamName", ctypes.c_wchar * 296)]

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.FindFirstStreamW.restype = wintypes.HANDLE
    k32.FindNextStreamW.argtypes = [wintypes.HANDLE, ctypes.c_void_p]
    k32.FindClose.argtypes = [wintypes.HANDLE]
    data = WIN32_FIND_STREAM_DATA()
    h = k32.FindFirstStreamW(path, 0, ctypes.byref(data), 0)
    if h in (None, ctypes.c_void_p(-1).value):
        return [] if ctypes.get_last_error() == 38 else None  # ERROR_HANDLE_EOF : aucun flux
    names = []
    try:
        while True:
            if data.cStreamName != "::$DATA":
                names.append(data.cStreamName)
            if not k32.FindNextStreamW(h, ctypes.byref(data)):
                break
    finally:
        k32.FindClose(h)
    return names


# ------------------------------------------------------------------ identité (jeton Windows)
PROBE_SCRIPT = r'''
import ctypes, json, os, sys, uuid
from ctypes import wintypes
a = ctypes.WinDLL("advapi32", use_last_error=True); k = ctypes.WinDLL("kernel32", use_last_error=True)
k.GetCurrentProcess.restype = wintypes.HANDLE
def info(cls):
    h = wintypes.HANDLE(); a.OpenProcessToken(wintypes.HANDLE(-1), 8, ctypes.byref(h))
    n = wintypes.DWORD(); a.GetTokenInformation(h, cls, None, 0, ctypes.byref(n))
    buf = ctypes.create_string_buffer(n.value); a.GetTokenInformation(h, cls, buf, n, ctypes.byref(n)); return buf
def sid_str(p):
    s = ctypes.c_wchar_p(); a.ConvertSidToStringSidW(ctypes.c_void_p(p), ctypes.byref(s)); return s.value
user = sid_str(ctypes.cast(info(1), ctypes.POINTER(ctypes.c_void_p))[0])
g = info(2); count = ctypes.cast(g, ctypes.POINTER(wintypes.DWORD))[0]
off = ctypes.sizeof(ctypes.c_void_p); groups = []
for i in range(count):
    base = ctypes.addressof(g) + off + i * 2 * ctypes.sizeof(ctypes.c_void_p)
    sid = ctypes.c_void_p.from_address(base).value; attrs = wintypes.DWORD.from_address(base + ctypes.sizeof(ctypes.c_void_p)).value
    groups.append((sid_str(sid), attrs))
elevated = bool(ctypes.cast(info(20), ctypes.POINTER(wintypes.DWORD))[0])
il = info(25); il_sid = ctypes.cast(il, ctypes.POINTER(ctypes.c_void_p))[0]
a.GetSidSubAuthorityCount.restype = ctypes.c_void_p
cnt = ctypes.cast(a.GetSidSubAuthorityCount(ctypes.c_void_p(il_sid)), ctypes.POINTER(ctypes.c_ubyte))[0]
a.GetSidSubAuthority.restype = ctypes.POINTER(wintypes.DWORD)
integrity = a.GetSidSubAuthority(ctypes.c_void_p(il_sid), cnt - 1)[0]
cfg = json.loads(sys.stdin.read())
probes = []
def denied(fn):
    try:
        fn(); return False, "allowed"
    except PermissionError:
        return True, "access_denied"
    except FileNotFoundError:
        return False, "missing"
    except OSError as e:
        return (getattr(e, "winerror", 0) == 5), type(e).__name__
for p in cfg["deny_read"]:
    d, why = denied(lambda: open(p, "rb").read(1)); probes.append({"probe": "read", "path": p, "denied": d, "why": why})
for p in cfg["deny_write"]:
    t = os.path.join(p, "autopilot-probe-" + uuid.uuid4().hex)
    def w():
        fd = os.open(t, os.O_WRONLY | os.O_CREAT | os.O_EXCL); os.close(fd); os.remove(t)
    d, why = denied(w); probes.append({"probe": "write", "path": p, "denied": d, "why": why})
import winreg
def hklm():
    winreg.CloseKey(winreg.CreateKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\autopilot-probe"))
    winreg.DeleteKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\autopilot-probe")
d, why = denied(hklm); probes.append({"probe": "write_hklm", "path": "HKLM\\SOFTWARE", "denied": d, "why": why})
admin = any(s == "S-1-5-32-544" for s, f in groups)  # membre, même en deny-only (UAC) : peut s'élever
print(json.dumps({"sid": user, "is_admin": admin, "elevated": elevated, "integrity_rid": integrity, "probes": probes}))
'''


def self_identity() -> dict[str, Any]:
    """Identité du processus courant (compte A), même code que la sonde, exécuté en processus."""
    out = subprocess.run([sys.executable, "-I", "-c", PROBE_SCRIPT], input='{"deny_read": [], "deny_write": []}',
                         capture_output=True, text=True, timeout=30, creationflags=0x08000000 if IS_WINDOWS else 0)
    if out.returncode != 0:
        raise IsolationError(f"identité runner illisible : {out.stderr[-300:]}")
    data = json.loads(out.stdout)
    data.pop("probes", None)
    return data


def run_probe(conf: IsolationConf, password: str, timeout_s: float = 60) -> dict[str, Any]:
    """Exécute PROBE_SCRIPT SOUS le compte agent (B) : identité réelle + refus réels de l'OS."""
    proc = LogonPopen([conf.probe_python, "-I", "-c", PROBE_SCRIPT], cwd=conf.agent_profile,
                      env=agent_env(conf, {}), user=conf.agent_user, password=password, suspended=False)
    try:
        proc.stdin.write(json.dumps({"deny_read": conf.deny_read, "deny_write": conf.deny_write}).encode())
        proc.stdin.close()
        out = proc.stdout.read(1 << 20)
        err = proc.stderr.read(1 << 16)
        code = proc.wait(timeout_s)
    finally:
        proc.close()
    if code != 0:
        raise IsolationError(f"sonde agent en échec (exit {code}) : {err[-300:].decode(errors='replace')}")
    return json.loads(out.decode("utf-8"))


def attestation(runner: dict, probe: dict, agent_process_sid: str | None) -> dict[str, Any]:
    probes = [{"probe": p["probe"], "path": p["path"], "denied": bool(p["denied"]), "why": p.get("why")}
              for p in probe.get("probes", [])]
    ok = (bool(probes) and all(p["denied"] for p in probes) and probe.get("is_admin") is False
          and probe.get("elevated") is False and runner.get("is_admin") is False and runner.get("sid") != probe.get("sid")
          and (agent_process_sid is None or agent_process_sid == probe.get("sid")))
    return {
        "ok": ok, "runner_sid": runner.get("sid"), "runner_is_admin": runner.get("is_admin"),
        "agent_sid": probe.get("sid"), "agent_is_admin": probe.get("is_admin"), "agent_elevated": probe.get("elevated"),
        "agent_integrity_rid": probe.get("integrity_rid"), "agent_process_sid": agent_process_sid, "probes": probes,
    }


def agent_env(conf: IsolationConf, extra: dict[str, str]) -> dict[str, str]:
    """Environnement minimal de B : son propre profil (auth OpenCode de B), jamais celui de A."""
    sysroot = os.environ.get("SYSTEMROOT", r"C:\Windows")
    prof = conf.agent_profile
    env = {
        "SYSTEMROOT": sysroot, "WINDIR": sysroot, "SYSTEMDRIVE": os.environ.get("SYSTEMDRIVE", "C:"),
        "COMSPEC": os.path.join(sysroot, "System32", "cmd.exe"),
        "PATH": os.pathsep.join([os.path.join(sysroot, "System32"), sysroot, rf"{prof}\AppData\Local\Microsoft\WindowsApps"]),
        "PATHEXT": ".COM;.EXE;.BAT;.CMD", "USERPROFILE": prof, "HOMEDRIVE": prof[:2], "HOMEPATH": prof[2:],
        "APPDATA": rf"{prof}\AppData\Roaming", "LOCALAPPDATA": rf"{prof}\AppData\Local",
        "TEMP": rf"{prof}\AppData\Local\Temp", "TMP": rf"{prof}\AppData\Local\Temp",
        "USERNAME": conf.agent_user, "PROGRAMDATA": os.environ.get("PROGRAMDATA", r"C:\ProgramData"),
        "PROGRAMFILES": os.environ.get("PROGRAMFILES", r"C:\Program Files"), "OS": "Windows_NT",
    }
    env.update(extra)
    return env


# ------------------------------------------------------------------ CreateProcessWithLogonW
class LogonPopen:
    """Sous-ensemble de subprocess.Popen pour un processus lancé sous un autre compte (B).

    Pipes anonymes (extrémités enfant héritables), fenêtre cachée, création éventuellement suspendue
    (rattachement au Job Object par l'appelant puis NtResumeProcess). Interface utilisée par le runner :
    stdin/stdout/stderr, pid, _handle, poll(), wait(), returncode, kill().
    """

    def __init__(self, argv: list[str], cwd: str, env: dict[str, str], user: str, password: str,
                 suspended: bool = True) -> None:
        if not IS_WINDOWS:
            raise IsolationError("CreateProcessWithLogonW : Windows uniquement")
        import msvcrt
        from ctypes import wintypes

        self._k32 = k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        adv = ctypes.WinDLL("advapi32", use_last_error=True)

        class SECURITY_ATTRIBUTES(ctypes.Structure):
            _fields_ = [("nLength", wintypes.DWORD), ("lpSecurityDescriptor", ctypes.c_void_p),
                        ("bInheritHandle", wintypes.BOOL)]

        class STARTUPINFOW(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("lpReserved", wintypes.LPWSTR), ("lpDesktop", wintypes.LPWSTR),
                        ("lpTitle", wintypes.LPWSTR), ("dwX", wintypes.DWORD), ("dwY", wintypes.DWORD),
                        ("dwXSize", wintypes.DWORD), ("dwYSize", wintypes.DWORD), ("dwXCountChars", wintypes.DWORD),
                        ("dwYCountChars", wintypes.DWORD), ("dwFillAttribute", wintypes.DWORD),
                        ("dwFlags", wintypes.DWORD), ("wShowWindow", wintypes.WORD), ("cbReserved2", wintypes.WORD),
                        ("lpReserved2", ctypes.c_void_p), ("hStdInput", wintypes.HANDLE),
                        ("hStdOutput", wintypes.HANDLE), ("hStdError", wintypes.HANDLE)]

        class PROCESS_INFORMATION(ctypes.Structure):
            _fields_ = [("hProcess", wintypes.HANDLE), ("hThread", wintypes.HANDLE),
                        ("dwProcessId", wintypes.DWORD), ("dwThreadId", wintypes.DWORD)]

        sa = SECURITY_ATTRIBUTES(ctypes.sizeof(SECURITY_ATTRIBUTES), None, True)
        pipes = []
        for child_reads in (True, False, False):  # stdin, stdout, stderr
            r, w = wintypes.HANDLE(), wintypes.HANDLE()
            if not k32.CreatePipe(ctypes.byref(r), ctypes.byref(w), ctypes.byref(sa), 0):
                raise IsolationError(f"CreatePipe ({ctypes.get_last_error()})")
            parent = w if child_reads else r
            k32.SetHandleInformation(parent, 1, 0)  # HANDLE_FLAG_INHERIT off côté parent
            pipes.append((r, w) if child_reads else (w, r))  # (extrémité enfant, extrémité parent)
        si = STARTUPINFOW()
        si.cb = ctypes.sizeof(STARTUPINFOW)
        si.dwFlags = 0x100 | 0x1  # STARTF_USESTDHANDLES | STARTF_USESHOWWINDOW
        si.wShowWindow = 0        # SW_HIDE
        si.hStdInput, si.hStdOutput, si.hStdError = pipes[0][0], pipes[1][0], pipes[2][0]
        pi = PROCESS_INFORMATION()
        flags = 0x00000400 | 0x08000000 | 0x00000200  # UNICODE_ENVIRONMENT | NO_WINDOW | NEW_PROCESS_GROUP
        if suspended:
            flags |= 0x00000004
        block = "".join(f"{k}={v}\0" for k, v in sorted(env.items())) + "\0"
        cmdline = ctypes.create_unicode_buffer(subprocess.list2cmdline(argv))
        ok = adv.CreateProcessWithLogonW(user, ".", password, 0x1, None, cmdline, flags,  # LOGON_WITH_PROFILE
                                         ctypes.create_unicode_buffer(block), cwd, ctypes.byref(si), ctypes.byref(pi))
        err = ctypes.get_last_error()
        for child_end, _ in pipes:
            k32.CloseHandle(child_end)
        if not ok:
            for _, parent_end in pipes:
                k32.CloseHandle(parent_end)
            raise IsolationError(f"CreateProcessWithLogonW a échoué ({err})")
        k32.CloseHandle(pi.hThread) if not suspended else None
        self._thread = pi.hThread if suspended else None
        self._handle = pi.hProcess
        self.pid = int(pi.dwProcessId)
        self.returncode: int | None = None
        fds = [msvcrt.open_osfhandle(parent.value, 0) for _, parent in pipes]
        self.stdin = os.fdopen(fds[0], "wb", buffering=0)
        self.stdout = os.fdopen(fds[1], "rb")
        self.stderr = os.fdopen(fds[2], "rb")

    def poll(self) -> int | None:
        if self.returncode is None and self._k32.WaitForSingleObject(self._handle, 0) == 0:
            code = ctypes.c_ulong()
            self._k32.GetExitCodeProcess(self._handle, ctypes.byref(code))
            self.returncode = int(code.value)
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        ms = 0xFFFFFFFF if timeout is None else int(timeout * 1000)
        if self._k32.WaitForSingleObject(self._handle, ms) != 0:
            self.kill()
            raise IsolationError("délai dépassé")
        return self.poll()

    def kill(self) -> None:
        self._k32.TerminateProcess(self._handle, 1)

    def close(self) -> None:
        for f in (self.stdin, self.stdout, self.stderr):
            try:
                f.close()
            except OSError:
                pass
        if self._thread:
            self._k32.CloseHandle(self._thread)
            self._thread = None


def process_sid(handle) -> str | None:
    """SID du jeton d'un processus (compte B) si A peut l'ouvrir ; None sinon (attestation via la sonde)."""
    if not IS_WINDOWS:
        return None
    from ctypes import wintypes

    adv = ctypes.WinDLL("advapi32", use_last_error=True)
    tok = wintypes.HANDLE()
    if not adv.OpenProcessToken(wintypes.HANDLE(int(handle)), 0x8, ctypes.byref(tok)):  # TOKEN_QUERY
        return None
    try:
        n = wintypes.DWORD()
        adv.GetTokenInformation(tok, 1, None, 0, ctypes.byref(n))
        buf = ctypes.create_string_buffer(n.value)
        if not adv.GetTokenInformation(tok, 1, buf, n, ctypes.byref(n)):
            return None
        s = ctypes.c_wchar_p()
        adv.ConvertSidToStringSidW(ctypes.c_void_p(ctypes.cast(buf, ctypes.POINTER(ctypes.c_void_p))[0]), ctypes.byref(s))
        return s.value
    finally:
        ctypes.WinDLL("kernel32").CloseHandle(tok)


# ------------------------------------------------------------------ enveloppe
def build_payload(*, runner_id: str, job: dict, exit_code: int | None, started_at: float, finished_at: float,
                  tree_killed: bool, att: dict, before: Scan, after: Scan) -> dict[str, Any]:
    changed = EV.diff_manifests(before.files, after.files)
    complete = before.complete and after.complete and len(changed) <= EV.MAX_CHANGED
    return {
        "v": EV.VERSION, "runner_id": runner_id, "job_id": job["job_id"], "fencing": job.get("fencing"),
        "workspace_id": job["workspace_id"], "mode": job["mode"], "exit_code": exit_code,
        "started_at": started_at, "finished_at": finished_at, "tree_killed": tree_killed,
        "runner_code_sha256": runner_code_digest(), "attestation": att,
        "manifest": {
            "complete": complete, "ads_checked": before.ads_checked and after.ads_checked,
            "alternate_streams": after.alternate_streams[:20], "reparse_points": after.reparse_points[:20],
            "before_sha256": EV.manifest_digest(before.files), "after_sha256": EV.manifest_digest(after.files),
            "files_after": len(after.files), "changed": changed[:EV.MAX_CHANGED],
        },
    }


def runner_code_digest() -> str:
    h = hashlib.sha256()
    base = Path(__file__).resolve().parent
    for p in sorted(base.glob("*.py")) + sorted((base.parent / "orch_protocol").glob("*.py")):
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()
