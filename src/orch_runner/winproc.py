"""Lancement de processus Windows confiné dans un Job Object.

- CreateProcess via `subprocess.Popen(list, shell=False)` : jamais cmd.exe/PowerShell ;
- exécutable obligatoirement `.exe` absolu (les shims .cmd/.bat passent par cmd.exe
  et réinterprètent les arguments : BatBadBut) ;
- création SUSPENDUE, rattachement au Job Object, puis reprise : aucun enfant ne
  peut naître hors du job ;
- JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE : si le runner meurt, tout l'arbre meurt ;
- annulation = TerminateJobObject (arbre entier).
"""

from __future__ import annotations

import ctypes
import os
import subprocess
from ctypes import wintypes

IS_WINDOWS = os.name == "nt"

CREATE_SUSPENDED = 0x00000004
CREATE_NEW_PROCESS_GROUP = 0x00000200
CREATE_NO_WINDOW = 0x08000000
JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
JobObjectBasicAccountingInformation = 1
JobObjectExtendedLimitInformation = 9
FILE_ATTRIBUTE_REPARSE_POINT = 0x400
DRIVE_FIXED = 3

if IS_WINDOWS:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    ntdll = ctypes.WinDLL("ntdll")

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [(n, ctypes.c_ulonglong) for n in (
            "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
            "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    class JOBOBJECT_BASIC_ACCOUNTING_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("TotalUserTime", ctypes.c_longlong),
            ("TotalKernelTime", ctypes.c_longlong),
            ("ThisPeriodTotalUserTime", ctypes.c_longlong),
            ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
            ("TotalPageFaultCount", wintypes.DWORD),
            ("TotalProcesses", wintypes.DWORD),
            ("ActiveProcesses", wintypes.DWORD),
            ("TotalTerminatedProcesses", wintypes.DWORD),
        ]

    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    kernel32.QueryInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p]
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.GetDriveTypeW.argtypes = [wintypes.LPCWSTR]
    kernel32.CreateMutexW.restype = wintypes.HANDLE
    kernel32.CreateMutexW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR]
    ntdll.NtResumeProcess.argtypes = [wintypes.HANDLE]
    ntdll.NtResumeProcess.restype = ctypes.c_long


class LaunchError(Exception):
    pass


class JobProcess:
    """Processus agent + son Job Object."""

    def __init__(self, argv: list[str], cwd: str, env: dict[str, str]) -> None:
        if not IS_WINDOWS:
            raise LaunchError("winproc réservé à Windows")
        exe = argv[0]
        if not os.path.isabs(exe) or not exe.lower().endswith(".exe") or not os.path.isfile(exe):
            raise LaunchError("exécutable refusé : chemin absolu vers un .exe existant requis")
        if any("\x00" in a for a in argv):
            raise LaunchError("argument contenant NUL")
        self.job = kernel32.CreateJobObjectW(None, None)
        if not self.job:
            raise LaunchError(f"CreateJobObject a échoué ({ctypes.get_last_error()})")
        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not kernel32.SetInformationJobObject(self.job, JobObjectExtendedLimitInformation, ctypes.byref(info), ctypes.sizeof(info)):
            self._close_job()
            raise LaunchError(f"SetInformationJobObject a échoué ({ctypes.get_last_error()})")
        try:
            self.proc = subprocess.Popen(
                argv,
                cwd=cwd,
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                creationflags=CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW,
            )
        except OSError as exc:
            self._close_job()
            raise LaunchError(f"CreateProcess a échoué : {exc}") from exc
        if not kernel32.AssignProcessToJobObject(self.job, int(self.proc._handle)):
            err = ctypes.get_last_error()
            self.proc.kill()
            self._close_job()
            raise LaunchError(f"AssignProcessToJobObject a échoué ({err})")
        self.resumed = False

    @property
    def pid(self) -> int:
        return self.proc.pid

    def resume(self) -> None:
        status = ntdll.NtResumeProcess(int(self.proc._handle))
        if status != 0:
            raise LaunchError(f"NtResumeProcess a échoué (0x{status & 0xFFFFFFFF:08x})")
        self.resumed = True

    def active_processes(self) -> int:
        acct = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION()
        if not self.job:
            return 0
        ok = kernel32.QueryInformationJobObject(
            self.job, JobObjectBasicAccountingInformation, ctypes.byref(acct), ctypes.sizeof(acct), None
        )
        return int(acct.ActiveProcesses) if ok else -1

    def kill_tree(self, exit_code: int = 1) -> None:
        if self.job:
            kernel32.TerminateJobObject(self.job, exit_code)

    def close(self) -> None:
        """Ferme le job : KILL_ON_JOB_CLOSE tue tout survivant éventuel."""
        self._close_job()

    def _close_job(self) -> None:
        if getattr(self, "job", None):
            kernel32.CloseHandle(self.job)
            self.job = None


def single_instance(name: str):
    """Mutex nommé de session : empêche deux runners pour le même runner_id."""
    if not IS_WINDOWS:
        return object()
    handle = kernel32.CreateMutexW(None, True, f"Local\\orch-runner-{name}")
    if not handle or ctypes.get_last_error() == 183:  # ERROR_ALREADY_EXISTS
        return None
    return handle


def is_fixed_drive(path: str) -> bool:
    if not IS_WINDOWS:
        return True
    drive = os.path.splitdrive(path)[0]
    if len(drive) != 2 or drive[1] != ":":
        return False
    return kernel32.GetDriveTypeW(drive + "\\") == DRIVE_FIXED
