"""Lancement de processus POSIX confiné dans un groupe de processus (setsid).

Équivalent Linux de `winproc.py` (Job Object Windows). Différences assumées,
documentées (pas de parité dure) :

- CreateProcess via `subprocess.Popen(list, shell=False)` : jamais de shell ;
- exécutable = chemin absolu vers un fichier régulier exécutable (bit X).
  Pas d'exigence `.exe` (inexistante sous Linux) ; les scripts avec shebang
  sont acceptés s'ils sont exécutables ;
- `start_new_session=True` (setsid) : le processus devient chef d'un nouveau
  groupe ; `kill_tree` = SIGKILL au groupe entier (`os.killpg`).
  Limite connue vs Job Object : un descendant qui appelle lui-même setsid /
  double-fork quitte le groupe et échappe au kill (pas de garantie noyau
  comme KILL_ON_JOB_CLOSE). Voir `docs/LINUX_ARM64_RUNNER.md` et, côté
  politique, `linux_allow_workspace_write` (fail-closed par défaut) ;
- pas de création SUSPENDUE : Linux ne sait pas créer un processus suspendu
  atomiquement. `resume()` envoie SIGCONT après un SIGSTOP immédiat post-spawn
  (best-effort : quelques instructions peuvent s'exécuter avant le STOP,
  contrairement à CREATE_SUSPENDED). Le runner garde la même séquence
  (spawn -> vérification cancel -> resume) mais sans la garantie dure ;
- `active_processes()` = comptage des processus du groupe via /proc
  (Linux uniquement), 1/0 en repli ;
- `single_instance()` = verrou fichier flock (POSIX), pas de mutex noyau ;
- `is_safe_path()` = garde-fous Linux (refus /proc, /sys, /dev, ...).
"""

from __future__ import annotations

import os
import signal
import subprocess
from pathlib import Path

try:
    import fcntl
except ImportError:  # Windows : pas de flock ; single_instance() dégrade en no-op.
    fcntl = None  # type: ignore[assignment]

IS_POSIX = os.name == "posix"

# Isolation annoncée au broker (champ `isolation` de Runner.info).
ISOLATION = "process-group-best-effort"

# Préfixes refusés pour un workspace ou un exécutable (système, pseudo-FS).
UNSAFE_PREFIXES = ("/proc", "/sys", "/dev", "/run", "/boot", "/etc", "/root")


class LaunchError(Exception):
    pass


def _refuse_unsafe_prefix(path: str) -> str | None:
    norm = os.path.normpath(path).replace("\\", "/")
    if norm == "/":
        return "racine refusée"
    for prefix in UNSAFE_PREFIXES:
        if norm == prefix or norm.startswith(prefix + "/"):
            return f"préfixe système refusé ({prefix})"
    return None


def check_executable(exe: str) -> None:
    """Valide un exécutable POSIX, ou lève LaunchError (fail-closed)."""
    if not isinstance(exe, str) or not exe:
        raise LaunchError("exécutable vide refusé")
    if "\x00" in exe:
        raise LaunchError("exécutable contenant NUL")
    if not os.path.isabs(exe):
        raise LaunchError("exécutable refusé : chemin absolu requis")
    reason = _refuse_unsafe_prefix(exe)
    if reason:
        raise LaunchError(f"exécutable refusé : {reason}")
    try:
        st = os.stat(exe)
    except OSError:
        raise LaunchError("exécutable refusé : fichier inexistant") from None
    import stat as _stat

    if not _stat.S_ISREG(st.st_mode):
        raise LaunchError("exécutable refusé : fichier régulier requis")
    if not os.access(exe, os.X_OK):
        raise LaunchError("exécutable refusé : bit exécutable absent")


class JobProcess:
    """Processus agent + son groupe de processus (même interface que winproc)."""

    def __init__(self, argv: list[str], cwd: str, env: dict[str, str]) -> None:
        if os.name != "posix":
            raise LaunchError("posproc réservé à POSIX")
        if not argv:
            raise LaunchError("argv vide refusé")
        check_executable(argv[0])
        if any("\x00" in a for a in argv):
            raise LaunchError("argument contenant NUL")
        if not (isinstance(cwd, str) and os.path.isdir(cwd)):
            raise LaunchError("cwd invalide : dossier requis")
        try:
            self.proc = subprocess.Popen(
                argv,
                cwd=cwd,
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                start_new_session=True,
            )
        except OSError as exc:
            raise LaunchError(f"spawn a échoué : {exc}") from exc
        # Gel best-effort (pas d'équivalent CREATE_SUSPENDED) : le processus
        # peut exécuter quelques instructions avant ce STOP (voir docstring).
        self.resumed = False
        try:
            os.kill(self.proc.pid, signal.SIGSTOP)
            self._stopped = True
        except OSError:
            self._stopped = False

    @property
    def pid(self) -> int:
        return self.proc.pid

    @property
    def pgid(self) -> int:
        try:
            return os.getpgid(self.proc.pid)
        except OSError:
            return self.proc.pid

    def resume(self) -> None:
        if self.resumed:
            return
        if getattr(self, "_stopped", False):
            try:
                os.kill(self.proc.pid, signal.SIGCONT)
            except OSError as exc:
                raise LaunchError(f"SIGCONT a échoué : {exc}") from exc
        self.resumed = True

    def active_processes(self) -> int:
        """Nombre de processus vivants du groupe (via /proc), -1 si illisible."""
        if self.proc.poll() is not None:
            # Le chef est mort : des orphelins du groupe peuvent survivre
            # (limite documentée) ; on compte quand même le groupe.
            pass
        pgid = self.pgid
        try:
            entries = os.listdir("/proc")
        except OSError:
            return 1 if self.proc.poll() is None else 0
        count = 0
        for entry in entries:
            if not entry.isdigit():
                continue
            try:
                if os.getpgid(int(entry)) == pgid:
                    count += 1
            except (OSError, ValueError):
                continue
        if count:
            return count
        # /proc illisible ou groupe vide : repli sur l'état du chef.
        return 1 if self.proc.poll() is None else 0

    def kill_tree(self, exit_code: int = 1) -> None:
        _ = exit_code  # SIGKILL ne porte pas de code ; conservé pour l'interface.
        try:
            os.killpg(self.pgid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass
        # Le chef aussi, au cas où il aurait quitté le groupe.
        try:
            self.proc.kill()
        except OSError:
            pass

    def close(self) -> None:
        """Tue tout survivant du groupe (pas de garantie noyau à la mort du
        runner comme KILL_ON_JOB_CLOSE : le runner nettoie explicitement)."""
        self.kill_tree()
        try:
            self.proc.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            pass
        for stream in (self.proc.stdin, self.proc.stdout, self.proc.stderr):
            try:
                if stream is not None:
                    stream.close()
            except OSError:
                pass


def single_instance(home: str | Path, name: str):
    """Verrou fichier flock : empêche deux runners pour le même runner_id.

    Retourne le descripteur verrouillé (à conserver vivant), ou None si déjà
    verrouillé. `home` doit exister (créé par l'appelant si besoin).
    """
    if os.name != "posix" or fcntl is None:
        return object()
    lock_path = Path(home) / f"runner-{name}.lock"
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    except OSError:
        return None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    return fd


def is_safe_path(path: str) -> bool:
    """Garde-fou rapide (vrai/faux) pour un chemin POSIX candidat."""
    if not isinstance(path, str) or not path or "\x00" in path:
        return False
    if not os.path.isabs(path):
        return False
    return _refuse_unsafe_prefix(os.path.normpath(path)) is None
