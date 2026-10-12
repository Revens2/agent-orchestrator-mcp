"""Adapters de runtimes : chaque runtime encode explicitement son contrat.

Un adapter ne reçoit jamais de commande : il construit lui-même argv à partir de
(exe configuré localement, mode borné, cwd validé) et choisit le transport du
prompt. Le prompt n'est jamais interpolé dans une chaîne de commande.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import orch_protocol as P
from orch_runner import desktop_patch as DP


@dataclass
class Launch:
    argv: list[str]
    stdin_text: str | None          # None => stdin fermé immédiatement
    env_extra: dict[str, str] = field(default_factory=dict)


@dataclass
class Outcome:
    ok: bool
    summary: str | None
    error: str | None
    # Cause fournisseur classifiée (P.PROVIDER_*) ; None = non reconnue.
    failure_class: str | None = None
    retry_after_s: float | None = None


class Adapter:
    id: str = ""
    # Reprise d'une session existante (`resume_session_id`) supportée : annoncé
    # au broker (probe -> hello) ; sinon le broker envoie un handoff complet.
    supports_resume_session: bool = False
    # modes supportés par l'adapter, avant intersection avec la config du workspace
    modes: tuple[str, ...] = P.MODES

    def __init__(self, exe: str, extra: dict | None = None, policy: str = P.UNATTENDED) -> None:
        if policy not in P.PERMISSION_POLICIES:
            raise ValueError(f"politique de permissions inconnue : {policy!r}")
        self.exe = exe
        self.extra = extra or {}
        # unattended : aucune demande d'autorisation en workspace_write (mode le plus autonome du runtime).
        # guarded : écritures limitées, le reste refusé sans prompt. read_only reste read-only dans les deux.
        self.policy = policy
        self.session_id: str | None = None
        self.last_text: str | None = None
        # Session runtime à reprendre (fournie par le broker, validée par le runner).
        self.resume_session_id: str | None = None
        # Cause fournisseur bloquante vue PENDANT l'exécution (ex. quota dont le
        # runtime retente en silence) : le runner arrête alors le processus.
        self.provider_block: str | None = None

    # -- découverte
    def probe(self) -> dict[str, Any]:
        info: dict[str, Any] = {"id": self.id, "available": False, "modes": list(self.modes)}
        if self.supports_resume_session:
            info["resume_session"] = True
        if not (os.path.isabs(self.exe) and self.exe.lower().endswith(".exe") and os.path.isfile(self.exe)):
            info["reason"] = "exécutable absent ou non .exe"
            return info
        try:
            out = subprocess.run(
                [self.exe, *self.version_args()], capture_output=True, text=True, timeout=20,
                creationflags=0x08000000, encoding="utf-8", errors="replace",
            )
            info["version"] = (out.stdout or out.stderr).strip().splitlines()[0][:80] if (out.stdout or out.stderr) else ""
            info["available"] = out.returncode == 0
        except Exception as exc:  # noqa: BLE001
            info["reason"] = type(exc).__name__
        return info

    def version_args(self) -> list[str]:
        return ["--version"]

    # -- exécution
    def build(self, prompt: str, mode: str, cwd: str, tmpdir: Path) -> Launch:
        raise NotImplementedError

    def on_line(self, line: str) -> tuple[str | None, str | None]:
        """(texte lisible à remonter, activité courte)."""
        return line, None

    def on_stderr(self, line: str) -> None:
        """Observation d'une ligne stderr (aucune par défaut)."""

    def finish(self, exit_code: int, tmpdir: Path) -> Outcome:
        ok = exit_code == 0
        return Outcome(ok, self.last_text, None if ok else f"exit code {exit_code}")


def _retry_after_s(headers: object, message: object = None) -> float | None:
    """Délai fournisseur : `retry-after-ms`, `retry-after` (secondes ou date
    HTTP), sinon « It will reset in … » (message opencode Zen Go). None si absent."""
    if isinstance(headers, dict):
        low = {str(k).lower(): v for k, v in headers.items()}
        try:
            if low.get("retry-after-ms") is not None:
                return P.finite_delay(float(low["retry-after-ms"]) / 1000.0)
        except (TypeError, ValueError):
            pass
        raw = low.get("retry-after")
        if raw is not None:
            try:
                return P.finite_delay(float(raw))
            except (TypeError, ValueError):
                try:
                    return P.finite_delay(parsedate_to_datetime(str(raw)).timestamp() - time.time())
                except (TypeError, ValueError, IndexError, OverflowError):
                    pass
    return P.parse_reset_in(message)


def _json(line: str) -> dict | None:
    line = line.strip()
    if not line.startswith("{"):
        return None
    try:
        data = json.loads(line)
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


class ClaudeCode(Adapter):
    """`claude -p` ; prompt sur stdin ; stream-json (NDJSON) ; fin = message `result`."""

    id = "claude-code"
    # --permission-prompts none : tout ce qui demanderait une autorisation est refusé, jamais d'attente.
    MODE = {
        P.UNATTENDED: {"read_only": ["--permission-mode", "plan", "--permission-prompts", "none"],
                       "workspace_write": ["--permission-mode", "bypassPermissions"]},
        P.GUARDED: {"read_only": ["--permission-mode", "plan", "--permission-prompts", "none"],
                    "workspace_write": ["--permission-mode", "acceptEdits", "--permission-prompts", "none"]},
    }

    def __init__(self, exe, extra=None, policy=P.UNATTENDED):
        super().__init__(exe, extra, policy)
        self.result: dict | None = None

    def build(self, prompt, mode, cwd, tmpdir):
        argv = [self.exe, "-p", "--output-format", "stream-json", "--verbose", *self.MODE[self.policy][mode]]
        if self.extra.get("max_budget_usd"):
            argv += ["--max-budget-usd", str(float(self.extra["max_budget_usd"]))]
        return Launch(argv, prompt)

    def on_line(self, line):
        data = _json(line)
        if data is None:
            return line, None
        self.session_id = data.get("session_id") or self.session_id
        kind = data.get("type")
        if kind == "assistant":
            texts, activity = [], None
            for part in (data.get("message") or {}).get("content") or []:
                if part.get("type") == "text" and part.get("text"):
                    texts.append(part["text"])
                    self.last_text = part["text"]
                elif part.get("type") == "tool_use":
                    activity = f"tool: {part.get('name')}"
                    texts.append(f"[tool_use {part.get('name')}]")
            return ("\n".join(texts) + "\n") if texts else None, activity or (texts[-1][:120] if texts else None)
        if kind == "result":
            self.result = data
            return None, f"result: {data.get('subtype')}"
        if kind == "system" and data.get("subtype") == "init":
            return None, "session started"
        return None, None

    def finish(self, exit_code, tmpdir):
        r = self.result or {}
        text = r.get("result") if isinstance(r.get("result"), str) else self.last_text
        if exit_code == 0 and r and not r.get("is_error"):
            return Outcome(True, text, None)
        if not r:
            return Outcome(False, text, f"exit code {exit_code}, aucun message result (fin non confirmée)")
        return Outcome(False, text, f"exit code {exit_code}, result {r.get('subtype')} is_error={r.get('is_error')}")


class Codex(Adapter):
    """`codex exec` ; prompt sur stdin (`-`) ; sandbox + approbation TOUJOURS explicites (jamais la config user)."""

    id = "codex"
    MODE = {
        P.UNATTENDED: {"read_only": "read-only", "workspace_write": "danger-full-access"},
        P.GUARDED: {"read_only": "read-only", "workspace_write": "workspace-write"},
    }

    def build(self, prompt, mode, cwd, tmpdir):
        last = tmpdir / "last_message.txt"
        argv = [self.exe, "exec", "--json", "-s", self.MODE[self.policy][mode], "-c", 'approval_policy="never"',
                "-C", cwd, "--skip-git-repo-check", "-o", str(last), "-"]
        return Launch(argv, prompt)

    def on_line(self, line):
        data = _json(line)
        if data is None:
            return line, None
        for key in ("thread_id", "session_id", "conversation_id"):
            if isinstance(data.get(key), str):
                self.session_id = data[key]
        kind = str(data.get("type") or data.get("method") or "event")
        item = data.get("item") if isinstance(data.get("item"), dict) else {}
        text = item.get("text") if isinstance(item.get("text"), str) else None
        if text:
            self.last_text = text
            return text + "\n", text[:120]
        return None, kind[:120]

    def finish(self, exit_code, tmpdir):
        last = tmpdir / "last_message.txt"
        summary = last.read_text(encoding="utf-8", errors="replace") if last.exists() else self.last_text
        ok = exit_code == 0
        return Outcome(ok, summary, None if ok else f"exit code {exit_code}")


class Agy(Adapter):
    """Antigravity CLI `agy --print=<prompt>` ; JSON final {status, response, conversation_id}."""

    id = "agy"
    MODE = {
        P.UNATTENDED: {"read_only": ["--mode", "plan", "--sandbox"],
                       "workspace_write": ["--mode", "accept-edits", "--dangerously-skip-permissions"]},
        P.GUARDED: {"read_only": ["--mode", "plan", "--sandbox"],
                    "workspace_write": ["--mode", "accept-edits", "--sandbox"]},
    }

    def __init__(self, exe, extra=None, policy=P.UNATTENDED):
        super().__init__(exe, extra, policy)
        self.buffer: list[str] = []

    def build(self, prompt, mode, cwd, tmpdir):
        timeout = str(int(self.extra.get("print_timeout_s", 3600))) + "s"
        # agy ignore le cwd du processus (workspace principal = son propre dossier) :
        # le workspace est ajouté par --add-dir et désigné explicitement dans le prompt.
        framed = (
            f"[orchestrator] Workspace for this task: {cwd}\n"
            "Treat that directory as the current directory; read and write files only inside it.\n\n"
            + prompt
        )
        # forme --flag=valeur : un prompt commençant par '-' ne peut pas être lu comme option
        argv = [self.exe, f"--print={framed}", "--output-format", "json", *self.MODE[self.policy][mode],
                "--add-dir", cwd, "--print-timeout", timeout]
        return Launch(argv, None)

    def on_line(self, line):
        self.buffer.append(line)
        return line, "running"

    def finish(self, exit_code, tmpdir):
        data = None
        blob = "".join(self.buffer).strip()
        start = blob.find("{")
        if start >= 0:
            try:
                data = json.loads(blob[start:])
            except ValueError:
                data = None
        if isinstance(data, dict):
            self.session_id = data.get("conversation_id") or self.session_id
            ok = exit_code == 0 and data.get("status") == "SUCCESS"
            return Outcome(ok, data.get("response"), None if ok else f"exit code {exit_code}, status {data.get('status')}")
        return Outcome(False, blob[-2000:] or None, f"exit code {exit_code}, sortie JSON illisible")


class OpenCode(Adapter):
    """`opencode run` ; read_only = agent plan ; workspace_write (unattended) = agent build --auto.

    Modèle épinglé par `model` (runner.toml) : le runner ne dépend pas du défaut global d'opencode.
    """

    id = "opencode"
    supports_resume_session = True
    MODE = {
        P.UNATTENDED: {"read_only": ["--agent", "plan"], "workspace_write": ["--agent", "build", "--auto"]},
        P.GUARDED: {"read_only": ["--agent", "plan"]},  # sans --auto, un prompt bloquerait le headless
    }

    def __init__(self, exe, extra=None, policy=P.UNATTENDED):
        super().__init__(exe, extra, policy)
        self._tail: list[str] = []  # dernières lignes vues (détection corruption)
        self._prompt_title = "session opencode"
        self._qbuf = ""  # accumulation bornée pour le bloc [[QUESTION]]
        self.pending_question: tuple[str, list[str]] | None = None
        self.error: str | None = None
        self.provider_class: str | None = None
        self.retry_after: float | None = None

    @property
    def modes(self):  # type: ignore[override]
        return tuple(self.MODE[self.policy])

    def _model(self) -> list[str]:
        model = str(self.extra.get("model", ""))
        return ["-m", model] if model else []

    def build(self, prompt, mode, cwd, tmpdir):
        if mode not in self.MODE[self.policy]:
            raise ValueError(f"opencode : mode {mode} non supporté en politique {self.policy}")
        # --title natif (tronque le prompt sinon) : titre stable dérivé du
        # premier objectif réel, jamais renommé ensuite.
        title = P.display_title(prompt, fallback="session opencode")
        self._prompt_title = title
        # Reprise d'une session existante (mission en pause-quota) : l'ID est
        # validé par le runner avant d'arriver ici (jamais un flag injecté).
        session = ["--session", self.resume_session_id] if self.resume_session_id else []
        # quota_watch_logs (opt-in runner.toml) : les erreurs fournisseur
        # partent sur stderr pendant que opencode retente en silence (retry-after
        # de plusieurs heures) ; le runner peut alors suspendre au lieu d'attendre
        # le timeout. Coût : opencode.log limité au niveau ERROR pour ce run.
        logs = ["--print-logs", "--log-level", "ERROR"] if self.extra.get("quota_watch_logs") else []
        return Launch([self.exe, "run", *self.MODE[self.policy][mode], *self._model(), *session, *logs,
                       "--format", "json", "--title", title, "--dir", cwd, "--", prompt], None)

    def probe(self):
        """--version ne prouve pas qu'un modèle répond : génération minimale (au démarrage du runner seulement)."""
        info = super().probe()
        if not info["available"] or not self.extra.get("probe_generation", True):
            return info
        errors: list = []
        try:
            # NOTE 2026-10-08 : le verdict DOIT etre calcule avant la sortie du
            # TemporaryDirectory. Sur Windows le rmtree echoue souvent (handle
            # garde par l'enfant / indexation) et l'except ci-dessous transformait
            # une generation REUSSIE en "generation de test echouee".
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
                out = subprocess.run(
                    [self.exe, "run", "--agent", "plan", *self._model(), "--format", "json", "--dir", tmp, "--", "Reply OK."],
                    capture_output=True, text=True, timeout=int(self.extra.get("probe_timeout_s", 120)),
                    creationflags=0x08000000, encoding="utf-8", errors="replace", stdin=subprocess.DEVNULL,
                )
                events = [d for d in map(_json, (out.stdout or "").splitlines()) if d]
                errors = [d for d in events if d.get("type") == "error"]
                ok = out.returncode == 0 and not errors and any((d.get("part") or {}).get("type") == "text" for d in events)
        except Exception:  # noqa: BLE001
            ok = False
        if not ok:
            info["available"] = False
            info["reason"] = "runtime_not_ready: génération de test échouée" + (" (erreur fournisseur)" if errors else "")
        return info

    def on_line(self, line):
        # Convention explicite waiting_for_user (jamais devinée sur `?`) :
        # l'agent émet [[QUESTION]]...[[/QUESTION]] (+ [[OPTIONS]]...).
        if self.pending_question is None and (
            P.QUESTION_OPEN_TAG in line
            or (self._qbuf and (P.QUESTION_CLOSE_TAG in line or P.QUESTION_OPTIONS_CLOSE in line))
        ):
            self._qbuf = (self._qbuf + line)[-4000:]
            parsed = P.parse_question_block(self._qbuf)
            if parsed is not None:
                self.pending_question = parsed
                self._qbuf = ""
        data = _json(line)
        if data is None:
            self._tail.append(line[-500:])
            del self._tail[:-30]
            return line, None
        if data.get("type") == "error":
            err = data.get("error") if isinstance(data.get("error"), dict) else {}
            edata = err.get("data") if isinstance(err.get("data"), dict) else {}
            msg = str(edata.get("message") or err.get("name") or "error")
            self.error = msg[:500]
            # Classification typée (APIError opencode) : nom + message + corps
            # fournisseur ; statusCode pris en compte seulement pour APIError.
            name = str(err.get("name") or "")
            body = edata.get("responseBody") if isinstance(edata.get("responseBody"), str) else ""
            status = edata.get("statusCode") if name == "APIError" else None
            cls = P.classify_provider_error(f"{name}\n{msg}\n{body[:4000]}", status)
            if cls is not None:
                self.provider_class = cls
                self.retry_after = _retry_after_s(edata.get("responseHeaders"), msg)
            self._tail.append(msg[:500])
            del self._tail[:-30]
            return f"[error] {msg}\n", f"error: {msg[:100]}"
        sid = data.get("sessionID") or (data.get("part") or {}).get("sessionID")
        if isinstance(sid, str):
            self.session_id = sid
        part = data.get("part") if isinstance(data.get("part"), dict) else {}
        if part.get("type") == "text" and isinstance(part.get("text"), str):
            self.last_text = part["text"]
            return part["text"] + "\n", part["text"][:120]
        return None, str(data.get("type") or "event")[:120]

    def on_stderr(self, line):
        # Seules les lignes ERROR de --print-logs (quota_watch_logs) sont lues ;
        # seul un QUOTA exact déclenche l'arrêt : un rate limit court est laissé
        # au retry natif d'opencode.
        if not self.extra.get("quota_watch_logs") or "level=ERROR" not in line:
            return
        # Modèle secondaire (titre, résumé : `small=true` / `agent=title`) :
        # observé en réel (2026-10-09) en « Insufficient account funds » pendant
        # un run principal SAIN. Seul le modèle de l'agent principal compte.
        if "small=true" in line or "agent=title" in line:
            return
        if P.classify_provider_error(line) == P.PROVIDER_QUOTA and self.provider_block is None:
            self.provider_block = P.PROVIDER_QUOTA
            self.provider_class = P.PROVIDER_QUOTA
            self.retry_after = P.parse_reset_in(line)
            self.error = self.error or line.strip()[-500:]

    def finish(self, exit_code, tmpdir):
        error = self.error
        if exit_code != 0:
            # Session reconnue corrompue : signature EXACTE uniquement (jamais
            # un simple mot "error"). La session est abandonnée ; le retry
            # (mission_retry) crée une NOUVELLE session + handoff, sans transcript.
            sign = P.match_corruption("\n".join(self._tail)) or P.match_corruption(error)
            if sign is not None:
                handoff = (
                    f"session abandonnée ({sign}) ; reprendre via mission_retry "
                    f"(nouvelle session) avec l'objectif « {self._prompt_title} »"
                )
                return Outcome(False, handoff, f"{P.SESSION_CORRUPTED_PREFIX}{sign}")
        ok = exit_code == 0 and error is None and self.provider_block is None
        if ok:
            return Outcome(True, self.last_text, None)
        detail = f"exit code {exit_code}" + (f", {error}" if error else "")
        cls = self.provider_class
        if cls is not None:
            return Outcome(False, self.last_text, P.format_provider_error(cls, detail, self.retry_after),
                           failure_class=cls, retry_after_s=self.retry_after)
        return Outcome(False, self.last_text, detail)


class ClaudeDesktop(Adapter):
    """Claude Desktop (application MSIX) pilotée via UIA par `claude_desktop_bridge.py`.

    - `exe` = interpréteur Python (le bridge est un module, comme `fake`) ;
    - prompt transporté par FICHIER (`--prompt-file`, jamais interpolé en argv) ;
    - `read_only` : réponse texte seule (aucune écriture) ;
    - `workspace_write` : VOIE PATCH CONFINÉE — le Desktop ne touche jamais au
      disque. Il propose des diffs unifiés en blocs ```diff, et le RUNNER les
      applique bornés au workspace (`desktop_patch.py` : chemins relatifs
      seuls, `.git`/symlinks/`..`/absolus refusés, contexte exact exigé,
      écriture atomique, tout-échec = rien appliqué). Le Desktop ne peut ni
      exécuter de commande ni lire les fichiers : le prompt doit contenir le
      contexte nécessaire (contenu des fichiers visés, fourni par l'appelant
      après une passe `read_only`, ou passé explicitement).
    - le bridge vérifie le package MSIX, le processus Desktop (CLI exclue), la
      fenêtre visible et le profil attendu, sérialise l'UI (verrou fichier) et
      écrit `last_message.txt` (même convention que `codex`).
    """

    id = "claude-desktop"
    modes: tuple[str, ...] = ("read_only", "workspace_write")

    # Cadrage patch : transmis AU Desktop dans le prompt (le Desktop n'a aucun
    # accès disque : il propose, le runner dispose).
    PATCH_HEADER = (
        "[orchestrator] Tu modifies des fichiers du workspace : {cwd}\n"
        "Tu n'as AUCUN accès au disque : propose UNIQUEMENT des patchs unified diff, "
        "un bloc ```diff par fichier, chemins RELATIFS à la racine du workspace "
        "(jamais d'absolu, jamais de `..`, jamais `.git/`) :\n"
        "```diff\n--- a/chemin/relatif\n+++ b/chemin/relatif\n@@ -a,b +c,d @@\n"
        " contexte\n-ligne supprimée\n+ligne ajoutée\n```\n"
        "Fichier à créer : `--- /dev/null`. Fichier à supprimer : `+++ /dev/null`. "
        "Le contexte doit recopier EXACTEMENT le contenu actuel fourni ci-dessous. "
        "Aucune commande shell, aucun accès hors workspace. Explication courte hors blocs autorisée.\n\n"
    )

    def __init__(self, exe, extra=None, policy=P.UNATTENDED):
        super().__init__(exe or sys.executable, extra, policy)
        self._bridge = str(Path(__file__).with_name("claude_desktop_bridge.py"))
        self._workspace_cwd: str | None = None
        self._patch_mode = False

    def _profile(self) -> str:
        return str(self.extra.get("profile") or os.environ.get("ORCH_CLAUDE_DESKTOP_PROFILE", "Caroline"))

    def probe(self) -> dict[str, Any]:
        info: dict[str, Any] = {"id": self.id, "available": False, "modes": list(self.modes)}
        if not os.path.isfile(self._bridge):
            info["reason"] = "bridge introuvable"
            return info
        if not (os.path.isabs(self.exe) and self.exe.lower().endswith(".exe") and os.path.isfile(self.exe)):
            info["reason"] = "exécutable absent ou non .exe"
            return info
        try:
            out = subprocess.run(
                [self.exe, self._bridge, "--verify-only", "--profile", self._profile()],
                capture_output=True, text=True, timeout=int(self.extra.get("verify_timeout_s", 90)),
                creationflags=0x08000000, encoding="utf-8", errors="replace",
                stdin=subprocess.DEVNULL,
            )
            tail = ((out.stdout or "") + (out.stderr or "")).strip().splitlines()
            info["version"] = tail[-1][:120] if tail else ""
            info["available"] = out.returncode == 0
            if out.returncode != 0:
                info["reason"] = (tail[-1] if tail else "verify_failed")[:200]
        except Exception as exc:  # noqa: BLE001
            info["reason"] = type(exc).__name__
        return info

    def build(self, prompt, mode, cwd, tmpdir):
        if mode not in self.modes:
            raise ValueError(f"claude-desktop : mode {mode} non supporté (attendus : {', '.join(self.modes)})")
        self._workspace_cwd = cwd
        self._patch_mode = mode == "workspace_write"
        framed = self.PATCH_HEADER.format(cwd=cwd) + prompt if self._patch_mode else prompt
        prompt_file = Path(tmpdir) / "prompt.txt"
        prompt_file.write_text(framed, encoding="utf-8")
        argv = [self.exe, self._bridge, "--prompt-file", str(prompt_file), "--out-dir", str(tmpdir), "--profile", self._profile()]
        if self.extra.get("timeout_s"):
            argv += ["--timeout-s", str(int(self.extra["timeout_s"]))]
        return Launch(argv, None)

    def on_line(self, line):
        data = _json(line)
        if data is None:
            if line.startswith("SUMMARY:"):
                self.last_text = line[8:].strip()
            return line, None
        kind = data.get("type")
        text = data.get("text") if isinstance(data.get("text"), str) else ""
        if kind == "result":
            self.last_text = text
            return text + "\n", text[:120] or "result"
        if kind == "activity":
            return None, text[:120] or "activity"
        if kind == "error":
            return f"[error] {text}\n", f"error: {text[:100]}"
        return None, kind[:120] if isinstance(kind, str) else None

    def finish(self, exit_code, tmpdir):
        last = Path(tmpdir) / "last_message.txt"
        response = last.read_text(encoding="utf-8", errors="replace").strip() if last.exists() else (self.last_text or "")
        response = (response or "")[:DP.MAX_PATCH_CHARS] or None
        summary = (response or "")[:8000] or None
        if not self._patch_mode:
            ok = exit_code == 0 and bool(summary)
            if ok:
                return Outcome(True, summary, None)
            err = f"exit code {exit_code}" if exit_code else "réponse vide"
            if summary:
                err += " (sortie partielle conservée)"
            return Outcome(False, summary, err)
        # Voie patch confinée : le bridge a échoué => rien à appliquer.
        if exit_code != 0:
            err = f"exit code {exit_code} (bridge : patchs non appliqués)"
            if summary:
                err += " (sortie partielle conservée)"
            return Outcome(False, summary, err)
        if not response:
            return Outcome(False, None, "réponse vide (aucun patch à appliquer)")
        if self._workspace_cwd is None:
            return Outcome(False, summary, "workspace inconnu (patchs non appliqués)")
        try:
            report, detail = DP.response_to_workspace(self._workspace_cwd, response)
        except DP.PatchError as exc:
            return Outcome(False, summary, f"{exc.code}: {exc.message}")
        applied = f"patch appliqué ({detail})"
        body = f"{applied}\n---\n{summary}"[:8000]
        return Outcome(True, body, None)


class Fake(Adapter):
    """Fixture de test : `python fake_agent.py`, script piloté par le prompt (stdin)."""

    id = "fake"
    supports_resume_session = True

    def __init__(self, exe, extra=None, policy=P.UNATTENDED):
        super().__init__(exe or sys.executable, extra, policy)

    def version_args(self):
        return ["--version"]

    def build(self, prompt, mode, cwd, tmpdir):
        script = str(Path(__file__).with_name("fake_agent.py"))
        env = {"PYTHONIOENCODING": "utf-8"}
        if self.resume_session_id:
            env["ORCH_FAKE_RESUME_SESSION"] = self.resume_session_id
        return Launch([self.exe, "-u", script, mode], prompt, env)

    def on_line(self, line):
        if line.startswith("SUMMARY:"):
            self.last_text = line[8:].strip()
        elif line.startswith("SESSION:"):
            self.session_id = line[8:].strip() or self.session_id
        elif line.startswith("PROVIDER_ERROR:"):
            # `PROVIDER_ERROR:<message exact fournisseur>|<retry_after_s>`
            msg, _, retry = line[15:].strip().partition("|")
            self._provider = (P.classify_provider_error(msg), float(retry) if retry else None, msg)
        return line, line.strip()[:120] or None

    def on_stderr(self, line):
        if line.startswith("PROVIDER_BLOCK:") and P.classify_provider_error(line) == P.PROVIDER_QUOTA:
            self.provider_block = P.PROVIDER_QUOTA
            self._provider = (P.PROVIDER_QUOTA, None, line[15:].strip())

    def finish(self, exit_code, tmpdir):
        cls, retry, msg = getattr(self, "_provider", (None, None, None))
        if cls is not None and (exit_code != 0 or self.provider_block):
            return Outcome(False, self.last_text, P.format_provider_error(cls, f"exit code {exit_code}, {msg}", retry),
                           failure_class=cls, retry_after_s=retry)
        return super().finish(exit_code, tmpdir)


ADAPTERS: dict[str, type[Adapter]] = {a.id: a for a in (ClaudeCode, Codex, Agy, OpenCode, Fake, ClaudeDesktop)}
