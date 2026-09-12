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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orch_protocol as P


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


class Adapter:
    id: str = ""
    # modes supportés par l'adapter, avant intersection avec la config du workspace
    modes: tuple[str, ...] = P.MODES

    def __init__(self, exe: str, extra: dict | None = None) -> None:
        self.exe = exe
        self.extra = extra or {}
        self.session_id: str | None = None
        self.last_text: str | None = None

    # -- découverte
    def probe(self) -> dict[str, Any]:
        info: dict[str, Any] = {"id": self.id, "available": False, "modes": list(self.modes)}
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

    def finish(self, exit_code: int, tmpdir: Path) -> Outcome:
        ok = exit_code == 0
        return Outcome(ok, self.last_text, None if ok else f"exit code {exit_code}")


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
    MODE = {"read_only": "plan", "workspace_write": "acceptEdits"}

    def __init__(self, exe, extra=None):
        super().__init__(exe, extra)
        self.result: dict | None = None

    def build(self, prompt, mode, cwd, tmpdir):
        argv = [self.exe, "-p", "--output-format", "stream-json", "--verbose", "--permission-mode", self.MODE[mode]]
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
    """`codex exec` ; prompt sur stdin (`-`) ; sandbox TOUJOURS explicite (config user = danger-full-access)."""

    id = "codex"
    MODE = {"read_only": "read-only", "workspace_write": "workspace-write"}

    def build(self, prompt, mode, cwd, tmpdir):
        last = tmpdir / "last_message.txt"
        argv = [self.exe, "exec", "--json", "-s", self.MODE[mode], "-c", 'approval_policy="never"',
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
    MODE = {"read_only": "plan", "workspace_write": "accept-edits"}

    def __init__(self, exe, extra=None):
        super().__init__(exe, extra)
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
        argv = [self.exe, f"--print={framed}", "--output-format", "json", "--mode", self.MODE[mode],
                "--add-dir", cwd, "--sandbox", "--print-timeout", timeout]
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
    """`opencode run --agent plan` (lecture seule uniquement : l'agent build autorise tout)."""

    id = "opencode"
    modes = ("read_only",)

    def build(self, prompt, mode, cwd, tmpdir):
        if mode != "read_only":
            raise ValueError("opencode : seul read_only est supporté")
        return Launch([self.exe, "run", "--agent", "plan", "--format", "json", "--dir", cwd, "--", prompt], None)

    def on_line(self, line):
        data = _json(line)
        if data is None:
            return line, None
        if data.get("type") == "error":
            err = data.get("error") if isinstance(data.get("error"), dict) else {}
            msg = str((err.get("data") or {}).get("message") or err.get("name") or "error")
            self.error = msg[:500]
            return f"[error] {msg}\n", f"error: {msg[:100]}"
        sid = data.get("sessionID") or (data.get("part") or {}).get("sessionID")
        if isinstance(sid, str):
            self.session_id = sid
        part = data.get("part") if isinstance(data.get("part"), dict) else {}
        if part.get("type") == "text" and isinstance(part.get("text"), str):
            self.last_text = part["text"]
            return part["text"] + "\n", part["text"][:120]
        return None, str(data.get("type") or "event")[:120]

    def finish(self, exit_code, tmpdir):
        error = getattr(self, "error", None)
        ok = exit_code == 0 and error is None
        return Outcome(ok, self.last_text, None if ok else (f"exit code {exit_code}" + (f", {error}" if error else "")))


class Fake(Adapter):
    """Fixture de test : `python fake_agent.py`, script piloté par le prompt (stdin)."""

    id = "fake"

    def __init__(self, exe, extra=None):
        super().__init__(exe or sys.executable, extra)

    def version_args(self):
        return ["--version"]

    def build(self, prompt, mode, cwd, tmpdir):
        script = str(Path(__file__).with_name("fake_agent.py"))
        return Launch([self.exe, "-u", script, mode], prompt, {"PYTHONIOENCODING": "utf-8"})

    def on_line(self, line):
        if line.startswith("SUMMARY:"):
            self.last_text = line[8:].strip()
        return line, line.strip()[:120] or None


ADAPTERS: dict[str, type[Adapter]] = {a.id: a for a in (ClaudeCode, Codex, Agy, OpenCode, Fake)}
