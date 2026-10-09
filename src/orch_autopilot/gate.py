"""Gate Opus (décisions structurantes) et notification hors bande des codes de consentement.

Le gate est CONSULTATIF : une réponse Opus ne crée jamais de consentement. Une décision
n'est « attestée » que si l'identité du modèle servi est vérifiable et égale à celle attendue
en configuration ; sinon `PENDING_GATE` (repli : approbation humaine avec code hors bande).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import httpx

from orch_autopilot.broker import HEADERS, BrokerUnavailable


@dataclass(frozen=True)
class GateResult:
    answer: str | None
    session_id: str | None
    served_model: str | None
    error: str | None = None


class Gate(Protocol):
    def ask(self, prompt: str) -> GateResult: ...


class OpusMCPGate:
    """Client du MCP Opus réel (`opus_think` + `opus_health`). URL et jeton viennent de la config."""

    def __init__(self, url: str, token: str | None = None, client: httpx.Client | None = None, timeout_s: float = 900.0) -> None:
        self.url = url
        headers = dict(HEADERS)
        if token:
            headers["authorization"] = f"Bearer {token}"
        self._headers = headers
        self._client = client or httpx.Client(timeout=timeout_s)
        self._n = 0

    def _call(self, name: str, args: dict) -> dict:
        self._n += 1
        body = {"jsonrpc": "2.0", "id": self._n, "method": "tools/call", "params": {"name": name, "arguments": args}}
        try:
            r = self._client.post(self.url, json=body, headers=self._headers)
        except httpx.HTTPError as exc:
            raise BrokerUnavailable(f"opus transport: {type(exc).__name__}") from exc
        if r.status_code != 200:
            raise BrokerUnavailable(f"opus http {r.status_code}")
        text = r.text
        if "data:" in text[:200]:
            text = "\n".join(line[5:].strip() for line in text.splitlines() if line.startswith("data:"))
        res = json.loads(text).get("result", {})
        data = res.get("structuredContent")
        if data is None:
            data = {"result": (res.get("content") or [{"text": ""}])[0].get("text", "")}
        raw = data.get("result", data) if isinstance(data, dict) else data
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except ValueError:
                raw = {"answer": raw}
        return raw if isinstance(raw, dict) else {}

    def ask(self, prompt: str) -> GateResult:
        try:
            think = self._call("opus_think", {"prompt": prompt})
            health = self._call("opus_health", {})
        except (BrokerUnavailable, ValueError, AttributeError, TypeError, KeyError, IndexError) as exc:
            # toute réponse inexploitable = gate indisponible (PENDING_GATE), jamais une exception de tick
            return GateResult(None, None, None, error=f"{type(exc).__name__}: {str(exc)[:280]}")
        served = ((health.get("opus") or {}).get("served_model_last")) if isinstance(health, dict) else None
        return GateResult(think.get("answer"), think.get("session_id"), served, None if think.get("answer") else "no_answer")


class UnavailableGate:
    """Gate non configuré : toute décision structurante reste PENDING_GATE."""

    def ask(self, prompt: str) -> GateResult:
        return GateResult(None, None, None, error="gate_not_configured")


def is_attested(result: GateResult, expected_served_model: str | None) -> bool:
    return bool(result.answer) and expected_served_model is not None and result.served_model == expected_served_model


VERDICT_RE = re.compile(r"^[\W_]*(NO[- ]GO|GO)(?![\w-])")


def verdict(answer: str | None) -> str | None:
    """`GO` / `NO-GO` si la PREMIÈRE ligne non vide COMMENCE par l'un des deux ; sinon None (réponse invalide).

    Un « GO » noyé plus loin dans le texte ne compte pas.
    """
    for line in (answer or "").splitlines():
        if line.strip():
            m = VERDICT_RE.match(line.strip().upper())
            return m.group(1).replace(" ", "-") if m else None
    return None


# ------------------------------------------------------------------ notification
class Notifier(Protocol):
    def send(self, subject: str, body: str) -> None: ...


class FileNotifier:
    """Écrit le message dans un fichier 0600 lisible par l'opérateur seulement (canal hors bande minimal)."""

    def __init__(self, directory: str | Path) -> None:
        self.dir = Path(directory)

    def send(self, subject: str, body: str) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        path = self.dir / f"{subject}.txt"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(body)


class CommandNotifier:
    """Transmet le message sur stdin d'une commande configurée (ex. script d'alerte Telegram existant)."""

    def __init__(self, argv: list[str]) -> None:
        self.argv = argv

    def send(self, subject: str, body: str) -> None:
        subprocess.run(self.argv, input=f"{subject}\n{body}", text=True, timeout=30, check=False)
