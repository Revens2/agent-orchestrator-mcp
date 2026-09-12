"""Masquage best-effort des secrets dans les textes remontés (sortie, erreurs, logs)."""

from __future__ import annotations

import re

_PATTERNS = [
    re.compile(r"(?i)\b(authorization|cookie|set-cookie)\s*[:=]\s*[^\r\n]+"),
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{16,}"),
    re.compile(r"\bsk-(?:ant-|proj-)?[A-Za-z0-9_-]{20,}"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{40,}"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
    re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"),
    re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"),
    re.compile(
        r"(?i)\b([A-Z0-9_]*(?:api[_-]?key|secret|token|password|passwd|pwd)[A-Z0-9_]*)\s*[:=]\s*['\"]?[^\s'\"]{8,}"
    ),
]


def redact(text: str | None) -> str | None:
    if not text:
        return text
    out = text
    for pat in _PATTERNS:
        out = pat.sub("[REDACTED]", out)
    return out
