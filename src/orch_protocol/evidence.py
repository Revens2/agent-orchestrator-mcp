"""Preuves d'exécution `runner_observed` : manifeste de workspace + attestation d'identité, signés.

Niveaux de preuve (du plus faible au plus fort) :
- `agent_reported`      : marqueurs / rapport JSON écrits par l'agent. Ne prouve rien.
- `runner_observed`     : enveloppe produite par le runner isolé (compte A), signée Ed25519 avec une clé
                          que le compte de l'agent (B) ne peut pas lire. Contient le diff de manifeste
                          du workspace (avant/après, arbre de processus tué) et l'attestation OS
                          (deux identités distinctes, sondes refusées).
- `runner_observed_same_user` : enveloppe signée par le runner PERSONNEL (même compte Windows,
                          mode `trusted_personal_runner`) : prouve que le job a tourné sur le runner
                          déclaré et lie le manifeste, mais NE prétend à AUCUNE isolation OS ni à une
                          attestation indépendante (agent_sid == runner_sid accepté, sondes non exigées).
- `externally_verified` : vérification par un tiers (git distant, CI, API). Non utilisé en V1,
                          et JAMAIS produit par le superviseur (ni en mode isolé, ni en mode personnel).

Transport : PREMIÈRE ligne de `result_summary` (champ écrit par le runner, jamais par l'agent ;
le broker coupe en queue, la tête est conservée). Format : `AUTOPILOT-EVIDENCE-V1 <zlib(payload)_hex>.<sig_hex>`,
signature sur le JSON canonique non compressé ; décompression bornée (pas de bombe zlib).
Hexadécimal minuscule : aucun motif de `orch_protocol.redact` ne peut s'y appliquer.
Une enveloppe trouvée ailleurs (stdout agent) est ignorée ; une signature invalide = absence de preuve.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import zlib
from dataclasses import dataclass
from typing import Any

PREFIX = "AUTOPILOT-EVIDENCE-V1 "
VERSION = 1
MAX_CHANGED = 24          # au-delà : manifeste incomplet => fail-closed (audit humain)
MAX_PAYLOAD_BYTES = 64 * 1024
MAX_ENVELOPE_CHARS = 7_000  # MAX_SUMMARY_CHARS broker = 8 000, en tête
# Chemins sensibles (exécution différée chez un humain/CI) : toute modification => audit.
SENSITIVE_GLOBS = (".git/config", ".git/hooks/*", ".git/info/*", ".gitattributes", ".gitmodules",
                   ".github/workflows/*", ".vscode/*", ".npmrc", ".env", ".env.*")


class EvidenceError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def canonical(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("ascii")


# ---------------------------------------------------------------- manifeste
def diff_manifests(before: dict[str, dict], after: dict[str, dict]) -> list[dict]:
    """before/after : {chemin_relatif_posix: {"sha256", "size"}}. Renvoie les changements triés."""
    changed = []
    for path in sorted(set(before) | set(after)):
        b, a = before.get(path), after.get(path)
        if b is None:
            changed.append({"path": path, "op": "added", "sha256": a["sha256"], "size": a["size"]})
        elif a is None:
            changed.append({"path": path, "op": "deleted"})
        elif a["sha256"] != b["sha256"]:
            changed.append({"path": path, "op": "modified", "sha256": a["sha256"], "size": a["size"]})
    return changed


def manifest_digest(manifest: dict[str, dict]) -> str:
    return hashlib.sha256(canonical(manifest)).hexdigest()


# ---------------------------------------------------------------- enveloppe
def encode_envelope(payload: dict, sign) -> str:
    """`sign(bytes) -> bytes` (clé privée du runner). Lève si l'enveloppe dépasse la taille admise."""
    raw = canonical(payload)
    line = PREFIX + zlib.compress(raw, 9).hex() + "." + sign(raw).hex()
    if len(line) > MAX_ENVELOPE_CHARS:
        raise EvidenceError("envelope_too_large", f"enveloppe {len(line)} > {MAX_ENVELOPE_CHARS}")
    return line


def decode_envelope(summary: str | None, verify) -> dict:
    """`verify(sig, data)` lève si invalide. Seule la première ligne du résumé est considérée."""
    if not summary or not summary.startswith(PREFIX):
        raise EvidenceError("evidence_missing", "aucune enveloppe runner en tête du résumé")
    line = summary.split("\n", 1)[0][len(PREFIX):].strip()
    try:
        payload_hex, sig_hex = line.split(".", 1)
        packed, sig = bytes.fromhex(payload_hex), bytes.fromhex(sig_hex)
        inflater = zlib.decompressobj()
        raw = inflater.decompress(packed, MAX_PAYLOAD_BYTES)
        if inflater.unconsumed_tail or not inflater.eof:
            raise ValueError("payload trop grand ou tronqué")
    except (ValueError, zlib.error) as exc:
        raise EvidenceError("evidence_malformed", "enveloppe illisible") from exc
    try:
        verify(sig, raw)
    except Exception as exc:  # toute erreur de vérification = preuve absente
        raise EvidenceError("evidence_bad_signature", "signature invalide") from exc
    try:
        payload = json.loads(raw.decode("ascii"))
    except ValueError as exc:
        raise EvidenceError("evidence_malformed", "payload non JSON") from exc
    if not isinstance(payload, dict) or payload.get("v") != VERSION:
        raise EvidenceError("evidence_malformed", "version d'enveloppe inconnue")
    return payload


def ed25519_signer(private_key_bytes: bytes):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    key = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
    return key.sign


def ed25519_verifier(public_key_hex: str):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    key = Ed25519PublicKey.from_public_bytes(bytes.fromhex(public_key_hex))
    return key.verify


# ---------------------------------------------------------------- politique
@dataclass(frozen=True)
class Expectation:
    """Attendus figés par la SPEC (cockpit), jamais par l'agent."""

    kind: str                                   # read | local_write
    artifacts: tuple[str, ...] = ()             # fichiers qui doivent exister après (ajoutés/modifiés)
    allowed_paths: tuple[str, ...] = ()         # globs autorisés à changer (défaut : artifacts)


@dataclass(frozen=True)
class Binding:
    """Ce que le superviseur sait indépendamment de l'agent."""

    runner_id: str
    job_id: str
    workspace_id: str
    mode: str
    exit_code: int | None
    expected_agent_sid: str | None
    not_before: float | None = None             # dispatch de la tentative (anti-rejeu)
    # prompt EXACT figé dans l'outbox pour cette tentative (contient objective/step/attempt). Le fencing
    # (claim courant) est imposé par le broker : une transition d'un ancien détenteur est refusée.
    prompt_sha256: str | None = None


def check_payload(payload: dict, exp: Expectation, bind: Binding, allow_same_user: bool = False) -> list[str]:
    """Renvoie la liste des violations (vide = preuve runner_observed acceptée).

    `allow_same_user=True` (mode `trusted_personal_runner` uniquement) : accepte une enveloppe
    signée du runner personnel où agent et runner partagent le même SID Windows. Les contrôles
    d'isolation (deux identités distinctes, non-admin, sondes refusées, attestation ok) sont
    remplacés par un contrôle d'identité même-utilisateur honnête ; les contrôles de liaison
    (runner/job/workspace/mode/exit/prompt/fraîcheur/arbre) et de manifeste restent intacts.
    Ne produit jamais `externally_verified`.
    """
    v: list[str] = []

    def need(cond: bool, code: str) -> None:
        if not cond:
            v.append(code)

    need(payload.get("runner_id") == bind.runner_id, "runner_id_mismatch")
    need(payload.get("job_id") == bind.job_id, "job_id_mismatch")       # rejeu d'une autre tentative
    need(payload.get("workspace_id") == bind.workspace_id, "workspace_mismatch")
    need(payload.get("mode") == bind.mode, "mode_mismatch")
    need(payload.get("exit_code") == bind.exit_code, "exit_code_mismatch")
    need(bind.prompt_sha256 is not None and payload.get("prompt_sha256") == bind.prompt_sha256, "prompt_mismatch")
    if bind.not_before is not None:
        started = payload.get("started_at")
        need(isinstance(started, (int, float)) and started >= bind.not_before - 300, "stale_evidence")
    need(payload.get("tree_killed") is True, "process_tree_not_closed")

    att = payload.get("attestation")
    if not isinstance(att, dict):
        v.append("attestation_missing")
    elif allow_same_user:
        # Mode personnel : aucune isolation prétendue. On exige une identité même-utilisateur
        # explicite (agent_sid == runner_sid, deux SIDs présents), sans exiger sondes refusées,
        # non-admin, ni attestation ok. Les sondes éventuelles sont conservées telles quelles
        # dans l'enveloppe à titre informatif, jamais comme preuve d'enfermement.
        same = att.get("agent_sid") and att.get("agent_sid") == att.get("runner_sid")
        need(bool(same), "personal_identity_mismatch")
        if bind.expected_agent_sid is not None:
            need(att.get("agent_sid") == bind.expected_agent_sid, "agent_sid_mismatch")
    else:
        need(att.get("ok") is True, "attestation_failed")
        need(att.get("agent_is_admin") is False, "agent_admin")
        need(att.get("agent_elevated") is False, "agent_elevated")
        need(bind.expected_agent_sid is not None and att.get("agent_sid") == bind.expected_agent_sid, "agent_sid_mismatch")
        need(att.get("agent_sid") != att.get("runner_sid"), "agent_shares_runner_identity")
        # SID lu sur le jeton du PROCESSUS agent par A : la sonde confirme, elle ne prouve pas
        need(att.get("agent_process_sid") is not None and att.get("agent_process_sid") == att.get("agent_sid"),
             "agent_process_identity_unverified")
        probes = att.get("probes")
        need(isinstance(probes, list) and len(probes) > 0 and all(
            isinstance(p, dict) and p.get("denied") is True for p in probes), "probe_not_denied")

    man = payload.get("manifest")
    if not isinstance(man, dict):
        v.append("manifest_missing")
        return v
    need(man.get("complete") is True, "manifest_incomplete")
    need(not man.get("reparse_points"), "reparse_point")
    need(not man.get("suspicious"), "suspicious_entry")   # hardlinks, noms à point/espace final
    need(man.get("ads_checked") is True and not man.get("alternate_streams"), "alternate_data_stream")
    changed = man.get("changed")
    if not isinstance(changed, list):
        v.append("manifest_missing")
        return v
    paths = [c.get("path") for c in changed if isinstance(c, dict)]
    if any(not isinstance(p, str) for p in paths):
        v.append("manifest_malformed")
        return v
    if any(_match(p, SENSITIVE_GLOBS) for p in paths):
        v.append("sensitive_path_changed")
    if exp.kind == "read":
        need(not changed, "read_step_modified_workspace")
        return v
    allowed = exp.allowed_paths or exp.artifacts
    outside = [p for p in paths if not _match(p, allowed)]
    need(not outside, "out_of_scope_change")
    final = {c["path"]: c for c in changed if c.get("op") in ("added", "modified")}
    for art in exp.artifacts:
        c = final.get(art)
        need(c is not None and isinstance(c.get("size"), int) and c["size"] > 0, f"artifact_missing:{art}")
    return v


def _match(path: str, globs) -> bool:
    return any(fnmatch.fnmatchcase(path, g) for g in globs)
