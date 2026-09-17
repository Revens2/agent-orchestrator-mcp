"""Voie d'écriture confinée du runtime `claude-desktop` : patchs unified-diff appliqués par le runner.

L'application Claude Desktop (MSIX) ne peut pas être confinée au workspace :
elle partage le profil/l'historique de l'utilisateur et aucun sandbox du
processus n'est démontrable. Le Desktop ne reçoit donc JAMAIS d'accès direct
au système de fichiers.

À la place, en mode `workspace_write` :
1. l'adapter cadre le prompt (instructions patch : diffs unifiés en blocs
   ```diff, chemins relatifs au workspace) et le transmet à l'UI via le
   bridge (comme en `read_only`) ;
2. la réponse texte du Desktop est relue par le runner (ce module) ;
3. les patchs sont extraits, validés puis appliqués PAR LE RUNNER, bornés
   strictement au workspace résolu (`resolve_workspace`).

Garanties (fail-closed : la moindre violation annule TOUTE l'application) :
- chemins relatifs uniquement (pas d'absolu, pas de `..`, pas d'UNC/device) ;
- `.git/` et liens symboliques refusés ;
- bornes : nb de fichiers, taille totale, taille du patch ;
- le contexte des hunks doit correspondre exactement au fichier existant
  (sinon refus, jamais d'application partielle) ;
- écriture atomique (fichier tmp + replace), parents créés, suppressions
  (`+++ /dev/null`) effectuées par le runner.

Aucune commande n'est jamais exécutée : le Desktop propose du texte, le
runner écrit des fichiers. Lancer un serveur/localhost reste une action
manuelle (ou un autre runtime) hors de ce module.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

# Bornes (fail-closed, jamais négociées par le broker/MCP).
MAX_PATCH_CHARS = 200_000      # réponse+patchs scannés
MAX_PATCH_BLOCKS = 10          # blocs ```diff par réponse
MAX_FILES_PER_JOB = 32         # fichiers distincts patchés par job
MAX_TOTAL_BYTES = 1_000_000    # contenu total écrit par job
MAX_FILE_BYTES = 500_000       # contenu d'un fichier après patch
MAX_PATH_CHARS = 512

# Codes d'erreur stables (adapter, logs, tests).
E_NO_PATCH = "no_patch"
E_PATH_ESCAPE = "path_escape"
E_ABSOLUTE = "absolute_path"
E_BAD_PATH = "bad_path"
E_GIT = "git_forbidden"
E_SYMLINK = "symlink_refused"
E_TOO_MANY = "too_many_files"
E_TOO_LARGE = "patch_too_large"
E_CONTEXT = "context_mismatch"
E_DUPLICATE = "duplicate_file"
E_EMPTY = "empty_patch"

_FENCE_RX = re.compile(r"```(\w[\w+-]*)?[ \t]*\n(.*?)```", re.DOTALL)
_DIFF_LANGS = {"diff", "patch", "udiff", "unified-diff", "unidiff"}
_HUNK_RX = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", re.DOTALL)
_NO_NEWLINE = "\\ No newline at end of file"


@dataclass
class FilePatch:
    rel: str                      # chemin relatif validé (slashes)
    old: str | None               # None => création (`--- /dev/null`)
    new: str | None               # None => suppression (`+++ /dev/null`)
    new_bytes: int = 0


@dataclass
class ApplyReport:
    applied: list[str] = field(default_factory=list)
    created: list[str] = field(default_factory=list)
    modified: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    bytes_written: int = 0


class PatchError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def extract_patches(text: str) -> list[str]:
    """Blocs diff candidats d'une réponse (bornés). Blocs clôturés ```diff (ou
    ```patch/```udiff) d'abord, puis tout bloc ``` contenant `--- ` ; repli :
    texte brut contenant des en-têtes diff."""
    if not isinstance(text, str) or not text.strip():
        return []
    text = text[:MAX_PATCH_CHARS]
    out: list[str] = []
    for lang, body in _FENCE_RX.findall(text):
        blob = body.strip("\n")
        if not blob or len(blob) > MAX_PATCH_CHARS:
            continue
        if ((lang or "").lower() in _DIFF_LANGS or "--- " in blob) and _looks_like_diff(blob):
            out.append(blob)
        if len(out) >= MAX_PATCH_BLOCKS:
            break
    if not out and _looks_like_diff(text):
        out.append(text.strip())
    return out[:MAX_PATCH_BLOCKS]


def _looks_like_diff(blob: str) -> bool:
    return ("--- " in blob and "+++ " in blob) or blob.startswith("diff --git ")


def _clean_path(raw: str) -> str:
    p = raw.strip().strip('"').strip("'").strip()
    p = p.replace("\\", "/")
    if p.startswith(("a/", "b/", "./")):
        p = p[2:]
    return p


def parse_unified_diff(diff: str) -> list[FilePatch]:
    """Parse un diff unifié (éventuellement multi-fichiers). Lève PatchError."""
    if not isinstance(diff, str) or not diff.strip():
        raise PatchError(E_EMPTY, "bloc diff vide")
    if len(diff) > MAX_PATCH_CHARS:
        raise PatchError(E_TOO_LARGE, f"bloc > {MAX_PATCH_CHARS} caractères")
    lines = diff.split("\n")
    files: list[FilePatch] = []
    i, n = 0, len(lines)
    while i < n:
        line = lines[i]
        if line.startswith("diff --git "):
            i += 1
            continue
        if line.startswith("--- "):
            old_raw = line[4:].split("\t", 1)[0]
            # Cherche le +++ correspondant (saute Index:/*** éventuels).
            j = i + 1
            while j < n and not lines[j].startswith("+++ ") and not lines[j].startswith("--- "):
                if lines[j].startswith("diff --git ") or lines[j].startswith("@@ "):
                    break
                j += 1
            if j >= n or not lines[j].startswith("+++ "):
                raise PatchError(E_EMPTY, "en-tête +++ manquant après ---")
            new_raw = lines[j][4:].split("\t", 1)[0]
            old = None if old_raw.strip() == "/dev/null" else _clean_path(old_raw)
            new = None if new_raw.strip() == "/dev/null" else _clean_path(new_raw)
            rel = new if new is not None else old
            if rel is None:
                raise PatchError(E_EMPTY, "--- et +++ /dev/null : patch vide de sens")
            check_rel(rel)
            if old is not None and new is not None and old != new:
                raise PatchError(E_BAD_PATH, f"renommage refusé : {old!r} -> {new!r} (patchs de contenu seuls)")
            hunks: list[str] = []
            k = j + 1
            while k < n and not lines[k].startswith("--- ") and not lines[k].startswith("diff --git "):
                hunks.append(lines[k])
                k += 1
            files.append(FilePatch(rel=rel, old=old, new=new, new_bytes=0))
            files[-1]._hunks = hunks  # type: ignore[attr-defined]  # résolus à l'application
            i = k
        else:
            i += 1
    if not files:
        raise PatchError(E_NO_PATCH, "aucun couple ---/+++ exploitable")
    if len(files) > MAX_FILES_PER_JOB:
        raise PatchError(E_TOO_MANY, f"{len(files)} fichiers > {MAX_FILES_PER_JOB}")
    seen: set[str] = set()
    for f in files:
        if f.rel in seen:
            raise PatchError(E_DUPLICATE, f"fichier patché deux fois : {f.rel!r}")
        seen.add(f.rel)
    return files


def check_rel(rel: str) -> str:
    """Valide un chemin relatif candidat. Retourne la forme normalisée
    (slashes) ou lève PatchError (fail-closed)."""
    if not isinstance(rel, str) or not rel.strip():
        raise PatchError(E_BAD_PATH, "chemin vide")
    p = rel.strip().replace("\\", "/")
    if len(p) > MAX_PATH_CHARS:
        raise PatchError(E_BAD_PATH, f"chemin trop long : {p[:80]!r}")
    low = p.lower()
    if p.startswith(("/", "//")) or low.startswith(("\\\\", "\\\\?\\", "\\\\.\\")):
        raise PatchError(E_ABSOLUTE, f"chemin absolu refusé : {p!r}")
    if re.match(r"^[a-zA-Z]:", p):
        raise PatchError(E_ABSOLUTE, f"chemin avec lecteur refusé : {p!r}")
    parts = [seg for seg in p.split("/") ]
    if any(seg in ("", ".", "..") for seg in parts):
        raise PatchError(E_PATH_ESCAPE, f"chemin hors workspace refusé : {p!r}")
    norm = "/".join(seg for seg in parts)
    low_norm = norm.lower()
    if low_norm == ".git" or low_norm.startswith(".git/"):
        raise PatchError(E_GIT, f".git intouchable : {p!r}")
    return norm


def _read_lines(path: Path) -> tuple[list[str], bool]:
    """(lignes sans fin de ligne, termine_par_newline). Fichier absent => ([], True).
    Normalise CRLF (workspaces Windows) : la comparaison de contexte se fait
    sur LF, l'écriture appliquée est en LF."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return [], True
    if len(data) > MAX_FILE_BYTES + 65536:
        raise PatchError(E_TOO_LARGE, f"fichier existant trop gros : {path.name}")
    try:
        text = data.decode("utf-8", errors="strict") if data else ""
    except UnicodeDecodeError:
        raise PatchError(E_CONTEXT, f"{path.name} : fichier non UTF-8 (patch texte seul)") from None
    if not text:
        return [], False
    nl = text.endswith(("\n", "\r"))
    raw = text.split("\n")
    if text.endswith("\n"):
        raw = raw[:-1]
    lines = [ln.removesuffix("\r") for ln in raw]
    return lines, nl


def _build_new_content(orig: list[str], hunks: list[str], rel: str) -> tuple[list[str], bool]:
    """Applique les hunks à `orig` (vérification exacte du contexte).
    Retourne (nouvelles lignes, trailing_newline)."""
    new: list[str] = []
    idx = 0  # pointeur dans orig (0-based)
    trailing_nl = True
    pending_no_nl = False
    k = 0
    hunks_seen = 0
    while k < len(hunks):
        line = hunks[k]
        if not line:
            # Ligne vide = contexte vide (diffs émis sans trailing space).
            if idx >= len(orig):
                raise PatchError(E_CONTEXT, f"{rel!r} : contexte vide hors fichier")
            new.append(orig[idx])
            idx += 1
            k += 1
            continue
        if line == _NO_NEWLINE:
            trailing_nl = False
            pending_no_nl = False
            k += 1
            continue
        if line.startswith("@@"):
            m = _HUNK_RX.match(line)
            if not m:
                raise PatchError(E_CONTEXT, f"{rel!r} : hunk illisible : {line[:60]!r}")
            old_start = int(m.group(1))
            # Positionne le pointeur (les hunks sont ordonnés).
            want = max(old_start - 1, 0)
            if want < idx:
                raise PatchError(E_CONTEXT, f"{rel!r} : hunks désordonnés")
            while idx < want:
                if idx >= len(orig):
                    raise PatchError(E_CONTEXT, f"{rel!r} : hunk au-delà de la fin du fichier")
                new.append(orig[idx])
                idx += 1
            hunks_seen += 1
            k += 1
            continue
        kind, payload = line[0], line[1:]
        if kind == " ":
            if idx >= len(orig) or orig[idx] != payload:
                raise PatchError(
                    E_CONTEXT,
                    f"{rel!r} : contexte inattendu ligne {idx + 1} "
                    f"(attendu {payload[:60]!r})",
                )
            new.append(orig[idx])
            idx += 1
        elif kind == "-":
            if idx >= len(orig) or orig[idx] != payload:
                raise PatchError(
                    E_CONTEXT,
                    f"{rel!r} : suppression sans correspondance ligne {idx + 1} "
                    f"(attendu {payload[:60]!r})",
                )
            idx += 1
        elif kind == "+":
            new.append(payload)
        else:
            raise PatchError(E_CONTEXT, f"{rel!r} : ligne de hunk inconnue : {line[:60]!r}")
        if pending_no_nl:
            trailing_nl = True
            pending_no_nl = False
        k += 1
        if k < len(hunks) and hunks[k] == _NO_NEWLINE:
            # Marqueur collé à la ligne précédente : s'applique à la fin.
            pass
    if hunks_seen == 0:
        raise PatchError(E_CONTEXT, f"{rel!r} : aucun hunk @@")
    # Queue du fichier après le dernier hunk.
    while idx < len(orig):
        new.append(orig[idx])
        idx += 1
    return new, trailing_nl


def _resolve_target(root: Path, rel: str) -> Path:
    """Chemin absolu du fichier cible, confinement vérifié (fail-closed).
    Refuse symlinks sur la cible et sur ses parents."""
    target = root / Path(*rel.split("/"))
    norm_root = os.path.normcase(os.path.normpath(str(root)))
    norm_target = os.path.normcase(os.path.normpath(str(target)))
    if norm_target != norm_root and not norm_target.startswith(norm_root + os.sep):
        raise PatchError(E_PATH_ESCAPE, f"chemin hors workspace refusé : {rel!r}")
    # Refuse les symlinks (cible ou parents jusqu'à root exclus).
    cur = target if target.is_symlink() else None
    if cur is not None:
        raise PatchError(E_SYMLINK, f"symlink refusé : {rel!r}")
    parent = target.parent
    while len(str(parent)) >= len(str(root)):
        if os.path.normcase(os.path.normpath(str(parent))) == norm_root:
            break
        try:
            if parent.is_symlink():
                raise PatchError(E_SYMLINK, f"symlink parent refusé : {rel!r}")
        except OSError:
            break
        parent = parent.parent
    return target


def apply_patches(workspace_root: str | Path, files: list[FilePatch]) -> ApplyReport:
    """Valide TOUT puis applique (jamais partiel). `files` vient de
    parse_unified_diff ; le contenu est reconstruit depuis les fichiers réels
    + hunks (le Desktop ne fournit que des diffs, jamais le contenu final)."""
    root = Path(workspace_root)
    if not root.is_dir():
        raise PatchError(E_BAD_PATH, f"workspace introuvable : {workspace_root!r}")
    if len(files) > MAX_FILES_PER_JOB:
        raise PatchError(E_TOO_MANY, f"{len(files)} fichiers > {MAX_FILES_PER_JOB}")
    # Passe 1 : résolution + lecture + reconstruction (aucune écriture).
    planned: list[tuple[Path, str, list[str] | None, bool]] = []
    total = 0
    seen: set[str] = set()
    for f in files:
        rel = check_rel(f.rel)
        if rel in seen:
            raise PatchError(E_DUPLICATE, f"fichier patché deux fois : {rel!r}")
        seen.add(rel)
        target = _resolve_target(root, rel)
        hunks: list[str] = getattr(f, "_hunks", [])
        if f.new is None:
            # Suppression : le fichier doit exister et matcher les suppressions.
            if not target.is_file() or target.is_symlink():
                raise PatchError(E_CONTEXT, f"{rel!r} : suppression d'un fichier absent")
            orig, _nl = _read_lines(target)
            _build_new_content(orig, hunks, rel)  # vérifie seulement
            planned.append((target, rel, None, False))
            continue
        if f.old is None:
            # Création : la cible ne doit pas exister.
            if target.exists():
                raise PatchError(E_CONTEXT, f"{rel!r} : création d'un fichier existant (re-baseline requise)")
            added = [h[1:] for h in hunks if h.startswith("+")]
            if not added and not any(h.startswith("@@") for h in hunks):
                raise PatchError(E_CONTEXT, f"{rel!r} : création sans contenu")
            new_lines, trailing_nl = _build_new_content([], hunks, rel)
        else:
            if not target.is_file() or target.is_symlink():
                raise PatchError(E_CONTEXT, f"{rel!r} : fichier absent (re-baseline requise)")
            orig, _nl = _read_lines(target)
            new_lines, trailing_nl = _build_new_content(orig, hunks, rel)
        blob = "\n".join(new_lines) + ("\n" if trailing_nl and new_lines else "")
        total += len(blob.encode("utf-8"))
        if len(blob.encode("utf-8")) > MAX_FILE_BYTES:
            raise PatchError(E_TOO_LARGE, f"{rel!r} : fichier final > {MAX_FILE_BYTES} octets")
        planned.append((target, rel, new_lines, trailing_nl))
    if total > MAX_TOTAL_BYTES:
        raise PatchError(E_TOO_LARGE, f"patch total {total} octets > {MAX_TOTAL_BYTES}")
    # Passe 2 : écriture atomique.
    report = ApplyReport()
    for target, rel, new_lines, trailing_nl in planned:
        if new_lines is None:
            try:
                target.unlink()
            except OSError as exc:
                raise PatchError(E_CONTEXT, f"{rel!r} : suppression impossible ({exc})") from exc
            report.applied.append(rel)
            report.deleted.append(rel)
            continue
        existed = target.exists()
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise PatchError(E_CONTEXT, f"{rel!r} : dossier parent non créable ({exc})") from exc
        blob = "\n".join(new_lines) + ("\n" if trailing_nl and new_lines else "")
        tmp = target.with_name(target.name + f".orch-tmp-{os.getpid()}")
        try:
            tmp.write_text(blob, encoding="utf-8", newline="\n")
            os.replace(tmp, target)
        except OSError as exc:
            raise PatchError(E_CONTEXT, f"{rel!r} : écriture impossible ({exc})") from exc
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass
        report.applied.append(rel)
        report.bytes_written += len(blob.encode("utf-8"))
        (report.modified if existed else report.created).append(rel)
    return report


def response_to_workspace(workspace_root: str | Path, response: str) -> tuple[ApplyReport, str]:
    """Pipeline complet : extrait les patchs d'une réponse Desktop et les
    applique au workspace. Retourne (rapport, détail). Lève PatchError."""
    blocks = extract_patches(response)
    if not blocks:
        raise PatchError(E_NO_PATCH, "aucun bloc ```diff exploitable dans la réponse")
    files: list[FilePatch] = []
    for b in blocks:
        files.extend(parse_unified_diff(b))
    report = apply_patches(workspace_root, files)
    return report, f"{len(report.applied)} fichier(s) : {', '.join(report.applied)[:500]}"
