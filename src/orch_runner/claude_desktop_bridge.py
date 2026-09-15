"""Bridge local pilotant l'application Claude Desktop (MSIX) pour le runtime orch `claude-desktop`.

Cascade reprise d'`osauto` (`C:\\Users\\Juliann\\Desktop\\ui controle`, Spec §cascade) :
`CLI/powershell` -> `UIA (.NET UIAutomationClient, brique Module 2 / Option B)` ->
`sondes d'effet de bord` (stabilité de la conversation + `IsHungAppWindow`, Module 3) ->
vision EXCLUE (tâche texte, jamais de capture).

AUCUN clic à coordonnées fixes : l'entrée est ciblée par AutomationId/Name UIA
(ValuePattern.SetValue, repli presse-papiers + Ctrl+V), l'envoi par InvokePattern
du bouton Send (repli touche Entrée). Le HWND est résolu par énumération des
fenêtres visibles (EnumWindows + GetWindowText, socle `osauto.core.win32`).

Garanties exigées par le broker :
- vérification fermée : package MSIX Claude (éditeur Anthropic), processus `Claude.exe`
  issu de `WindowsApps`, fenêtre visible, profil attendu (défaut `Caroline`,
  via `ORCH_CLAUDE_DESKTOP_PROFILE` ou `--profile`) présent dans l'arbre UIA.
  Écart de profil => échec `profile_mismatch` (le compte Desktop n'est JAMAIS
  modifié ni basculé par ce bridge) ;
- sérialisation : verrou fichier `%TEMP%\\orch-claude-desktop.lock` (locking
  exclusif, attente bornée) — un seul pilote UI à la fois ;
- résultat texte : la réponse est extraite par diff de conversation, écrite dans
  `<out-dir>/last_message.txt` et émise en NDJSON (`{"type": "result", ...}`),
  format consommé par l'adapter (résumé borné exploitable par le broker).

Confinement : l'application Desktop partage le profil/l'historique de
l'utilisateur, aucun confinement au workspace n'est démontrable → le runtime
n'annonce QUE `read_only` (refus broker + runner en `workspace_write`).

Usage (appelé par l'adapter, jamais à la main en prod) :
    python claude_desktop_bridge.py --prompt-file P.txt --out-dir D [--profile Caroline]
    python claude_desktop_bridge.py --verify-only
    python claude_desktop_bridge.py --self-test
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from ctypes import wintypes
from pathlib import Path

# Nom réel constaté : `Claude` (PackageFullName `Claude_*__pzs8sxrjxfjjc`,
# éditeur Anthropic). On matche large sur le nom + éditeur, jamais exact seul.
MSIX_NAME_RX = re.compile(r"claude", re.IGNORECASE)
MSIX_PUBLISHER_RX = re.compile(r"anthropic", re.IGNORECASE)
MSIX_PATH_MARK = "windowsapps"  # comparé en minuscules
WINDOW_TITLE_RX = re.compile(r"claude", re.IGNORECASE)
DEFAULT_PROFILE = os.environ.get("ORCH_CLAUDE_DESKTOP_PROFILE", "Caroline")
LOCK_NAME = "orch-claude-desktop.lock"
INSPECT_CAP = 500
TEXT_CAP = 2000
RESULT_CAP = 8000

# Codes d'erreur stables (l'adapter et les tests s'appuient sur ces préfixes).
E_NOT_WINDOWS = "not_windows"
E_NO_PACKAGE = "claude_desktop_not_installed"
E_NOT_RUNNING = "claude_desktop_not_running"
E_NO_WINDOW = "claude_desktop_no_window"
E_PROFILE = "profile_mismatch"
E_HUNG = "claude_desktop_hung"
E_UIA = "uia_unavailable"
E_INPUT = "input_not_found"
E_BUSY = "ui_busy"
E_TIMEOUT = "response_timeout"
E_SEND = "send_failed"


def emit(kind: str, text: str) -> None:
    print(json.dumps({"type": kind, "text": text}, ensure_ascii=False), flush=True)


def fail(code: str, detail: str, out_dir: Path | None = None) -> int:
    text = detail if detail.startswith(code + ":") else f"{code}: {detail}"
    emit("error", text)
    if out_dir is not None:
        try:
            (out_dir / "bridge_error.txt").write_text(f"{code}: {detail}\n", encoding="utf-8")
        except OSError:
            pass
    return 1


def lock_path() -> Path:
    return Path(os.environ.get("TEMP", tempfile.gettempdir())) / LOCK_NAME


# ------------------------------------------------------------ pure helpers ---
def profile_in_texts(texts: list[str], expected: str) -> bool:
    """Le profil attendu apparaît-il dans l'arbre UIA (insensible à la casse) ?"""
    want = expected.strip().lower()
    if not want:
        return False
    return any(want in (t or "").lower() for t in texts)


def _ctname(raw: str) -> str:
    return raw.split(".")[-1] if raw else ""


# Mot entier uniquement : un `go` sous-chaîne matchait "Google Drive…" avant le
# vrai bouton (régression E2E 2026-09-15 : l'envoi partait sur un faux bouton).
_SEND_RX = re.compile(r"\b(envoyer|send|submit|transmettre)\b", re.IGNORECASE)
_EDIT_KEYS = ("input", "message", "chat", "prompt", "composer", "ask", "search")


def find_input_and_send(elements: list[dict]) -> tuple[dict | None, dict | None]:
    """Repère la zone de saisie (Edit) et le bouton d'envoi, sans coordonnées.

    Edit : AutomationId évocateur, puis Name évocateur, puis dernier Edit.
    Bouton : mot entier (envoyer/send/…) ; en cas d'homonymes, le premier
    APRÈS l'Edit (le bouton d'envoi jouxte le composeur), sinon le dernier.
    """
    indexed = list(enumerate(elements))
    edits = [(i, e) for (i, e) in indexed if _ctname(str(e.get("ct", ""))) == "Edit"]
    edit: dict | None = None
    edit_idx = -1
    for i, e in edits:
        blob = f"{e.get('aid', '')} {e.get('name', '')}".lower()
        if any(k in blob for k in _EDIT_KEYS):
            edit, edit_idx = e, i
            break
    if edit is None:
        for i, e in edits:
            if e.get("name") or e.get("aid"):
                edit, edit_idx = e, i
                break
    if edit is None and edits:
        edit_idx, edit = edits[-1]
    cands = [
        (i, e)
        for (i, e) in indexed
        if _ctname(str(e.get("ct", ""))) in ("Button", "SplitButton") and _SEND_RX.search(str(e.get("name", "")))
    ]
    send: dict | None = None
    if cands:
        after = [(i, e) for (i, e) in cands if i > edit_idx]
        send = (after or [cands[-1]])[0][1]
    return edit, send


def composer_holds(elements: list[dict], anchor: str, width: int = 24) -> bool:
    """Le composeur contient-il encore notre texte (envoi non parti) ?"""
    edit, _ = find_input_and_send(elements)
    if edit is None:
        return False
    return anchor[:width] in " ".join(str(edit.get("text") or "").split())


def conversation_text(elements: list[dict]) -> str:
    """Texte conversationnel concaténé (Documents/Textes/Edits non vides)."""
    parts = []
    for e in elements:
        ct = _ctname(str(e.get("ct", "")))
        if ct in ("Document", "Text", "Edit", "ListItem", "Hyperlink"):
            t = (e.get("text") or "").strip()
            if t:
                parts.append(t)
    return "\n".join(parts)


def conversation_diff(before: str, after: str, cap: int = RESULT_CAP) -> str:
    """Nouveau contenu apparu entre deux snapshots (repli : queue bornée)."""
    if after.startswith(before):
        new = after[len(before) :].strip()
    else:
        # Fenêtre re-rendue (virtualisation) : on prend la queue inédite.
        tail = after.strip()
        new = tail if tail not in before else tail[-cap:]
    return new[:cap].strip()


def is_stable(history: list[str], stable_rounds: int = 5) -> bool:
    """Stabilité = `stable_rounds` snapshots identiques consécutifs et non vides."""
    if len(history) < stable_rounds:
        return False
    tail = history[-stable_rounds:]
    return bool(tail[-1].strip()) and all(t == tail[-1] for t in tail)


# Annonce de fin de génération (dépend de la locale/version : fast-path seul,
# la stabilité reste le critère de repli). Recherchée APRÈS l'écho du prompt.
RESPONSE_MARKS = ("a terminé la réponse", "finished responding", "response complete")


def prompt_anchor(prompt: str, width: int = 48) -> str:
    """Ancre de localisation de notre message : début normalisé (espaces repliés)."""
    return " ".join(prompt.split())[:width]


def response_tail(current: str, anchor: str) -> str | None:
    """Texte situé après la première occurrence de l'ancre, sinon None."""
    norm = " ".join(current.split())
    idx = norm.find(anchor)
    if idx < 0:
        return None
    return norm[idx + len(anchor) :].strip()


def response_finished(tail: str) -> bool:
    """Une annonce de fin de génération figure-t-elle dans la queue ?"""
    low = tail.lower()
    return any(m in low for m in RESPONSE_MARKS)


# ---------------------------------------------------------------- windows ---
def _user32():
    if sys.platform != "win32":
        raise OSError(E_NOT_WINDOWS)
    return ctypes.windll.user32


def run_powershell(script: str, timeout_s: int = 60) -> str:
    """Exécute un script PowerShell STATIQUE (aucune interpolation du prompt :
    les paramètres transitent par variables d'environnement / fichiers)."""
    out = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
        capture_output=True,
        text=True,
        timeout=timeout_s,
        creationflags=0x08000000,
        encoding="utf-8",
        errors="replace",
        stdin=subprocess.DEVNULL,
    )
    if out.returncode != 0:
        raise OSError(f"{E_UIA}: powershell exit {out.returncode}: {(out.stderr or out.stdout)[:500]}")
    return out.stdout or ""


# Détection large : nom ~Claude ET éditeur ~Anthropic (le nom exact du package
# a varié selon les canaux : `Claude` constaté, `Anthropic.Claude` vu ailleurs).
# Sortie : `Nom|Version` (vide si introuvable).
PS_PACKAGE = (
    "Get-AppxPackage -ErrorAction SilentlyContinue | "
    "Where-Object { ($_.Name -like '*Claude*') -and ($_.Publisher -like '*Anthropic*') } | "
    "Select-Object -First 1 | "
    "ForEach-Object { Write-Output ($_.Name + '|' + $_.Version) }"
)

# Get-Process (complet, Path renseigné pour le MSIX). ToString() obligatoire :
# `int + string` tenterait une addition arithmétique et échouerait par élément.
PS_DESKTOP_PROCS = (
    "Get-Process -Name Claude -ErrorAction SilentlyContinue | ForEach-Object { Write-Output ($_.Id.ToString() + '|' + $_.Path) }"
)

# Inspection UIA (.NET UIAutomationClient, brique osauto Module 2 / Option B).
# Entrées par env : ORCH_UIA_HWND, ORCH_UIA_CAP. Sortie : JSON [{ct,name,aid,text}].
PS_INSPECT = r"""
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName UIAutomationClient
$hwnd = [IntPtr]::new([int64]$env:ORCH_UIA_HWND)
$cap = [int]$env:ORCH_UIA_CAP
$el = [System.Windows.Automation.AutomationElement]::FromHandle($hwnd)
if ($null -eq $el) { Write-Output '[]'; exit 0 }
$walker = [System.Windows.Automation.TreeWalker]::ContentViewWalker
$out = @()
$stack = New-Object System.Collections.Generic.Stack[object]
$stack.Push($el)
$ctProp = [System.Windows.Automation.AutomationElement]::ControlTypeProperty
$nameProp = [System.Windows.Automation.AutomationElement]::NameProperty
$aidProp = [System.Windows.Automation.AutomationElement]::AutomationIdProperty
while ($stack.Count -gt 0 -and $out.Count -lt $cap) {
  $n = $stack.Pop()
  try {
    $ct = $n.GetCurrentPropertyValue($ctProp)
    $nm = [string]$n.GetCurrentPropertyValue($nameProp)
  } catch { continue }
  $aid = ''
  try { $aid = [string]$n.GetCurrentPropertyValue($aidProp) } catch {}
  $val = ''
  try {
    $pat = $n.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern)
    if ($null -ne $pat) {
      $vp = [System.Windows.Automation.ValuePattern]$pat
      if ($vp.Current.IsReadOnly -eq $false) { $val = [string]$vp.Current.Value }
    }
  } catch {}
  $txt = $nm
  if ($val.Length -gt $txt.Length) { $txt = $val }
  $m = [Math]::Min(2000, $txt.Length)
  $out += [pscustomobject]@{ct = [string]$ct.ProgrammaticName; name = $nm; aid = $aid; text = $txt.Substring(0, $m)}
  try {
    $kids = @()
    $k = $walker.GetFirstChild($n)
    while ($null -ne $k) { $kids += $k; try { $k = $walker.GetNextSibling($k) } catch { $k = $null } }
    for ($i = $kids.Count - 1; $i -ge 0; $i--) { $stack.Push($kids[$i]) }
  } catch {}
}
$out | ConvertTo-Json -Compress -Depth 3
"""

# Action UIA : focus + saisie (ValuePattern, repli presse-papiers) + envoi.
# Localisation par AutomationId si renseigné, sinon par Name, sinon premier
# Edit (le composeur Desktop n'expose aucun AutomationId : Name "Prompt").
# Méthodes d'envoi (ORCH_UIA_METHOD) : `auto` (Invoke du bouton puis Entrée),
# `invoke` (bouton seul), `ctrl_enter`, `enter`. Le pilotage vérifie ensuite
# que le composeur s'est vidé (retry escaladé), jamais de clic coordonné.
PS_ACT = r"""
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName System.Windows.Forms
$hwnd = [IntPtr]::new([int64]$env:ORCH_UIA_HWND)
$root = [System.Windows.Automation.AutomationElement]::FromHandle($hwnd)
if ($null -eq $root) { Write-Output '{"ok": false, "detail": "no_root"}'; exit 0 }
$aidProp = [System.Windows.Automation.AutomationElement]::AutomationIdProperty
$nameProp = [System.Windows.Automation.AutomationElement]::NameProperty
$ctProp = [System.Windows.Automation.AutomationElement]::ControlTypeProperty
$method = $env:ORCH_UIA_METHOD
function Find-ByProp($prop, $value) {
  if (-not $value) { return $null }
  $cond = New-Object System.Windows.Automation.PropertyCondition($prop, $value)
  return $root.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $cond)
}
$edit = Find-ByProp $aidProp $env:ORCH_UIA_AID
if ($null -eq $edit) { $edit = Find-ByProp $nameProp $env:ORCH_UIA_ANAME }
if ($null -eq $edit) {
  $cond = New-Object System.Windows.Automation.PropertyCondition($ctProp, [System.Windows.Automation.ControlType]::Edit)
  $edit = $root.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $cond)
}
if ($null -eq $edit) { Write-Output '{"ok": false, "detail": "no_edit"}'; exit 0 }
$edit.SetFocus()
Start-Sleep -Milliseconds 400
$text = [System.IO.File]::ReadAllText($env:ORCH_UIA_TEXT_FILE)
$set = $false
try {
  $vp = [System.Windows.Automation.ValuePattern]$edit.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern)
  if ($vp.Current.IsReadOnly -eq $false) { $vp.SetValue($text); $set = $true }
} catch {}
if (-not $set) {
  [System.Windows.Forms.Clipboard]::SetText($text)
  $edit.SetFocus()
  Start-Sleep -Milliseconds 200
  [System.Windows.Forms.SendKeys]::SendWait('^a')
  Start-Sleep -Milliseconds 200
  [System.Windows.Forms.SendKeys]::SendWait('^v')
  Start-Sleep -Milliseconds 600
}
$invoked = $false
if ($method -eq 'invoke' -or $method -eq 'auto') {
  $btn = Find-ByProp $aidProp $env:ORCH_UIA_SEND
  if ($null -eq $btn) { $btn = Find-ByProp $nameProp $env:ORCH_UIA_SNAME }
  if ($null -ne $btn) {
    try {
      $inv = [System.Windows.Automation.InvokePattern]$btn.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern)
      $inv.Invoke(); $invoked = $true
    } catch {}
  }
}
if (-not $invoked -and ($method -eq 'auto' -or $method -eq 'enter')) {
  $edit.SetFocus()
  Start-Sleep -Milliseconds 200
  [System.Windows.Forms.SendKeys]::SendWait('{ENTER}')
}
if (-not $invoked -and $method -eq 'ctrl_enter') {
  $edit.SetFocus()
  Start-Sleep -Milliseconds 200
  [System.Windows.Forms.SendKeys]::SendWait('^~')
}
Write-Output ('{"ok": true, "detail": "' + $method + '"}')
"""


def package_id() -> str:
    """`Nom|Version` du package Desktop, ou chaîne vide (jamais d'exception)."""
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", PS_PACKAGE],
            capture_output=True,
            text=True,
            timeout=30,
            creationflags=0x08000000,
            encoding="utf-8",
            errors="replace",
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    line = (out.stdout or "").strip().splitlines()
    return line[0].strip() if line else ""


def desktop_pids() -> dict[int, str]:
    """PID -> chemin des `Claude.exe` issus du package MSIX (pas la CLI)."""
    try:
        out = run_powershell(PS_DESKTOP_PROCS, timeout_s=30)
    except OSError:
        return {}
    found: dict[int, str] = {}
    for line in out.splitlines():
        if "|" not in line:
            continue
        pid_s, path = line.split("|", 1)
        try:
            pid = int(pid_s.strip())
        except ValueError:
            continue
        path = (path or "").strip()
        if path and MSIX_PATH_MARK in path.lower() and "claude" in path.lower():
            found[pid] = path
    return found


def enum_windows() -> list[tuple[int, str, int]]:
    """(hwnd, titre, pid) des fenêtres visibles titrées (socle osauto win32)."""
    user32 = _user32()
    found: list[tuple[int, str, int]] = []
    proto = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)

    def _cb(hwnd, _lp):
        try:
            if not user32.IsWindowVisible(hwnd):
                return True
            length = int(user32.GetWindowTextLengthW(hwnd))
            if length <= 0:
                return True
            buf = ctypes.create_unicode_buffer(length + 1)
            user32.GetWindowTextW(hwnd, buf, length + 1)
            pid = wintypes.DWORD()
            user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
            found.append((hwnd, buf.value, int(pid.value)))
        except Exception:  # noqa: BLE001, S110 - callback d'énumération : ne doit jamais mourir
            pass
        return True

    user32.EnumWindows(proto(_cb), 0)
    return found


def find_desktop_window() -> tuple[int, str, int, str] | None:
    """Fenêtre Claude Desktop : titre ~Claude + PID issu du MSIX."""
    pids = desktop_pids()
    if not pids:
        return None
    cands = [(h, t, p) for (h, t, p) in enum_windows() if p in pids and WINDOW_TITLE_RX.search(t or "")]
    if not cands:
        # Repli : toute fenêtre visible du processus Desktop (titre localisé ?).
        cands = [(h, t, p) for (h, t, p) in enum_windows() if p in pids and (t or "").strip()]
    if not cands:
        return None
    hwnd, title, pid = max(cands, key=lambda c: len(c[1] or ""))
    return hwnd, title, pid, pids[pid]


def is_hung(hwnd: int) -> bool:
    try:
        return bool(_user32().IsHungAppWindow(wintypes.HWND(hwnd)))
    except Exception:  # noqa: BLE001 - sonde best-effort : doute = non figé
        return False


def bring_to_front(hwnd: int) -> None:
    try:
        _user32().SetForegroundWindow(wintypes.HWND(hwnd))
    except Exception:  # noqa: BLE001, S110 - best-effort (le focus peut être refusé)
        pass


def inspect_uia(hwnd: int, cap: int = INSPECT_CAP, timeout_s: int = 90) -> list[dict]:
    env = dict(os.environ, ORCH_UIA_HWND=str(hwnd), ORCH_UIA_CAP=str(cap))
    proc = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", PS_INSPECT],
        capture_output=True,
        text=True,
        timeout=timeout_s,
        creationflags=0x08000000,
        encoding="utf-8",
        errors="replace",
        stdin=subprocess.DEVNULL,
        env=env,
    )
    if proc.returncode != 0:
        raise OSError(f"{E_UIA}: inspect exit {proc.returncode}: {(proc.stderr or proc.stdout)[:500]}")
    try:
        data = json.loads((proc.stdout or "").strip() or "[]")
    except ValueError as exc:
        raise OSError(f"{E_UIA}: inspect JSON illisible: {exc}") from exc
    if isinstance(data, dict):
        data = [data]
    return [d for d in data if isinstance(d, dict)][:cap]


def act_uia(hwnd: int, edit: dict, send: dict | None, text_file: Path, timeout_s: int = 120, method: str = "auto") -> dict:
    env = dict(
        os.environ,
        ORCH_UIA_HWND=str(hwnd),
        ORCH_UIA_AID=str(edit.get("aid") or ""),
        ORCH_UIA_ANAME=str(edit.get("name") or ""),
        ORCH_UIA_TEXT_FILE=str(text_file),
        ORCH_UIA_SEND=str((send or {}).get("aid") or ""),
        ORCH_UIA_SNAME=str((send or {}).get("name") or ""),
        ORCH_UIA_METHOD=method,
    )
    proc = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", PS_ACT],
        capture_output=True,
        text=True,
        timeout=timeout_s,
        creationflags=0x08000000,
        encoding="utf-8",
        errors="replace",
        stdin=subprocess.DEVNULL,
        env=env,
    )
    if proc.returncode != 0:
        raise OSError(f"{E_UIA}: act exit {proc.returncode}: {(proc.stderr or proc.stdout)[:500]}")
    try:
        return json.loads((proc.stdout or "").strip() or "{}")
    except ValueError as exc:
        raise OSError(f"{E_UIA}: act JSON illisible: {exc}") from exc


class UiLock:
    """Verrou fichier exclusif (msvcrt) : sérialise l'accès à l'UI Desktop."""

    def __init__(self, path: Path, timeout_s: float = 300) -> None:
        self.path = path
        self.timeout_s = timeout_s
        self._fh = None

    def acquire(self) -> None:
        import msvcrt

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a+b")  # noqa: SIM115 - handle conservé jusqu'à release()
        deadline = time.monotonic() + self.timeout_s
        while True:
            try:
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
                self._fh.seek(0, 2)
                self._fh.write(f"{os.getpid()} {time.time()}\n".encode())
                self._fh.flush()
                return
            except OSError:
                if time.monotonic() >= deadline:
                    try:
                        self._fh.close()
                    except OSError:
                        pass
                    self._fh = None
                    raise TimeoutError(E_BUSY)
                time.sleep(1.0)

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            import msvcrt

            try:
                self._fh.seek(0, 2)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
            self._fh.close()
        finally:
            self._fh = None


# ------------------------------------------------------------------ flows ---
def do_verify(profile: str) -> tuple[int, str, dict]:
    """Vérification fermée Desktop + profil. Retourne (hwnd, title, infos)."""
    if sys.platform != "win32":
        raise OSError(E_NOT_WINDOWS)
    pkg = package_id()
    if not pkg:
        raise OSError(f"{E_NO_PACKAGE}: package MSIX Claude Desktop (éditeur Anthropic) introuvable")
    ver = pkg.split("|", 1)[1] if "|" in pkg else pkg
    if not desktop_pids():
        raise OSError(f"{E_NOT_RUNNING}: aucun Claude.exe MSIX actif (CLI claude-code exclue)")
    win = find_desktop_window()
    if win is None:
        raise OSError(f"{E_NO_WINDOW}: aucune fenêtre visible du Desktop")
    hwnd, title, _pid, _path = win
    if is_hung(hwnd):
        raise OSError(f"{E_HUNG}: la fenêtre Desktop ne répond pas (IsHungAppWindow)")
    elements = inspect_uia(hwnd)
    texts = [str(e.get("text") or "") for e in elements] + [title]
    if not profile_in_texts(texts, profile):
        raise OSError(
            f"{E_PROFILE}: profil {profile!r} absent de l'arbre UIA (compte inattendu : refus de piloter, bascule interdite)"
        )
    return hwnd, title, {"version": ver, "elements": len(elements)}


def do_drive(
    prompt_file: Path,
    out_dir: Path,
    profile: str,
    timeout_s: int,
    poll_s: float = 2.0,
    stable_s: float = 10.0,
    lock_timeout_s: float = 300.0,
    min_response_s: float = 25.0,
) -> str:
    prompt = prompt_file.read_text(encoding="utf-8", errors="replace")
    if not prompt.strip():
        raise ValueError("prompt vide")
    if len(prompt) > 100_000:
        raise ValueError("prompt > 100 000 caractères")
    lock = UiLock(lock_path(), timeout_s=lock_timeout_s)
    emit("activity", "verrou UI en cours d'acquisition…")
    try:
        lock.acquire()
    except TimeoutError:
        raise OSError(f"{E_BUSY}: une autre session pilote déjà Claude Desktop") from None
    try:
        emit("activity", "vérification Claude Desktop + profil…")
        hwnd, title, info = do_verify(profile)
        emit("activity", f"Desktop vérifié (v{info['version']}, {info['elements']} éléments UIA)")
        if is_hung(hwnd):
            raise OSError(f"{E_HUNG}: Desktop figé avant action")
        bring_to_front(hwnd)
        time.sleep(0.6)
        elements = inspect_uia(hwnd)
        baseline = conversation_text(elements)
        edit, send = find_input_and_send(elements)
        if edit is None:
            raise OSError(f"{E_INPUT}: aucune zone de saisie Edit dans l'arbre UIA")
        text_file = out_dir / "prompt_uia.txt"
        text_file.write_text(prompt, encoding="utf-8")
        anchor = prompt_anchor(prompt)
        emit("activity", "saisie du prompt via UIA (ValuePattern, repli presse-papiers)…")
        # Envoi vérifié : le composeur doit se vider, sinon retry escaladé
        # (invoke bouton → ctrl+entrée → entrée). Un texte resté dans le
        # composeur ressemble à un écho mais n'a jamais été envoyé.
        for method in ("auto", "ctrl_enter", "enter"):
            res = act_uia(hwnd, edit, send, text_file, method=method)
            if not res.get("ok"):
                raise OSError(f"{E_UIA}: envoi impossible ({res.get('detail')})")
            time.sleep(4.0)
            try:
                probe = inspect_uia(hwnd)
            except OSError:
                probe = None
            if probe is None or not composer_holds(probe, anchor):
                emit("activity", f"envoi parti (méthode {method})")
                break
            emit("activity", f"composeur non vidé (méthode {method}), nouvel essai…")
        else:
            raise OSError(f"{E_SEND}: le prompt reste dans le composeur après 3 méthodes")
        if is_hung(hwnd):
            raise OSError(f"{E_HUNG}: Desktop figé après envoi")
        try:
            text_file.unlink()
        except OSError:
            pass
        # Sonde Module 3 : on attend la réponse À NOTRE message, pas une
        # stabilité quelconque. L'écho du prompt ancre la zone ; ensuite soit
        # l'annonce de fin de génération (fast-path, +2 polls de tassement),
        # soit la stabilité de la queue passée `min_response_s` (le modèle met
        # plusieurs secondes à commencer à répondre : une stabilité précoce
        # ne capture que l'écho, cf. E2E 2026-09-15).
        emit("activity", "attente de la réponse (écho + fin de génération)…")
        deadline = time.monotonic() + timeout_s
        sent_at = time.monotonic()
        history: list[str] = []
        tails: list[str] = []
        current = baseline
        settled = False
        while time.monotonic() < deadline:
            time.sleep(poll_s)
            try:
                current = conversation_text(inspect_uia(hwnd))
            except OSError as exc:
                emit("activity", f"sonde illisible ({str(exc)[:120]}), nouvel essai…")
                continue
            history.append(current)
            del history[:-12]
            tail = response_tail(current, anchor)
            if tail is None:
                continue  # écho pas encore rendu : on attend, jamais de sortie ici
            elapsed = time.monotonic() - sent_at
            tails.append(tail)
            del tails[:-12]
            rounds = max(2, int(stable_s / poll_s))
            if response_finished(tail) and len(tails) >= 2 and tails[-1] == tails[-2] and tail.strip():
                settled = True
                break
            if elapsed >= min_response_s and is_stable(tails, stable_rounds=rounds):
                settled = True
                break
        if not settled:
            raise OSError(f"{E_TIMEOUT}: réponse non stabilisée en {timeout_s} s")
        result = conversation_diff(baseline, current)
        if not result:
            raise OSError(f"{E_TIMEOUT}: aucune réponse détectée")
        (out_dir / "last_message.txt").write_text(result, encoding="utf-8")
        emit("result", result)
        print(f"SUMMARY:{result[:4000]}", flush=True)
        return result
    finally:
        lock.release()


def do_self_test() -> dict:
    """Contrôle sans Desktop : verrou + powershell + UIA + parsing + heuristiques."""
    checks: dict[str, str] = {}
    if sys.platform != "win32":
        return {"platform": f"SKIP ({sys.platform}, pas de UIA)"}
    try:
        lock = UiLock(lock_path(), timeout_s=5)
        lock.acquire()
        lock.release()
        checks["lock"] = "OK"
    except (OSError, TimeoutError) as exc:
        checks["lock"] = f"FAIL: {exc}"
    try:
        run_powershell("Write-Output 'ps_ok'", timeout_s=30)
        checks["powershell"] = "OK"
    except (OSError, subprocess.SubprocessError) as exc:
        checks["powershell"] = f"FAIL: {str(exc)[:200]}"
    try:
        run_powershell("Add-Type -AssemblyName UIAutomationClient; Write-Output 'uia_ok'", timeout_s=60)
        checks["uia_assembly"] = "OK"
    except (OSError, subprocess.SubprocessError) as exc:
        checks["uia_assembly"] = f"FAIL: {str(exc)[:200]}"
    # Heuristiques pures sur fixture (indépendantes de toute UI réelle).
    fixture = [
        {"ct": "ControlType.Text", "name": "Caroline · Pro", "aid": "", "text": "Caroline · Pro"},
        {"ct": "ControlType.Edit", "name": "", "aid": "chat-input", "text": ""},
        {"ct": "ControlType.Button", "name": "Send", "aid": "send-btn", "text": "Send"},
    ]
    checks["profile_fixture"] = "OK" if profile_in_texts([e["text"] for e in fixture], "Caroline") else "FAIL"
    edit, send = find_input_and_send(fixture)
    checks["heuristics_fixture"] = (
        "OK" if (edit or {}).get("aid") == "chat-input" and (send or {}).get("aid") == "send-btn" else "FAIL"
    )
    checks["diff_fixture"] = "OK" if conversation_diff("bonjour", "bonjour\nvoici la réponse") == "voici la réponse" else "FAIL"
    return checks


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="claude_desktop_bridge")
    ap.add_argument("--prompt-file", type=Path)
    ap.add_argument("--out-dir", type=Path)
    ap.add_argument("--profile", default=DEFAULT_PROFILE)
    ap.add_argument("--timeout-s", type=int, default=3000)
    ap.add_argument("--poll-s", type=float, default=2.0)
    ap.add_argument("--stable-s", type=float, default=10.0)
    ap.add_argument("--lock-timeout-s", type=float, default=300.0)
    ap.add_argument("--min-response-s", type=float, default=25.0)
    ap.add_argument("--verify-only", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--inspect-dump", type=Path, help="vérifie + écrit l'arbre UIA (elements.json), sans envoyer")
    args = ap.parse_args(argv)

    if args.self_test:
        for k, v in do_self_test().items():
            emit("activity", f"self-test {k}: {v}")
        failed = [v for v in do_self_test().values() if v.startswith("FAIL")]
        return 1 if failed else 0

    if args.verify_only:
        try:
            hwnd, title, info = do_verify(args.profile)
        except (OSError, ValueError) as exc:
            return fail(str(exc).split(":")[0] if str(exc).startswith("profile_mismatch") else "verify_failed", str(exc))
        emit("activity", f"verify OK: hwnd={hwnd} title={title!r} version={info['version']}")
        return 0

    if args.inspect_dump:
        try:
            hwnd, _title, _info = do_verify(args.profile)
            bring_to_front(hwnd)
            time.sleep(0.6)
            elements = inspect_uia(hwnd)
            args.inspect_dump.mkdir(parents=True, exist_ok=True)
            (args.inspect_dump / "elements.json").write_text(json.dumps(elements, ensure_ascii=False, indent=1), encoding="utf-8")
            edit, send = find_input_and_send(elements)
            emit(
                "activity",
                f"inspect-dump OK: {len(elements)} éléments, "
                f"edit={((edit or {}).get('aid') or (edit or {}).get('name'))!r}, "
                f"send={((send or {}).get('aid') or (send or {}).get('name'))!r}",
            )
        except (OSError, ValueError) as exc:
            return fail("inspect_failed", str(exc))
        return 0

    if not args.prompt_file or not args.out_dir:
        ap.error("--prompt-file et --out-dir sont requis (ou --verify-only / --self-test / --inspect-dump)")
    try:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        do_drive(
            args.prompt_file,
            args.out_dir,
            args.profile,
            args.timeout_s,
            args.poll_s,
            args.stable_s,
            args.lock_timeout_s,
            args.min_response_s,
        )
    except (OSError, ValueError) as exc:
        msg = str(exc)
        code = (
            msg.split(":")[0]
            if ":" in msg
            and msg.split(":")[0]
            in (
                E_NOT_WINDOWS,
                E_NO_PACKAGE,
                E_NOT_RUNNING,
                E_NO_WINDOW,
                E_PROFILE,
                E_HUNG,
                E_UIA,
                E_INPUT,
                E_BUSY,
                E_TIMEOUT,
                E_SEND,
            )
            else "bridge_failed"
        )
        return fail(code, msg, args.out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
