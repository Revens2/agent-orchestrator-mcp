"""Voie patch confinée `claude-desktop` (`workspace_write`) : applier + adapter.

Le Desktop n'a aucun accès disque/shell : il propose des diffs unifiés, le
runner applique borné au workspace. Fail-closed partout : la moindre violation
(chemin, contexte, borne) annule TOUTE l'application, rien n'est écrit.
"""

import sys
from pathlib import Path

import pytest

import orch_protocol as P
from orch_runner import adapters as A
from orch_runner import desktop_patch as DP

PY = sys.executable
WS = r"C:\ws\e2e"


def _ws(tmp_path: Path, files: dict[str, str] | None = None) -> Path:
    root = tmp_path / "ws"
    root.mkdir(parents=True, exist_ok=True)
    for rel, content in (files or {}).items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8", newline="\n")
    return root


def _diff_modify() -> str:
    return (
        "```diff\n--- a/a.txt\n+++ b/a.txt\n"
        "@@ -1,3 +1,3 @@\n l1\n-l2\n+l2-mod\n l3\n```"
    )


# ------------------------------------------------------------ extraction -----
def test_extract_fenced_diff_only():
    text = "blabla\n```python\nprint('--- pas un diff')\n```\n" + _diff_modify()
    blocks = DP.extract_patches(text)
    assert len(blocks) == 1 and "l2-mod" in blocks[0]


def test_extract_empty_or_prose():
    assert DP.extract_patches("") == []
    assert DP.extract_patches("voici mon analyse, sans patch.") == []


def test_extract_raw_diff_fallback():
    raw = "--- a/f.txt\n+++ b/f.txt\n@@ -1 +1 @@\n-a\n+b\n"
    assert DP.extract_patches(raw) == [raw.strip()]


# ------------------------------------------------------------------ parse ---
def test_parse_modify_new_delete(tmp_path):
    root = _ws(tmp_path, {"a.txt": "l1\nl2\nl3\n"})
    rep, _ = DP.response_to_workspace(
        root, _diff_modify() + "\n```diff\n--- /dev/null\n+++ b/n.txt\n@@ -0,0 +1 @@\n+new\n```"
    )
    assert rep.modified == ["a.txt"] and rep.created == ["n.txt"]
    assert (root / "a.txt").read_text(encoding="utf-8") == "l1\nl2-mod\nl3\n"


def test_parse_delete(tmp_path):
    root = _ws(tmp_path, {"gone.txt": "x\n"})
    rep, _ = DP.response_to_workspace(
        root, "```diff\n--- a/gone.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-x\n```"
    )
    assert rep.deleted == ["gone.txt"] and not (root / "gone.txt").exists()


def test_rename_refused(tmp_path):
    root = _ws(tmp_path, {"a.txt": "x\n"})
    with pytest.raises(DP.PatchError) as exc:
        DP.response_to_workspace(
            root, "```diff\n--- a/a.txt\n+++ b/b.txt\n@@ -1 +1 @@\n-x\n+y\n```"
        )
    assert exc.value.code == DP.E_BAD_PATH
    assert (root / "a.txt").read_text(encoding="utf-8") == "x\n"


def test_duplicate_file_refused(tmp_path):
    root = _ws(tmp_path, {"a.txt": "l1\nl2\nl3\n"})
    with pytest.raises(DP.PatchError) as exc:
        DP.response_to_workspace(root, _diff_modify() + "\n" + _diff_modify())
    assert exc.value.code == DP.E_DUPLICATE
    assert (root / "a.txt").read_text(encoding="utf-8") == "l1\nl2\nl3\n"


# -------------------------------------------------------------- confinement -
@pytest.mark.parametrize("evil", ["../evil.txt", "a/../../evil.txt", "/abs.txt", "C:/win.txt",
                                  "\\\\srv\\x.txt", ".git/hooks/x", "a/.git/y"])
def test_evil_paths_refused_nothing_written(tmp_path, evil):
    root = _ws(tmp_path, {"ok.txt": "v1\n"})
    quote = evil.replace("\\", "/")
    blob = (
        "```diff\n--- a/ok.txt\n+++ b/ok.txt\n@@ -1 +1 @@\n-v1\n+v2\n```\n"
        f"```diff\n--- a/{quote}\n+++ b/{quote}\n@@ -0,0 +1 @@\n+x\n```"
    )
    with pytest.raises(DP.PatchError):
        DP.response_to_workspace(root, blob)
    # Fail-closed : le fichier légitime du même lot est intact.
    assert (root / "ok.txt").read_text(encoding="utf-8") == "v1\n"
    assert not (root / "evil.txt").exists()


def test_context_mismatch_keeps_file_intact(tmp_path):
    root = _ws(tmp_path, {"a.txt": "l1\nl2\nl3\n"})
    with pytest.raises(DP.PatchError) as exc:
        DP.response_to_workspace(
            root, "```diff\n--- a/a.txt\n+++ b/a.txt\n@@ -1,3 +1,3 @@\n l1\n-WRONG\n+x\n l3\n```"
        )
    assert exc.value.code == DP.E_CONTEXT
    assert (root / "a.txt").read_text(encoding="utf-8") == "l1\nl2\nl3\n"


def test_no_patch_raises(tmp_path):
    root = _ws(tmp_path)
    with pytest.raises(DP.PatchError) as exc:
        DP.response_to_workspace(root, "analyse sans patch.")
    assert exc.value.code == DP.E_NO_PATCH


def test_create_existing_refused(tmp_path):
    root = _ws(tmp_path, {"n.txt": "already\n"})
    with pytest.raises(DP.PatchError) as exc:
        DP.response_to_workspace(
            root, "```diff\n--- /dev/null\n+++ b/n.txt\n@@ -0,0 +1 @@\n+new\n```"
        )
    assert exc.value.code == DP.E_CONTEXT
    assert (root / "n.txt").read_text(encoding="utf-8") == "already\n"


# ----------------------------------------------------------------- adapter --
def test_build_workspace_write_frames_prompt_file(tmp_path):
    ws = _ws(tmp_path, {"a.txt": "l1\nl2\nl3\n"})
    job = tmp_path / "job"
    job.mkdir()
    hostile = '"; powershell -c calc `$x` %PATH% > NUL'
    ad = A.ClaudeDesktop(PY, {"profile": "Caroline"})
    launch = ad.build("Modifier a.txt. " + hostile, "workspace_write", str(ws), job)
    assert launch.stdin_text is None
    assert hostile not in " ".join(launch.argv)
    assert ad._patch_mode is True and ad._workspace_cwd == str(ws)
    framed = (job / "prompt.txt").read_text(encoding="utf-8")
    assert "```diff" in framed and hostile in framed and str(ws) in framed
    assert "--prompt-file" in launch.argv and "Caroline" in launch.argv


def test_build_read_only_has_no_framing(tmp_path):
    job = tmp_path / "job"
    job.mkdir()
    ad = A.ClaudeDesktop(PY, {"profile": "Caroline"})
    ad.build("Bonjour.", "read_only", WS, job)
    assert ad._patch_mode is False
    assert (job / "prompt.txt").read_text(encoding="utf-8") == "Bonjour."


def test_finish_workspace_write_applies_patch(tmp_path):
    ws = _ws(tmp_path, {"a.txt": "l1\nl2\nl3\n"})
    job = tmp_path / "job"
    job.mkdir()
    ad = A.ClaudeDesktop(PY, None)
    ad.build("patch a.txt", "workspace_write", str(ws), job)
    (job / "last_message.txt").write_text("voici :\n" + _diff_modify(), encoding="utf-8")
    out = ad.finish(0, job)
    assert out.ok and "a.txt" in (out.summary or "")
    assert (ws / "a.txt").read_text(encoding="utf-8") == "l1\nl2-mod\nl3\n"


def test_finish_workspace_write_no_patch_fails_clean(tmp_path):
    ws = _ws(tmp_path, {"a.txt": "v1\n"})
    job = tmp_path / "job"
    job.mkdir()
    ad = A.ClaudeDesktop(PY, None)
    ad.build("analyse", "workspace_write", str(ws), job)
    (job / "last_message.txt").write_text("simple analyse, rien à changer.", encoding="utf-8")
    out = ad.finish(0, job)
    assert not out.ok and out.error.startswith("no_patch")
    assert (ws / "a.txt").read_text(encoding="utf-8") == "v1\n"


def test_finish_workspace_write_evil_blocked(tmp_path):
    ws = _ws(tmp_path, {"ok.txt": "v1\n"})
    job = tmp_path / "job"
    job.mkdir()
    ad = A.ClaudeDesktop(PY, None)
    ad.build("patch", "workspace_write", str(ws), job)
    (job / "last_message.txt").write_text(
        "```diff\n--- a/../evil.txt\n+++ b/../evil.txt\n@@ -0,0 +1 @@\n+x\n```", encoding="utf-8"
    )
    out = ad.finish(0, job)
    assert not out.ok and out.error.startswith("path_escape")
    assert not (tmp_path / "evil.txt").exists()


def test_finish_workspace_write_bridge_failure_applies_nothing(tmp_path):
    ws = _ws(tmp_path, {"a.txt": "l1\nl2\nl3\n"})
    job = tmp_path / "job"
    job.mkdir()
    ad = A.ClaudeDesktop(PY, None)
    ad.build("patch", "workspace_write", str(ws), job)
    (job / "last_message.txt").write_text(_diff_modify(), encoding="utf-8")
    out = ad.finish(1, job)
    assert not out.ok and "non appliqués" in (out.error or "")
    assert (ws / "a.txt").read_text(encoding="utf-8") == "l1\nl2\nl3\n"


def test_protocol_and_modes_cover_patch():
    assert P.RUNTIME_MODES["claude-desktop"] == ("read_only", "workspace_write")
    assert A.ClaudeDesktop(PY, None).modes == ("read_only", "workspace_write")
