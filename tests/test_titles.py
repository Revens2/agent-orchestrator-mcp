"""Titres de conversations : stables, sans secrets, sans migration."""

import orch_protocol as P
from orch_runner.adapters import OpenCode


def test_display_title_first_objective():
    assert P.display_title("Corriger le bug de facturation\nDétails...", "fb") == "Corriger le bug de facturation"
    assert P.display_title("# Titre markdown\nsuite", "fb") == "Titre markdown"
    assert P.display_title("> citation\nVrai objectif", "fb") == "citation"


def test_display_title_truncation():
    long = " ".join(f"mot{i}" for i in range(30))
    t = P.display_title(long, "fb")
    assert len(t.split()) == P.MAX_TITLE_WORDS and len(t) <= P.MAX_TITLE_CHARS + 1
    very_long_word = "a" * 200
    assert P.display_title(very_long_word, "fb").endswith("…")


def test_display_title_fallbacks():
    assert P.display_title("", "fb") == "fb"
    assert P.display_title(None, "fb") == "fb"
    assert P.display_title("   \n  \n", "fb") == "fb"
    assert P.display_title("Bearer abcdefghijklmnop", "fb") == "fb"  # secret => fallback
    assert P.display_title("sk-ant-" + "x" * 40, "fb") == "fb"
    assert P.display_title(123, "fb") == "fb"


def test_display_title_special_chars_kept_safe():
    t = P.display_title("Déployer l'app (prod) — vérif' \"quotes\" & accents éè", "fb")
    assert "Déployer" in t and len(t) <= P.MAX_TITLE_CHARS + 1


def test_display_title_stable_and_no_ids():
    t1 = P.display_title("Analyser les logs du broker", "fb")
    t2 = P.display_title("Analyser les logs du broker", "fb")
    assert t1 == t2  # jamais de renommage en boucle


def test_opencode_adapter_sets_native_title(tmp_path):
    import tempfile

    a = OpenCode("C:\\opencode.exe", {"model": "m"})
    with tempfile.TemporaryDirectory() as tmp:
        from pathlib import Path

        launch = a.build("Corriger le bug de facturation\nDétails", "read_only", "C:\\w", Path(tmp))
    assert "--title" in launch.argv
    i = launch.argv.index("--title")
    assert launch.argv[i + 1] == "Corriger le bug de facturation"
    # titre avec secret => fallback, jamais de fuite en argv
    with tempfile.TemporaryDirectory() as tmp:
        from pathlib import Path

        launch2 = a.build("voici sk-ant-" + "y" * 40, "read_only", "C:\\w", Path(tmp))
    assert launch2.argv[launch2.argv.index("--title") + 1] == "session opencode"


def test_job_and_mission_views_expose_display_title(tmp_path):
    from orch_mcp.store import Store

    s = Store(tmp_path / "t.db")
    s.hello("pc", {"runtimes": [{"id": "fake", "available": True}],
                   "workspaces": [{"id": "demo", "modes": ["read_only"], "description": "d"}]}, [])
    job, _ = s.create_job("pc", "fake", "demo", "Analyser les logs du broker\ndétails", "read_only")
    assert s.get_job(job["job_id"])["display_title"] == "Analyser les logs du broker"
    assert s.list_jobs()[0]["display_title"] == "Analyser les logs du broker"
    m = s.create_mission("Refondre la doc", ["ok"], runner_id="pc", runtime="fake", workspace_id="demo")
    assert s.get_mission(m["mission_id"])["display_title"] == "Refondre la doc"
