"""Ouverture de conversation : skill de départ et sous-agents, jusque dans l'argv.

Deux demandes, un seul mécanisme : ce que l'agent reçoit AVANT la mission.
Ces tests vérifient le bout de la chaîne (l'invocation réelle construite par
chaque adapter), pas seulement la fonction qui compose le préambule.

Discipline tenue ici : on n'envoie une commande `/skill` qu'aux runtimes qui
l'interprètent, on n'autorise les sous-agents que là où le runtime les
documente, et on ne rejoue rien sur une conversation reprise.
"""

from pathlib import Path

import pytest

import orch_protocol as P
from orch_runner.adapters import ADAPTERS

CWD = r"C:\ws\e2e"
MISSION = "corrige le bug de pagination"


def build(runtime, mode="workspace_write", resume=None, skill=P.DEFAULT_START_SKILL, subagents=True):
    adapter = ADAPTERS[runtime](r"C:\bin\agent.exe", {}, P.UNATTENDED)
    if resume:
        adapter.resume_session_id = resume
    adapter.session_preamble = P.build_session_preamble(runtime, skill, subagents, resuming=bool(resume))
    return adapter, adapter.build(MISSION, mode, CWD, Path("/tmp"))


def sent_text(launch):
    """Le texte réellement transmis à l'agent (stdin ou dernier argument)."""
    return launch.stdin_text if launch.stdin_text is not None else launch.argv[-1]


def test_claude_code_receives_the_skill_first_then_the_mission():
    _, launch = build("claude-code")
    text = sent_text(launch)
    assert text.startswith("/caveman ultra\n\n"), "la commande doit ouvrir le prompt pour être interprétée"
    assert "sous-agents" in text
    assert text.endswith(MISSION), "la mission de l'utilisateur n'est jamais modifiée"


@pytest.mark.parametrize("runtime", ["codex", "agy", "opencode"])
def test_no_slash_command_where_it_would_be_read_as_text(runtime):
    _, launch = build(runtime, mode="read_only" if runtime == "opencode" else "workspace_write")
    assert "/caveman" not in sent_text(launch)


def test_opencode_gets_subagents_but_keeps_a_title_from_the_mission():
    _, launch = build("opencode", mode="read_only")
    text = sent_text(launch)
    assert "sous-agents" in text and text.endswith(MISSION)
    title = launch.argv[launch.argv.index("--title") + 1]
    assert "orchestrateur" not in title and "pagination" in title


def test_codex_is_promised_nothing_unverified():
    _, launch = build("codex")
    assert sent_text(launch) == MISSION


def test_resumed_conversation_replays_nothing():
    _, launch = build("opencode", mode="read_only", resume="ses_42")
    assert launch.argv[launch.argv.index("--session") + 1] == "ses_42"
    assert "--title" not in launch.argv, "une session reprise n'est jamais renommée"
    assert sent_text(launch) == MISSION


def test_adapter_refuses_to_replay_a_preamble_even_if_wrongly_given_one():
    """Invariant tenu par l'adapter lui-même, pas seulement par le runner."""
    adapter = ADAPTERS["opencode"](r"C:\bin\agent.exe", {}, P.UNATTENDED)
    adapter.resume_session_id = "ses_7"
    adapter.session_preamble = ["/caveman ultra", "blabla"]
    launch = adapter.build(MISSION, "read_only", CWD, Path("/tmp"))
    assert sent_text(launch) == MISSION


def test_disabling_the_skill_disables_only_the_skill():
    _, launch = build("claude-code", skill="")
    text = sent_text(launch)
    assert "/caveman" not in text and "sous-agents" in text
    _, launch = build("claude-code", subagents=False)
    text = sent_text(launch)
    assert text.startswith("/caveman ultra") and "sous-agents" not in text
