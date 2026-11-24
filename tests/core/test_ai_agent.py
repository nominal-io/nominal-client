import pytest

from nominal.core._utils.ai_agent import ai_agent_header_value, detect_ai_agent


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        ({}, None),
        ({"AI_AGENT": "claude-code_2-1-326_agent"}, "claude-code_2-1-326_agent"),
        ({"AI_AGENT": "devin@1", "CLAUDECODE": "1"}, "devin@1"),
        ({"AI_AGENT": "0", "CLAUDECODE": "1"}, None),
        ({"AI_AGENT": "  ", "CLAUDECODE": "1"}, "claude-code"),
        ({"CODEX_THREAD_ID": "019a"}, "codex"),
        ({"KILO_PID": "4242", "OPENCODE": "1", "AGENT": "1"}, "kilo-code"),
        ({"AGENT": "goose"}, "goose"),
        ({"AGENT": "1"}, "unknown"),
        ({"AGENT": "false"}, None),
        ({"CLAUDECODE": ""}, None),
        ({"CURSOR_AGENT": "1"}, "cursor"),
        ({"CURSOR_TRACE_ID": "abc", "TERM_PROGRAM": "vscode"}, None),
    ],
)
def test_detect_ai_agent(environ: dict[str, str], expected: str | None) -> None:
    """AI_AGENT wins, then an agent's own variable, then AGENT; None when nothing marks the process."""
    assert detect_ai_agent(environ) == expected


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        ({}, "none"),
        ({"AI_AGENT": "0", "CLAUDECODE": "1"}, "none"),
        ({"CLAUDECODE": "1"}, "claude-code"),
        ({"AI_AGENT": "devin@1"}, "devin_1"),
        ({"AI_AGENT": "my agent (v2)/x" + "a" * 100}, ("my_agent__v2__x" + "a" * 100)[:64]),
    ],
)
def test_ai_agent_header_value(environ: dict[str, str], expected: str) -> None:
    """The header carries a single bounded token, and "none" when no agent was found."""
    assert ai_agent_header_value(environ) == expected
