import pytest

from nominal.core._utils.api_tools import construct_user_agent_string, detect_ai_agent


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        ({}, "none"),
        ({"AI_AGENT": "claude-code_2-1-326_agent"}, "claude-code_2-1-326_agent"),
        ({"AI_AGENT": "devin@1", "CLAUDECODE": "1"}, "devin_1"),
        ({"AI_AGENT": "0", "CLAUDECODE": "1"}, "none"),
        ({"AI_AGENT": "  ", "CLAUDECODE": "1"}, "claude-code"),
        ({"CODEX_THREAD_ID": "019a"}, "codex"),
        ({"KILO_PID": "4242", "OPENCODE": "1", "AGENT": "1"}, "kilo-code"),
        ({"AGENT": "goose"}, "goose"),
        ({"AGENT": "1"}, "unknown"),
        ({"AGENT": "false"}, "none"),
        ({"CLAUDECODE": ""}, "none"),
    ],
)
def test_detect_ai_agent(environ: dict[str, str], expected: str) -> None:
    """AI_AGENT wins, then an agent's own variable, then AGENT; "none" when nothing marks the process."""
    assert detect_ai_agent(environ) == expected


def test_detect_ai_agent_sanitizes_value_for_user_agent() -> None:
    """Values that would break the user-agent header are reduced to a single bounded token."""
    assert detect_ai_agent({"AI_AGENT": "my agent (v2)/x" + "a" * 100}) == ("my_agent__v2__x" + "a" * 100)[:64]


def test_user_agent_always_reports_agent_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    """The detection result is appended as a product token, including "none" when no agent was found."""
    monkeypatch.setenv("AI_AGENT", "codex")
    assert construct_user_agent_string().endswith(" ai-agent/codex")
    monkeypatch.setenv("AI_AGENT", "0")
    assert construct_user_agent_string().endswith(" ai-agent/none")
