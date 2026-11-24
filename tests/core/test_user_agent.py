import pytest

from nominal.core._utils.api_tools import _user_agent_token, construct_user_agent_string


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "none"),
        ("", "none"),
        ("claude-code", "claude-code"),
        ("devin@1", "devin_1"),
        ("my agent (v2)/x" + "a" * 100, ("my_agent__v2__x" + "a" * 100)[:64]),
    ],
)
def test_user_agent_token(value: str | None, expected: str) -> None:
    """Values that would break the user-agent header are reduced to a single bounded token."""
    assert _user_agent_token(value) == expected


def test_user_agent_always_reports_agent_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    """The detection result is appended as a product token, including "none" when no agent was found."""
    monkeypatch.setenv("AI_AGENT", "codex")
    assert construct_user_agent_string().endswith(" ai-agent/codex")
    monkeypatch.setenv("AI_AGENT", "0")
    assert construct_user_agent_string().endswith(" ai-agent/none")
