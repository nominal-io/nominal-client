from __future__ import annotations

import os
import re
from typing import Mapping

# Request header carrying the detected AI coding agent, or "none".
AI_AGENT_HEADER = "X-Nominal-AI-Agent"
NO_AI_AGENT = "none"
_HEADER_VALUE_DISALLOWED = re.compile(r"[^A-Za-z0-9._+-]")

# Variables an AI coding agent sets in the commands it runs, mapped to the agent's slug. Each one is set by the agent
# itself (checked against its docs or source), never user configuration or a marker of an IDE terminal a person also
# types in. Agents with no such variable (Copilot CLI, Kiro, Antigravity, Amp) are detected only through AI_AGENT.
_AGENT_ENV_VARS: Mapping[str, str] = {
    "KILO_PID": "kilo-code",  # Kilo also sets OPENCODE, so it comes first.
    "CLAUDE_CODE_CHILD_SESSION": "claude-code",  # Unlike CLAUDECODE, not set in IDE extension terminals.
    "CODEX_THREAD_ID": "codex",
    "CODEX_SANDBOX": "codex",
    "CODEX_CI": "codex",
    "CURSOR_AGENT": "cursor",
    "GEMINI_CLI": "gemini",
    "OPENCODE": "opencode",
    "CLINE_ACTIVE": "cline",
    "ROO_ACTIVE": "roo-code",
    "PI_CODING_AGENT": "pi",
    "GOOSE_TERMINAL": "goose",
    "QWEN_CODE": "qwen-code",
    "CRUSH": "crush",
    "JUNIE_SHIM_PATH": "junie",
}
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})


def detect_ai_agent(environ: Mapping[str, str] = os.environ) -> str | None:
    """Returns the name of the AI coding agent running this process, or None when there is none.

    `AI_AGENT` (see agentsmd/agents.md#136) wins and names the agent; a false value such as `0` opts out. Otherwise a
    known agent's own variable, then the older generic `AGENT`, mark the process as agent-run. The server normalizes
    the name to an allowlist, so an approximate name here is fine.
    """
    ai_agent = environ.get("AI_AGENT", "").strip()
    if ai_agent.lower() in _FALSE_VALUES:
        return None
    if ai_agent:
        return ai_agent
    known = next((agent for var, agent in _AGENT_ENV_VARS.items() if environ.get(var)), None)
    if known:
        return known
    agent = environ.get("AGENT", "").strip()
    if not agent or agent.lower() in _FALSE_VALUES:
        return None
    return "unknown" if agent.lower() in _TRUE_VALUES else agent


def ai_agent_header_value(environ: Mapping[str, str] = os.environ) -> str:
    """Returns the detected agent as a bounded header token, or "none" when no agent was found."""
    return _HEADER_VALUE_DISALLOWED.sub("_", detect_ai_agent(environ) or "")[:64] or NO_AI_AGENT
