from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock

import pytest

from nominal.experimental.analyst import Skill
from nominal.protos.skills.v1 import skills_pb2


@pytest.mark.parametrize("instructions", ["", "Review this run.", "Check ΔT > 5 °C.\nPreserve this line."])
def test_skill_prompt_encodes_identity_without_reading_content(instructions: str) -> None:
    """Selection carries the record identity and unchanged instructions without fetching skill content."""
    clients = MagicMock()
    rid = "ri.file-store.main.skill.0b8f6c1e-0d3c-4c43-9a55-6f0d3c1e2b7a"
    skill = Skill._from_proto(clients, skills_pb2.Skill(skill_rid=rid, workspace_rid="workspace"))
    selection, text = skill.as_prompt(instructions).split("</SKILL>\n\n", maxsplit=1)
    ref = json.loads(selection.removeprefix("<SKILL>"))["skillRef"]
    assert ref.startswith("skill:v2:")
    encoded = ref.removeprefix("skill:v2:")
    assert json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))) == {
        "skillRid": rid,
        "source": "record",
        "v": 2,
    }
    assert text == instructions
    assert clients.mock_calls == []
