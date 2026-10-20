from __future__ import annotations

import pytest

from nominal.core import Workspace
from nominal.protos.types import types_pb2
from nominal.protos.workspaces.v1 import workspaces_pb2


@pytest.mark.parametrize("display_name", [None, "", "Flight test"])
def test_workspace_without_preferences(display_name: str | None) -> None:
    """Absent preferences are empty, while an absent display name differs from an empty one."""
    workspace = Workspace._from_proto(
        workspaces_pb2.Workspace(rid="workspace", id="test", org="org", display_name=display_name)
    )

    assert workspace.display_name == display_name
    assert workspace.preferred_refnames == ()
    assert workspace.preferred_procedure_rids == ()


def test_workspace_preferences_are_detached_from_response() -> None:
    """Preferences deduplicate names in first-seen order independently of the response."""
    raw = workspaces_pb2.Workspace(
        settings=workspaces_pb2.WorkspaceSettings(
            ref_names=workspaces_pb2.PreferredRefNameConfiguration(
                v1=[
                    types_pb2.RefNameAndType(name="telemetry", type=types_pb2.DATASET),
                    types_pb2.RefNameAndType(name="live", type=types_pb2.CONNECTION),
                    types_pb2.RefNameAndType(name="camera", type=types_pb2.VIDEO),
                    types_pb2.RefNameAndType(name="position", type=types_pb2.SPATIAL),
                    types_pb2.RefNameAndType(name="telemetry", type=types_pb2.SPATIAL),
                    types_pb2.RefNameAndType(name="unset"),
                    types_pb2.RefNameAndType(name="future", type=999),
                ]
            ),
            procedures=workspaces_pb2.ProcedureSettings(
                v1=workspaces_pb2.ProcedureSettingsV1(workspace_procedures=["second", "first"])
            ),
        )
    )
    workspace = Workspace._from_proto(raw)
    raw.ClearField("settings")

    assert workspace.preferred_refnames == ("telemetry", "live", "camera", "position", "unset", "future")
    assert workspace.preferred_procedure_rids == ("second", "first")
