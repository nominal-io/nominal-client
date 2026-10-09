"""Legacy video bindings must follow the target scope when instantiating a template."""

from unittest.mock import MagicMock, patch

import pytest
from conjure_python_client import ConjureEncoder
from nominal_api import scout_chartdefinition_api, scout_notebook_api, scout_workbookcommon_api

from nominal.core.workbook import Workbook, WorkbookType
from nominal.core.workbook_template import WorkbookTemplate

_SOURCE_ASSET = "ri.scout.test.asset.source"
_SOURCE_RUN = "ri.scout.test.run.source"
_TARGET_ASSET = "ri.scout.test.asset.target"
_TARGET_RUN = "ri.scout.test.run.target"


def _instantiate(
    content: scout_workbookcommon_api.WorkbookContent, *, asset: str | None = None, run_assets: tuple[str, ...] = ()
) -> tuple[scout_notebook_api.CreateNotebookRequest, MagicMock]:
    clients = MagicMock()
    clients.template.get.return_value.content = content
    clients.run.get_run.return_value.assets = list(run_assets)
    template = WorkbookTemplate("template", "Template", "", [], {}, WorkbookType.WORKBOOK, clients)
    with patch.object(Workbook, "_from_conjure"):
        template.create_workbook(asset=asset, run=None if asset is not None else _TARGET_RUN)
    return clients.notebook.create.call_args.args[1], clients


@pytest.mark.parametrize(
    ("asset", "run_assets", "video_asset", "video_run"),
    [
        (None, (), None, None),
        (None, (_TARGET_ASSET,), _TARGET_ASSET, _TARGET_RUN),
        (_TARGET_ASSET, (), _TARGET_ASSET, None),
    ],
    ids=["run-without-assets", "run-with-asset", "asset"],
)
def test_legacy_video_uses_only_the_target_scope(
    asset: str | None, run_assets: tuple[str, ...], video_asset: str | None, video_run: str | None
) -> None:
    """Old templates must never retain source video bindings, even when the target has no assets."""
    video = scout_chartdefinition_api.VizDefinition(
        video=scout_chartdefinition_api.VideoVizDefinition(
            v1=scout_chartdefinition_api.VideoVizDefinitionV1(
                comparison_run_groups=[],
                datasource=scout_chartdefinition_api.VideoPanelDataSource(
                    asset_rid=_SOURCE_ASSET, run_rid=_SOURCE_RUN, ref_name="camera"
                ),
            )
        )
    )
    content = scout_workbookcommon_api.WorkbookContent(
        channel_variables={},
        charts={"video": video},
        data_scope_inputs={},
        inputs={},
        report_content="report",
        settings=scout_workbookcommon_api.WorkbookSettings(),
        time_range_inputs={},
        version="20.0.0",
    )
    before = ConjureEncoder.do_encode(content)
    request, clients = _instantiate(content, asset=asset, run_assets=run_assets)
    result = request.content_v2.workbook
    assert result is not None
    result_video = result.charts["video"].video
    assert result_video is not None and result_video.v1 is not None
    if video_asset is None:
        assert result_video.v1.datasource is None
        assert result_video.v1.ref_name == "camera"
    else:
        datasource = result_video.v1.datasource
        assert datasource is not None
        assert (datasource.asset_rid, datasource.run_rid, datasource.ref_name) == (video_asset, video_run, "camera")
    assert request.data_scope.asset_rids == ([asset] if asset is not None else None)
    assert request.data_scope.run_rids == (None if asset is not None else [_TARGET_RUN])
    after = ConjureEncoder.do_encode(result)
    assert {key: value for key, value in after.items() if key != "charts"} == {
        key: value for key, value in before.items() if key != "charts"
    }
    assert ConjureEncoder.do_encode(content) == before
    if asset is not None:
        clients.run.get_run.assert_not_called()
    else:
        clients.run.get_run.assert_called_once_with(clients.auth_header, _TARGET_RUN)


def test_channel_video_keeps_content_and_skips_legacy_asset_lookup() -> None:
    """Channel-based videos do not need the legacy datasource rewrite."""
    content = scout_workbookcommon_api.WorkbookContent(
        channel_variables={},
        charts={
            "video": scout_chartdefinition_api.VizDefinition(
                video=scout_chartdefinition_api.VideoVizDefinition(
                    v2=scout_chartdefinition_api.VideoVizDefinitionV2(variable="camera")
                )
            )
        },
    )
    request, clients = _instantiate(content)
    assert request.content_v2.workbook is content
    clients.run.get_run.assert_not_called()
