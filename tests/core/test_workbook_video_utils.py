"""Unit tests for video panel datasource stripping and re-binding utilities."""

from __future__ import annotations

from nominal_api import scout_chartdefinition_api, scout_workbookcommon_api

from nominal.core.workbook import _strip_video_datasources
from nominal.core.workbook_template import _rebind_video_datasources

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ASSET_RID = "ri.scout.cerulean-staging.asset.abc123"
_RUN_RID = "ri.scout.cerulean-staging.run.def456"
_NEW_ASSET_RID = "ri.scout.cerulean-staging.asset.newasset"
_NEW_RUN_RID = "ri.scout.cerulean-staging.run.newrun"


def _make_video_viz(
    asset_rid: str | None = _ASSET_RID,
    run_rid: str | None = _RUN_RID,
    ref_name: str = "camera",
    panel_ref_name: str | None = None,
) -> scout_chartdefinition_api.VizDefinition:
    datasource = (
        scout_chartdefinition_api.VideoPanelDataSource(
            asset_rid=asset_rid,  # type: ignore[arg-type]  # intentionally None in some tests
            ref_name=ref_name,
            run_rid=run_rid,
        )
        if asset_rid is not None
        else None
    )
    v1 = scout_chartdefinition_api.VideoVizDefinitionV1(
        comparison_run_groups=[],
        datasource=datasource,
        ref_name=panel_ref_name,
        title="Video",
    )
    return scout_chartdefinition_api.VizDefinition(video=scout_chartdefinition_api.VideoVizDefinition(v1=v1))


def _make_markdown_viz() -> scout_chartdefinition_api.VizDefinition:
    return scout_chartdefinition_api.VizDefinition(
        markdown=scout_chartdefinition_api.MarkdownPanelDefinition(
            v1=scout_chartdefinition_api.MarkdownPanelDefinitionV1(content="# hello")
        )
    )


def _make_content(
    charts: dict[str, scout_chartdefinition_api.VizDefinition],
) -> scout_workbookcommon_api.WorkbookContent:
    return scout_workbookcommon_api.WorkbookContent(channel_variables={}, charts=charts)


# ---------------------------------------------------------------------------
# _strip_video_datasources
# ---------------------------------------------------------------------------


class TestStripVideoDatasources:
    def test_strips_asset_and_run_rid_from_datasource(self) -> None:
        content = _make_content({"vid": _make_video_viz()})
        result = _strip_video_datasources(content)
        v1 = result.charts["vid"].video.v1  # type: ignore[union-attr]
        assert v1.datasource is None

    def test_preserves_ref_name_at_panel_level(self) -> None:
        content = _make_content({"vid": _make_video_viz(ref_name="front-camera")})
        result = _strip_video_datasources(content)
        v1 = result.charts["vid"].video.v1  # type: ignore[union-attr]
        assert v1.ref_name == "front-camera"

    def test_datasource_ref_name_takes_precedence_over_panel_ref_name(self) -> None:
        content = _make_content({"vid": _make_video_viz(ref_name="ds-ref", panel_ref_name="existing")})
        result = _strip_video_datasources(content)
        v1 = result.charts["vid"].video.v1  # type: ignore[union-attr]
        # datasource.ref_name wins; it becomes the new panel-level ref_name
        assert v1.ref_name == "ds-ref"

    def test_returns_same_object_when_no_video_charts(self) -> None:
        content = _make_content({"md": _make_markdown_viz()})
        result = _strip_video_datasources(content)
        assert result is content

    def test_non_video_charts_pass_through_unchanged(self) -> None:
        content = _make_content({"md": _make_markdown_viz(), "vid": _make_video_viz()})
        result = _strip_video_datasources(content)
        assert result.charts["md"] is content.charts["md"]

    def test_video_without_datasource_passes_through_unchanged(self) -> None:
        content = _make_content({"vid": _make_video_viz(asset_rid=None)})
        result = _strip_video_datasources(content)
        assert result is content

    def test_does_not_mutate_original_content(self) -> None:
        viz = _make_video_viz()
        content = _make_content({"vid": viz})
        _strip_video_datasources(content)
        # original datasource is untouched
        assert content.charts["vid"].video.v1.datasource is not None  # type: ignore[union-attr]
        assert content.charts["vid"].video.v1.datasource.asset_rid == _ASSET_RID  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# _rebind_video_datasources
# ---------------------------------------------------------------------------


class TestRebindVideoDatasources:
    def test_sets_asset_rid_on_video_panel(self) -> None:
        content = _make_content({"vid": _make_video_viz(asset_rid=None)})
        result = _rebind_video_datasources(content, _NEW_ASSET_RID, None)
        datasource = result.charts["vid"].video.v1.datasource  # type: ignore[union-attr]
        assert datasource is not None
        assert datasource.asset_rid == _NEW_ASSET_RID

    def test_sets_run_rid_on_video_panel(self) -> None:
        content = _make_content({"vid": _make_video_viz(asset_rid=None)})
        result = _rebind_video_datasources(content, _NEW_ASSET_RID, _NEW_RUN_RID)
        datasource = result.charts["vid"].video.v1.datasource  # type: ignore[union-attr]
        assert datasource is not None
        assert datasource.run_rid == _NEW_RUN_RID

    def test_run_rid_is_none_when_not_provided(self) -> None:
        content = _make_content({"vid": _make_video_viz(asset_rid=None)})
        result = _rebind_video_datasources(content, _NEW_ASSET_RID, None)
        datasource = result.charts["vid"].video.v1.datasource  # type: ignore[union-attr]
        assert datasource is not None
        assert datasource.run_rid is None

    def test_preserves_ref_name_from_panel_level(self) -> None:
        content = _make_content({"vid": _make_video_viz(asset_rid=None, panel_ref_name="side-camera")})
        result = _rebind_video_datasources(content, _NEW_ASSET_RID, None)
        datasource = result.charts["vid"].video.v1.datasource  # type: ignore[union-attr]
        assert datasource is not None
        assert datasource.ref_name == "side-camera"

    def test_falls_back_to_default_ref_name(self) -> None:
        content = _make_content({"vid": _make_video_viz(asset_rid=None, panel_ref_name=None)})
        result = _rebind_video_datasources(content, _NEW_ASSET_RID, None)
        datasource = result.charts["vid"].video.v1.datasource  # type: ignore[union-attr]
        assert datasource is not None
        assert datasource.ref_name == "default"

    def test_non_video_charts_pass_through_unchanged(self) -> None:
        content = _make_content({"md": _make_markdown_viz(), "vid": _make_video_viz(asset_rid=None)})
        result = _rebind_video_datasources(content, _NEW_ASSET_RID, None)
        assert result.charts["md"] is content.charts["md"]

    def test_replaces_existing_datasource_rids(self) -> None:
        content = _make_content({"vid": _make_video_viz(asset_rid=_ASSET_RID, run_rid=_RUN_RID)})
        result = _rebind_video_datasources(content, _NEW_ASSET_RID, _NEW_RUN_RID)
        datasource = result.charts["vid"].video.v1.datasource  # type: ignore[union-attr]
        assert datasource is not None
        assert datasource.asset_rid == _NEW_ASSET_RID
        assert datasource.run_rid == _NEW_RUN_RID
