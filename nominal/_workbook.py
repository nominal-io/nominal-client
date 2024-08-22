# not yet working - leaving here for reference

import keyring as kr
from _api.scout import NotebookService
from ._utils import create_service
from .cloud import get_base_url

from _api.scout_notebook_api import CreateNotebookRequest, ChartWithOverlays
from _api.scout_layout_api import WorkbookLayout, WorkbookLayoutV1, Panel, ChartPanel, ChartPanelV1
from _api.scout_workbookcommon_api import WorkbookContent
from _api.scout_rids_api import Version, VersionedChartRid

from _api.scout_chartdefinition_api import (
    ChartDefinition,
    ChartDefinitionMap,
    TimeSeriesChartDefinition,
    TimeSeriesChartDefinitionV1,
    TimeSeriesRow,
)

from uuid import uuid4

TOKEN = kr.get_password("Nominal API", "python-client")
notebook = create_service(NotebookService, get_base_url())

chart_rid = "ri.chart.hmm." + str(uuid4())
version = Version(1)

notebook.create(
    TOKEN,
    CreateNotebookRequest(
        title="alx notebook",
        description="",
        is_draft=False,
        state_as_json="{}",
        charts=[ChartWithOverlays(rid=chart_rid, version=version, overlays="{}")],
        run_rid="ri.scout.gov-staging.run.e326f85b-c517-4605-8241-e850541238a2",
        layout=WorkbookLayout(
            v1=WorkbookLayoutV1(
                root_panel=Panel(
                    chart=ChartPanel(
                        v1=ChartPanelV1(
                            id=str(uuid4()),
                            chart_rid=VersionedChartRid(rid=chart_rid, version=version),
                            hide_legend=True,
                        )
                    )
                )
            )
        ),
        content=WorkbookContent(
            channel_variables={
                "var_name": ComputeSpec()  # serialized json of the FE api
            },
            charts={
                chart_rid: ChartDefinition(
                    time_series=TimeSeriesChartDefinition(
                        v1=TimeSeriesChartDefinitionV1(
                            rows=[TimeSeriesRow(plots=[], row_flex_size=1.0)],
                            comparison_run_groups=[],
                            events=[],
                            value_axes=[],
                        )
                    )
                )
            },
        ),
    ),
)
