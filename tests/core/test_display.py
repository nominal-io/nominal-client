from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, NonCallableMock

import pytest
from nominal_api import api, datasource, scout_catalog, scout_compute_api

from nominal.core import (
    Asset,
    Attachment,
    Bounds,
    Channel,
    ChannelDataType,
    Dataset,
    NominalClient,
    Run,
    _display,
    get_display_options,
    set_display_options,
)
from nominal.core.attachment import _attachments_section
from nominal.exceptions import NominalConfigError
from nominal.protos.run.v1 import run_service_pb2

START = 1_767_225_600_000_000_000  # 2026-01-01T00:00:00Z
SECOND = api.Timestamp(seconds=START // 1_000_000_000, nanos=0)


def _offline_clients() -> NonCallableMock:
    """Clients that fail on any attribute access, so a test fails if displaying makes a request."""
    return NonCallableMock(spec=[])


def _linked_clients() -> MagicMock:
    clients = MagicMock()
    clients.app_base_url = "https://app.example.com"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.1"
    return clients


def _dataset(clients: object, **overrides: object) -> Dataset:
    fields: dict[str, object] = dict(
        rid="ri.catalog.dataset.1",
        name="Rotor telemetry",
        description=None,
        properties={},
        labels=[],
        bounds=Bounds(start=START, end=START + 90_000_000_000),
        is_archived=False,
        _clients=clients,
    )
    fields.update(overrides)
    return Dataset(**fields)  # type: ignore[arg-type]


def _attachment(clients: object, rid: str, name: str) -> Attachment:
    return Attachment(
        rid=rid,
        name=name,
        description="",
        properties={},
        labels=["touchstone"],
        created_at=START,
        is_archived=False,
        _clients=clients,  # type: ignore[arg-type]
    )


def _run(clients: object, **overrides: object) -> Run:
    fields: dict[str, object] = dict(
        rid="ri.scout.run.1",
        name="Spin-up",
        description="",
        properties={},
        labels=[],
        links=[],
        start=START,
        end=START + 60_000_000_000,
        run_number=7,
        assets=["ri.scout.asset.1"],
        created_at=START,
        is_archived=False,
        _clients=clients,
    )
    fields.update(overrides)
    return Run(**fields)  # type: ignore[arg-type]


def test_cards_make_no_requests_with_links_and_loading_off():
    """With links and loading off, every card renders from the object's own fields."""
    clients = _offline_clients()
    asset = Asset(
        rid="ri.scout.asset.1",
        name="Helicopter",
        description=None,
        properties={"tail_number": "N123HX"},
        labels=["fleet"],
        created_at=START,
        is_archived=False,
        _clients=clients,
    )
    channel = Channel("rotor_speed", "ri.catalog.dataset.1", ChannelDataType.DOUBLE, "rad/s", None, clients)
    attachment = _attachment(clients, "ri.attachments.attachment.1", "amplifier.s2p")
    resources = (
        _dataset(clients),
        _dataset(clients, bounds=None),
        asset,
        _run(clients, end=None),
        channel,
        attachment,
    )
    with set_display_options(show_link=False, fetch_details=False):
        for resource in resources:
            card = resource._repr_html_()
            assert card is not None and resource.name in card


def test_run_links_its_assets_without_loading_them():
    """With loading off, a run still links the asset RIDs it holds."""
    with set_display_options(fetch_details=False):
        card = _run(_linked_clients())._repr_html_()
    assert card is not None
    assert 'href="https://app.example.com/w/ri.workspace.1/assets/ri.scout.asset.1"' in card


def test_identifiers_link_and_copy():
    """An ID links to its page and has a button that copies the raw value."""
    html = _display.identifier("ri.catalog.dataset.1", lambda: "https://app.example.com/datasets/1")
    assert 'href="https://app.example.com/datasets/1"' in html
    assert 'data-copy="ri.catalog.dataset.1"' in html


def test_card_renders_without_links_when_workspace_cannot_resolve():
    """A workspace error leaves out the links rather than failing the display."""
    clients = _linked_clients()
    clients.resolve_default_workspace_rid.side_effect = NominalConfigError("no default workspace")
    with set_display_options(fetch_details=False):
        card = _run(clients)._repr_html_()
    assert card is not None and "<a " not in card and "Spin-up" in card


def test_failed_section_becomes_a_note():
    """A related section that fails to load says so, so the rest of the card still displays."""

    def load() -> _display.Section:
        raise RuntimeError("service unavailable\nstack details")

    loading = _display.fetched("Runs", load)
    assert loading is not None
    assert loading.result().note == "Couldn't load: RuntimeError service unavailable"


def test_slow_section_stops_the_card_waiting_at_its_deadline():
    """A section still loading at the deadline says so, and the card displays without it."""
    release = threading.Event()

    def slow() -> _display.Section:
        release.wait(10)
        return _display.Section("Runs", [["late"]])

    @_display.card
    def build(_: object) -> str | None:
        return _display.render(
            kind="Test",
            title="",
            fallback="",
            fields=[],
            sections=[
                _display.fetched("Runs", slow),
                _display.fetched("Quick", lambda: _display.Section("Quick", [["ok"]])),
            ],
        )

    try:
        with set_display_options(show_link=False, fetch_timeout=0.2):
            started = time.monotonic()
            card = build(None)
            elapsed = time.monotonic() - started
    finally:
        release.set()
    assert card is not None and "load within 0.2 s." in card and "late" not in card
    assert "<td>ok</td>" in card
    assert elapsed < 2


def test_slow_workspace_leaves_links_out_at_the_deadline():
    """A workspace that doesn't resolve in time shows the card's names and IDs without links."""
    release = threading.Event()
    clients = _linked_clients()
    clients.resolve_default_workspace_rid.side_effect = lambda: release.wait(10) and "ri.workspace.1"
    try:
        with set_display_options(fetch_details=False, fetch_timeout=0.2):
            started = time.monotonic()
            card = _run(clients)._repr_html_()
            elapsed = time.monotonic() - started
    finally:
        release.set()
    assert card is not None and "<a " not in card and "ri.scout.asset.1" in card
    assert elapsed < 2


def test_card_that_fails_to_build_falls_back_to_repr():
    """An error while building a card returns None, so the notebook shows the plain repr, not a traceback."""
    with set_display_options(show_link=False, fetch_details=False):
        card = _run(_offline_clients(), start="not a timestamp")._repr_html_()
    assert card is None


def test_asset_runs_stop_loading_past_max_items():
    """An asset card asks for runs newest first and loads only the pages it lists."""
    clients = _linked_clients()
    page = [run_service_pb2.Run(rid=f"ri.scout.run.{i}", title=f"Run {i}", run_number=i) for i in range(3)]
    clients.run.SearchRuns.return_value = run_service_pb2.SearchRunsResponse(results=page, next_page_token="more")
    asset = Asset(
        rid="ri.scout.asset.1",
        name="Helicopter",
        description=None,
        properties={},
        labels=[],
        created_at=START,
        is_archived=False,
        _clients=clients,
    )
    with set_display_options(show_link=False, max_items=4):
        section = asset._runs_section()
    assert clients.run.SearchRuns.call_count == 2
    assert [row[0] for row in section.rows] == ["#0", "#1", "#2", "#0"] and section.has_more


def test_attachments_link_to_their_downloads_by_name():
    """Attachments list by name, up to max_items, each linked to download its file outside any workspace."""
    clients = _linked_clients()
    attachments = [
        _attachment(clients, f"ri.attachments.attachment.{i}", name)
        for i, name in enumerate(["coupler.s4p", "amplifier.s2p", "antenna.s1p"])
    ]
    with set_display_options(max_items=2):
        section = _attachments_section(attachments)
    expected = [("ri.attachments.attachment.1", "amplifier.s2p"), ("ri.attachments.attachment.2", "antenna.s1p")]
    assert len(section.rows) == len(expected)
    for row, (rid, name) in zip(section.rows, expected, strict=True):
        assert f'href="https://app.example.com/api/downloadAttachment/{rid}"' in row[0] and f">{name}</a>" in row[0]
    assert section.has_more


def _channel_with_response(response: scout_compute_api.ComputeNodeResponse) -> Channel:
    clients = MagicMock()
    clients.catalog.get_enriched_datasets.return_value = [
        MagicMock(bounds=scout_catalog.Bounds(start=SECOND, end=SECOND, type=datasource.TimestampType.ABSOLUTE))
    ]
    clients.compute.compute.return_value = response
    return Channel("rotor_speed", "ri.catalog.dataset.1", ChannelDataType.DOUBLE, "rad/s", None, clients)


def test_channel_statistics_combine_buckets():
    """A numeric channel's card combines its buckets into whole-channel statistics and a chart."""
    later = api.Timestamp(seconds=START // 1_000_000_000 + 2, nanos=0)
    response = scout_compute_api.ComputeNodeResponse(
        bucketed_numeric=scout_compute_api.BucketedNumericPlot(
            # Buckets without first and last points fall back to their own timestamps.
            timestamps=[SECOND, SECOND, later],
            buckets=[
                scout_compute_api.NumericBucket(aggregations={}, count=3, min=1.0, max=4.0, mean=2.0),
                scout_compute_api.NumericBucket(aggregations={}, count=0),
                scout_compute_api.NumericBucket(aggregations={}, count=1, min=6.0, max=6.0, mean=6.0),
            ],
        )
    )
    section = _channel_with_response(response)._data_section()
    assert section.rows[:4] == [["Samples", "4"], ["Min", "1 rad/s"], ["Max", "6 rad/s"], ["Mean", "3 rad/s"]]
    assert [row[0] for row in section.rows[4:]] == ["First", "Last"]
    assert "2026-01-01T00:00:00Z" in section.rows[4][1] and "2026-01-01T00:00:02Z" in section.rows[5][1]
    assert section.body.startswith("<svg")


def test_channel_statistics_of_a_single_point():
    """A channel with one sample comes back as a single point, which is its own statistics."""
    response = scout_compute_api.ComputeNodeResponse(
        numeric_point=scout_compute_api.NumericPoint(timestamp=SECOND, value=2.5)
    )
    section = _channel_with_response(response)._data_section()
    assert section.rows[:4] == [["Samples", "1"], ["Min", "2.5 rad/s"], ["Max", "2.5 rad/s"], ["Mean", "2.5 rad/s"]]


def test_user_values_are_escaped():
    """Names, properties and links can't inject markup or script into the notebook."""
    run = _run(
        _offline_clients(),
        name="<script>alert(1)</script>",
        properties={"<b>key</b>": "<i>value</i>"},
        links=[{"url": "javascript:alert(1)", "title": "bad"}, {"url": "https://example.com/?a=1&b=2"}],
    )
    with set_display_options(show_link=False, fetch_details=False):
        card = run._repr_html_()
    assert card is not None
    assert "<script>" not in card and "&lt;script&gt;" in card
    assert "&lt;b&gt;key&lt;/b&gt;" in card and "&lt;i&gt;value&lt;/i&gt;" in card
    assert 'href="javascript' not in card
    assert 'href="https://example.com/?a=1&amp;b=2"' in card


def _time_text(html: str) -> str:
    return html[html.index(">") + 1 : html.index("</time>")]


@pytest.mark.parametrize(
    ("nanoseconds", "expected"),
    [
        (START, "2026-01-01T00:00:00Z"),
        (START + 120_000_000, "2026-01-01T00:00:00.120Z"),
        (START + 5, "2026-01-01T00:00:00.000000005Z"),
        (-1, "1969-12-31T23:59:59.999999999Z"),
    ],
)
def test_nanosecond_precision_shows_exact_timestamps(nanoseconds, expected):
    """At nanosecond precision, timestamps show the sub-second digits they need and no more."""
    with set_display_options(time_precision="ns"):
        assert _time_text(_display.timestamp(nanoseconds)) == expected


@pytest.mark.parametrize(
    ("precision", "expected"),
    [("s", "2026-01-01T00:00:00Z"), ("ms", "2026-01-01T00:00:00.999Z"), ("us", "2026-01-01T00:00:00.999999Z")],
)
def test_precision_cuts_off_finer_digits(precision, expected):
    """Coarser precision cuts digits off rather than rounding into the next second."""
    with set_display_options(time_precision=precision):
        html = _display.timestamp(START + 999_999_999)
    assert _time_text(html) == expected
    assert 'title="2026-01-01T00:00:00.999999999Z"' in html


def test_timestamp_uses_configured_timezone_and_format():
    """The timezone option converts timestamps; the readable format names the zone."""
    with set_display_options(timezone="America/Los_Angeles"):
        assert _time_text(_display.timestamp(START)) == "2025-12-31T16:00:00-08:00"
    with set_display_options(timezone="America/Los_Angeles", time_format="readable"):
        assert _time_text(_display.timestamp(START)) == "2025-12-31 16:00:00 PST"


@pytest.mark.parametrize(
    ("nanoseconds", "expected"),
    [
        (0, "0 s"),
        (1_500_000_000, "1.5 s"),
        (1_000_000_001, "1.000000001 s"),
        (59_999_999_999, "59.999999999 s"),
        (3_723_000_000_000, "1 h 2 min 3 s"),
        (86_400_000_000_000, "1 d"),
    ],
)
def test_duration(nanoseconds, expected):
    """Durations read in days, hours, minutes and seconds, with exact fractions of a second."""
    assert _display.duration(nanoseconds) == expected


def test_text_style_falls_back_to_repr_without_requests():
    """The text style returns None, so the notebook shows the plain repr, until the block ends.

    With links and loading on, it still makes no requests, since no card is built.
    """
    clients = _offline_clients()
    channel = Channel("rotor_speed", "ri.catalog.dataset.1", ChannelDataType.DOUBLE, "rad/s", None, clients)
    with set_display_options(style="text"):
        for resource in (_dataset(clients), _run(clients), channel):
            assert resource._repr_html_() is None
    assert get_display_options().style == "html"


def test_set_display_options_lasts_without_a_with_block():
    """Called on its own, the change stays; restoring it is the caller's to do."""
    before = get_display_options()
    set_display_options(fetch_timeout=2)
    try:
        assert get_display_options().fetch_timeout == 2
    finally:
        set_display_options(fetch_timeout=before.fetch_timeout)
    assert get_display_options() == before


def test_invalid_option_raises_and_keeps_current_options():
    """An invalid value is rejected before it changes anything."""
    before = get_display_options()
    with pytest.raises(ValueError, match="unknown time zone"):
        set_display_options(timezone="Mars/Olympus_Mons")
    with pytest.raises(ValueError, match="fetch_timeout"):
        set_display_options(fetch_timeout=0)
    assert get_display_options() == before


def test_large_sections_collapse_and_truncate():
    """Sections over the threshold start collapsed, and list at most max_items."""
    properties = {f"key{i:02d}": str(i) for i in range(30)}
    dataset = _dataset(_offline_clients(), properties=properties)
    with set_display_options(show_link=False, fetch_details=False, collapse_threshold=5, max_items=20):
        card = dataset._repr_html_()
    assert card is not None
    assert "<details><summary>Properties (30)</summary>" in card
    assert "<th>key19</th>" in card and "<th>key20</th>" not in card and "… and 10 more" in card

    with set_display_options(show_link=False, fetch_details=False):
        card = _dataset(_offline_clients(), properties={"a": "1"})._repr_html_()
    assert card is not None and "<details open><summary>Properties (1)</summary>" in card


def test_first_stops_loading_past_max_items():
    """Loading stops one item past max_items, and the section shows that more exist."""
    loaded: list[int] = []

    def numbers():
        for i in range(1000):
            loaded.append(i)
            yield i

    with set_display_options(max_items=3):
        items, has_more = _display.first(numbers())
        section = _display.Section("Items", [[str(i)] for i in items], has_more=has_more)
        card = _display.render(kind="Test", title="", fallback="", fields=[], sections=[section])
    assert items == [0, 1, 2] and has_more and len(loaded) == 4
    assert card is not None and "Items (3+)" in card and "Showing the first 3." in card


def test_client_card_shows_configuration_without_requests():
    """The client card shows its profile and endpoints without contacting the server."""
    clients = NonCallableMock(spec=["workspace_rid", "app_base_url", "api_base_url"])
    clients.workspace_rid = None
    clients.app_base_url = "https://app.example.com"
    clients.api_base_url = "https://api.example.com"
    card = NominalClient(_clients=clients, _profile="staging")._repr_html_()
    assert card is not None
    assert "staging" in card and 'href="https://app.example.com"' in card and "the organization's default" in card
