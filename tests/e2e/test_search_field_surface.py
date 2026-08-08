"""End-to-end tests for which fields each search filter matches.

`test_search.py` cannot detect a change in field surface: its fixture puts the session tag in
several fields of the same resource, so an assertion passes regardless of which field matched.
This module creates one probe resource per type, each field carrying its own token, so a match is
attributable to exactly one field.

Tokens must not share substrings across fields. `search_text` matches name and description by
similarity, so two strings sharing a long substring match each other even when neither contains
the other -- which would make every negative assertion unreliable.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Iterator, Protocol, Sequence, cast
from uuid import uuid4

import pytest

from nominal.core import EventType, NominalClient
from nominal.core._utils.api_tools import HasRid
from tests.e2e import _create_random_start_end

FIELDS = ("name", "description", "label", "property")


def _field_tokens() -> dict[str, str]:
    """One token per field.

    The words are for readability in failure output; the independent random tails are what make a
    match attributable, since no token shares a substring with any other.
    """
    return {
        "name": f"apples{uuid4().hex}",
        "description": f"bananas{uuid4().hex}",
        "label": f"cherries{uuid4().hex}",
        "property": f"dates{uuid4().hex}",
    }


def _rids(items: Sequence[object]) -> set[str]:
    return {cast(HasRid, item).rid for item in items}


def _kgrams(value: str, k: int = 8) -> set[str]:
    """Every length-k slice of `value`, for detecting shared substrings between tokens."""
    return {value[i : i + k] for i in range(len(value) - k + 1)}


@dataclass(frozen=True)
class Probe:
    """A resource whose four fields each carry a distinct, unrelated token."""

    tokens: dict[str, str]
    rid: str


class HasArchive(Protocol):
    def archive(self) -> None: ...


def _archive_all(resources: Sequence[HasArchive]) -> None:
    """Archive every resource, continuing past failures so one bad archive can't orphan the rest."""
    for resource in resources:
        try:
            resource.archive()
        except Exception as e:
            print(f"WARNING: failed to archive probe resource {resource!r}: {e!r}")


def test_field_tokens_share_no_substring() -> None:
    """Each field's token shares no 8-character substring with any other, so a match names exactly one field."""
    tokens = _field_tokens()
    assert set(tokens) == set(FIELDS)
    kgrams = {field: _kgrams(token) for field, token in tokens.items()}
    for field, grams in kgrams.items():
        others = [other for name, other in kgrams.items() if name != field]
        assert all(grams.isdisjoint(other) for other in others)


@dataclass(frozen=True)
class Target:
    """A searchable type: how to create a probe for it, and how to search it."""

    name: str
    make: Callable[[NominalClient, dict[str, str]], HasRid]
    warm: Callable[[NominalClient, str], Sequence[object]]


def _make_dataset(client: NominalClient, tokens: dict[str, str]) -> HasRid:
    return client.create_dataset(
        tokens["name"],
        description=tokens["description"],
        labels=[tokens["label"]],
        properties={"probe": tokens["property"]},
    )


def _make_run(client: NominalClient, tokens: dict[str, str]) -> HasRid:
    start, end = _create_random_start_end()
    return client.create_run(
        tokens["name"],
        start,
        end,
        tokens["description"],
        labels=[tokens["label"]],
        properties={"probe": tokens["property"]},
    )


def _make_secret(client: NominalClient, tokens: dict[str, str]) -> HasRid:
    return client.create_secret(
        tokens["name"],
        "probe-value",
        tokens["description"],
        labels=[tokens["label"]],
        properties={"probe": tokens["property"]},
    )


def _make_video(client: NominalClient, tokens: dict[str, str]) -> HasRid:
    return client.create_video(
        tokens["name"],
        description=tokens["description"],
        labels=[tokens["label"]],
        properties={"probe": tokens["property"]},
    )


def _make_asset(client: NominalClient, tokens: dict[str, str]) -> HasRid:
    return client.create_asset(
        tokens["name"],
        tokens["description"],
        labels=[tokens["label"]],
        properties={"probe": tokens["property"]},
    )


def _make_event(client: NominalClient, tokens: dict[str, str]) -> HasRid:
    # Event creation requires an asset even though `assets` is optional client-side. This
    # throwaway asset carries none of the probe's tokens and is archived immediately.
    asset = client.create_asset(f"event-asset-{uuid4().hex}")
    try:
        start, _ = _create_random_start_end()
        event = client.create_event(
            tokens["name"],
            EventType.INFO,
            start,
            description=tokens["description"],
            assets=[asset],
            labels=[tokens["label"]],
            properties={"probe": tokens["property"]},
        )
    finally:
        asset.archive()
    return event


def _make_template(client: NominalClient, tokens: dict[str, str]) -> HasRid:
    return client.create_workbook_template(
        title=tokens["name"],
        description=tokens["description"],
        labels=[tokens["label"]],
        properties={"probe": tokens["property"]},
    )


TARGETS = (
    Target("datasets", _make_dataset, lambda c, t: c.search_datasets(search_text=t)),
    Target("runs", _make_run, lambda c, t: c.search_runs(search_text=t)),
    Target("secrets", _make_secret, lambda c, t: c.search_secrets(search_text=t)),
    Target("videos", _make_video, lambda c, t: c.search_videos(search_text=t)),
    Target("assets", _make_asset, lambda c, t: c.search_assets(search_text=t)),
    Target("events", _make_event, lambda c, t: c.search_events(search_text=t)),
    Target("templates", _make_template, lambda c, t: c.search_workbook_templates(search_text=t)),
)


_INDEX_POLL_SECONDS = 5.0
_INDEX_POLL_ATTEMPTS = 12  # up to a minute


def _wait_for_indexed(
    search: Callable[[NominalClient, str], Sequence[object]],
    client: NominalClient,
    token: str,
    rid: str,
) -> None:
    """Block until `rid` is findable by `token`, so later negative assertions are trustworthy."""
    for attempt in range(_INDEX_POLL_ATTEMPTS):
        if rid in _rids(search(client, token)):
            return
        if attempt < _INDEX_POLL_ATTEMPTS - 1:
            time.sleep(_INDEX_POLL_SECONDS)
    raise AssertionError(f"probe {rid} never became searchable by {token!r}")


@pytest.fixture(scope="session")
def probes(client: NominalClient) -> Iterator[dict[str, Probe]]:
    """One probe resource per target, archived on teardown."""
    created: dict[str, Probe] = {}
    resources: list[HasArchive] = []
    try:
        for target in TARGETS:
            tokens = _field_tokens()
            resource = target.make(client, tokens)
            resources.append(cast("HasArchive", resource))
            created[target.name] = Probe(tokens=tokens, rid=resource.rid)
        for target in TARGETS:
            probe = created[target.name]
            # Warming on the name token alone is enough: "name" is in matching_fields for every
            # SURFACE_CASES case, so each filter still gets its own positive-control cell below.
            _wait_for_indexed(target.warm, client, probe.tokens["name"], probe.rid)
        yield created
    finally:
        _archive_all(resources)


# (target name, filter name, search callable, fields the filter is expected to match)
SURFACE_CASES = (
    ("datasets", "search_text", lambda c, t: c.search_datasets(search_text=t), FIELDS),
    ("runs", "search_text", lambda c, t: c.search_runs(search_text=t), FIELDS),
    ("secrets", "search_text", lambda c, t: c.search_secrets(search_text=t), FIELDS),
    ("videos", "search_text", lambda c, t: c.search_videos(search_text=t), FIELDS),
    ("assets", "search_text", lambda c, t: c.search_assets(search_text=t), FIELDS),
    ("events", "search_text", lambda c, t: c.search_events(search_text=t), FIELDS),
    ("datasets", "exact_match", lambda c, t: c.search_datasets(exact_match=t), FIELDS),
    ("runs", "exact_match", lambda c, t: c.search_runs(exact_match=t), FIELDS),
    ("runs", "name_substring", lambda c, t: c.search_runs(name_substring=t), FIELDS),
    ("assets", "exact_substring", lambda c, t: c.search_assets(exact_substring=t), FIELDS),
    ("templates", "exact_match", lambda c, t: c.search_workbook_templates(exact_match=t), ("name",)),
    ("templates", "search_text", lambda c, t: c.search_workbook_templates(search_text=t), ("name", "description")),
)


@pytest.mark.parametrize(
    ("target_name", "filter_name", "search", "matching_fields"),
    SURFACE_CASES,
    ids=[f"{target}-{filter_name}" for target, filter_name, _, _ in SURFACE_CASES],
)
@pytest.mark.parametrize("field", FIELDS)
def test_field_surface(
    client: NominalClient,
    probes: dict[str, Probe],
    target_name: str,
    filter_name: str,
    search: Callable[[NominalClient, str], Sequence[object]],
    matching_fields: tuple[str, ...],
    field: str,
) -> None:
    """The filter matches a token living only in `field` exactly when `field` is in its surface."""
    probe = probes[target_name]
    found = probe.rid in _rids(search(client, probe.tokens[field]))
    assert found == (field in matching_fields)


# (target name, labels search, properties search) -- one per distinct query construction
SET_SEMANTICS_CASES = (
    (
        "runs",
        lambda c, labels: c.search_runs(labels=labels),
        lambda c, props: c.search_runs(properties=props),
    ),
    (
        "datasets",
        lambda c, labels: c.search_datasets(labels=labels),
        lambda c, props: c.search_datasets(properties=props),
    ),
    (
        "templates",
        lambda c, labels: c.search_workbook_templates(labels=labels),
        lambda c, props: c.search_workbook_templates(properties=props),
    ),
)

_SET_IDS = [name for name, _, _ in SET_SEMANTICS_CASES]


@pytest.mark.parametrize(("target_name", "search_labels", "_search_props"), SET_SEMANTICS_CASES, ids=_SET_IDS)
def test_labels_filter_requires_all_labels(
    client: NominalClient,
    probes: dict[str, Probe],
    target_name: str,
    search_labels: Callable[[NominalClient, list[str]], Sequence[object]],
    _search_props: Callable[[NominalClient, dict[str, str]], Sequence[object]],
) -> None:
    """A labels filter requires every given label, so adding an absent one excludes the resource."""
    probe = probes[target_name]
    present = probe.tokens["label"]
    absent = f"cherries{uuid4().hex}"

    assert probe.rid in _rids(search_labels(client, [present]))
    assert probe.rid not in _rids(search_labels(client, [present, absent]))


@pytest.mark.parametrize(("target_name", "_search_labels", "search_props"), SET_SEMANTICS_CASES, ids=_SET_IDS)
def test_properties_filter_matches_key_and_value(
    client: NominalClient,
    probes: dict[str, Probe],
    target_name: str,
    _search_labels: Callable[[NominalClient, list[str]], Sequence[object]],
    search_props: Callable[[NominalClient, dict[str, str]], Sequence[object]],
) -> None:
    """A properties filter matches on key and value, so a wrong key or a wrong value excludes it."""
    probe = probes[target_name]
    value = probe.tokens["property"]

    assert probe.rid in _rids(search_props(client, {"probe": value}))
    assert probe.rid not in _rids(search_props(client, {"probe": f"dates{uuid4().hex}"}))
    assert probe.rid not in _rids(search_props(client, {f"probe{uuid4().hex}": value}))


_T1 = datetime(2023, 3, 1, 12, 0, 0)
_T2 = _T1 + timedelta(hours=1)
_ONE_MICRO = timedelta(microseconds=1)


@dataclass(frozen=True)
class TimeProbes:
    """A run and an event both spanning exactly [_T1, _T2], each carrying its own probe token."""

    run_rid: str
    run_token: str
    event_rid: str
    event_token: str


@pytest.fixture(scope="session")
def time_probes(client: NominalClient) -> Iterator[TimeProbes]:
    """A run and an event spanning exactly [_T1, _T2], archived on every exit path."""
    run_tokens = _field_tokens()
    event_tokens = _field_tokens()
    resources: list[HasArchive] = []
    try:
        run = client.create_run(run_tokens["name"], _T1, _T2, properties={"probe": run_tokens["property"]})
        resources.append(run)
        # Event creation requires an asset even though `assets` is optional client-side.
        event_asset = client.create_asset(f"event-asset-{uuid4().hex}")
        resources.append(event_asset)
        event = client.create_event(
            event_tokens["name"],
            EventType.INFO,
            _T1,
            _T2 - _T1,
            assets=[event_asset],
            properties={"probe": event_tokens["property"]},
        )
        resources.append(event)
        _wait_for_indexed(lambda c, t: c.search_runs(search_text=t), client, run_tokens["name"], run.rid)
        _wait_for_indexed(lambda c, t: c.search_events(search_text=t), client, event_tokens["name"], event.rid)

        yield TimeProbes(
            run_rid=run.rid,
            run_token=run_tokens["property"],
            event_rid=event.rid,
            event_token=event_tokens["property"],
        )
    finally:
        _archive_all(resources)


# Every case below also filters on properties={"probe": <token>}, narrowing the search to the probe
# resource instead of scanning the whole corpus. Filters are ANDed, and the expected=True cases are
# same-shape positive controls proving the property conjunct alone doesn't exclude the probe -- so an
# expected=False result here is attributable solely to the time bound.
RUN_TIME_BOUND_CASES = (
    (lambda c, t: c.search_runs(start=_T1, properties={"probe": t}), True),
    (lambda c, t: c.search_runs(start=_T1 + _ONE_MICRO, properties={"probe": t}), False),
    (lambda c, t: c.search_runs(end=_T2, properties={"probe": t}), True),
    (lambda c, t: c.search_runs(end=_T2 - _ONE_MICRO, properties={"probe": t}), False),
)


@pytest.mark.parametrize(
    ("search", "expected"),
    RUN_TIME_BOUND_CASES,
    ids=["start-at-boundary", "start-past-boundary", "end-at-boundary", "end-past-boundary"],
)
def test_search_runs_time_bounds_are_inclusive(
    client: NominalClient,
    time_probes: TimeProbes,
    search: Callable[[NominalClient, str], Sequence[object]],
    expected: bool,
) -> None:
    """Run start and end bounds include a run sitting exactly on the boundary."""
    results = search(client, time_probes.run_token)
    assert (time_probes.run_rid in _rids(results)) == expected


def test_search_runs_time_bounds_select_contained_runs(client: NominalClient, time_probes: TimeProbes) -> None:
    """A window strictly inside the run excludes it, so the bounds are containment not overlap."""
    results = client.search_runs(
        start=_T1 + _ONE_MICRO, end=_T2 - _ONE_MICRO, properties={"probe": time_probes.run_token}
    )
    assert time_probes.run_rid not in _rids(results)


def test_search_runs_exact_window_matches(client: NominalClient, time_probes: TimeProbes) -> None:
    """A window exactly equal to the run's span contains it."""
    results = client.search_runs(start=_T1, end=_T2, properties={"probe": time_probes.run_token})
    assert time_probes.run_rid in _rids(results)


EVENT_TIME_BOUND_CASES = (
    (lambda c, t: c.search_events(after=_T1, properties={"probe": t}), True),
    (lambda c, t: c.search_events(after=_T2, properties={"probe": t}), False),
    (lambda c, t: c.search_events(before=_T2, properties={"probe": t}), True),
    (lambda c, t: c.search_events(before=_T1, properties={"probe": t}), False),
)


@pytest.mark.parametrize(
    ("search", "expected"),
    EVENT_TIME_BOUND_CASES,
    ids=["after-overlaps", "after-at-end", "before-overlaps", "before-at-start"],
)
def test_search_events_time_bounds_are_exclusive(
    client: NominalClient,
    time_probes: TimeProbes,
    search: Callable[[NominalClient, str], Sequence[object]],
    expected: bool,
) -> None:
    """Event bounds are exclusive and overlap based, unlike the containment bounds on runs."""
    results = search(client, time_probes.event_token)
    assert (time_probes.event_rid in _rids(results)) == expected
