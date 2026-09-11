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

import itertools
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Iterator, Protocol, Sequence
from uuid import uuid4

import pytest

from nominal.core import EventType, NominalClient
from tests.e2e import _create_random_start_end

_PROPERTY_KEY = "probe"

_FIELD_WORDS = {"name": "apples", "description": "bananas", "label": "cherries", "property": "dates"}
FIELDS = tuple(_FIELD_WORDS)


def _field_tokens() -> dict[str, str]:
    """One token per field.

    The words are for readability in failure output; the independent random tails are what make a
    match attributable, since no token shares a substring with any other.
    """
    return {field: f"{word}{uuid4().hex}" for field, word in _FIELD_WORDS.items()}


class Archivable(Protocol):
    """A created resource: readable rid, archivable on teardown."""

    @property
    def rid(self) -> str: ...

    def archive(self) -> None: ...


def _rids(items: Sequence[Archivable]) -> set[str]:
    return {item.rid for item in items}


def _kgrams(value: str, k: int = 8) -> set[str]:
    """Every length-k slice of `value`, for detecting shared substrings between tokens."""
    return {value[i : i + k] for i in range(len(value) - k + 1)}


@dataclass(frozen=True)
class Probe:
    """A resource whose four fields each carry a distinct, unrelated token."""

    tokens: dict[str, str]
    rid: str


def _archive_all(resources: Sequence[Archivable]) -> None:
    """Archive every resource, continuing past failures so one bad archive can't orphan the rest.

    Failures are collected and reported once, after the loop. Reporting from inside the `except`
    would let the reporting call itself abort the loop -- `pyproject.toml` sets
    `filterwarnings = ["error"]`, so a `warnings.warn` there raises and orphans every resource
    that had not been reached yet. An orphaned probe is harmless (unique tokens, no collision),
    so this must never fail the run either.
    """
    failures = []
    for resource in resources:
        try:
            resource.archive()
        except Exception as e:
            failures.append(f"{resource!r}: {e!r}")
    if failures:
        print(f"WARNING: failed to archive {len(failures)} of {len(resources)} probe resources: {'; '.join(failures)}")


def test_field_tokens_share_no_substring() -> None:
    """Each field's token shares no 8-character substring with any other, so a match names exactly one field."""
    tokens = _field_tokens()
    assert set(tokens) == set(FIELDS)
    kgrams = {field: _kgrams(token) for field, token in tokens.items()}
    for field_a, field_b in itertools.combinations(kgrams, 2):
        assert kgrams[field_a].isdisjoint(kgrams[field_b]), f"{field_a!r} and {field_b!r} tokens share a substring"


@dataclass(frozen=True)
class Target:
    """A searchable type: how to create a probe for it."""

    name: str
    make: Callable[[NominalClient, dict[str, str]], Archivable]


def _make_dataset(client: NominalClient, tokens: dict[str, str]) -> Archivable:
    return client.create_dataset(
        tokens["name"],
        description=tokens["description"],
        labels=[tokens["label"]],
        properties={_PROPERTY_KEY: tokens["property"]},
    )


def _make_run(client: NominalClient, tokens: dict[str, str]) -> Archivable:
    start, end = _create_random_start_end()
    return client.create_run(
        tokens["name"],
        start,
        end,
        tokens["description"],
        labels=[tokens["label"]],
        properties={_PROPERTY_KEY: tokens["property"]},
    )


def _make_secret(client: NominalClient, tokens: dict[str, str]) -> Archivable:
    return client.create_secret(
        tokens["name"],
        "probe-value",
        tokens["description"],
        labels=[tokens["label"]],
        properties={_PROPERTY_KEY: tokens["property"]},
    )


def _make_video(client: NominalClient, tokens: dict[str, str]) -> Archivable:
    return client.create_video(
        tokens["name"],
        description=tokens["description"],
        labels=[tokens["label"]],
        properties={_PROPERTY_KEY: tokens["property"]},
    )


def _make_asset(client: NominalClient, tokens: dict[str, str]) -> Archivable:
    return client.create_asset(
        tokens["name"],
        tokens["description"],
        labels=[tokens["label"]],
        properties={_PROPERTY_KEY: tokens["property"]},
    )


def _make_event(client: NominalClient, tokens: dict[str, str]) -> Archivable:
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
            properties={_PROPERTY_KEY: tokens["property"]},
        )
    finally:
        asset.archive()
    return event


def _make_template(client: NominalClient, tokens: dict[str, str]) -> Archivable:
    return client.create_workbook_template(
        title=tokens["name"],
        description=tokens["description"],
        labels=[tokens["label"]],
        properties={_PROPERTY_KEY: tokens["property"]},
    )


TARGETS = (
    Target("datasets", _make_dataset),
    Target("runs", _make_run),
    Target("secrets", _make_secret),
    Target("videos", _make_video),
    Target("assets", _make_asset),
    Target("events", _make_event),
    Target("templates", _make_template),
)


SearchFn = Callable[[NominalClient, str], Sequence[Archivable]]


@dataclass(frozen=True)
class SurfaceCase:
    """One filter's expected field surface on one target type."""

    target: str
    filter_name: str
    search: SearchFn
    matching_fields: tuple[str, ...]

    @property
    def id(self) -> str:
        return f"{self.target}-{self.filter_name}"


SURFACE_CASES = (
    SurfaceCase("datasets", "search_text", lambda c, t: c.search_datasets(search_text=t), FIELDS),
    SurfaceCase("runs", "search_text", lambda c, t: c.search_runs(search_text=t), FIELDS),
    SurfaceCase("secrets", "search_text", lambda c, t: c.search_secrets(search_text=t), FIELDS),
    SurfaceCase("videos", "search_text", lambda c, t: c.search_videos(search_text=t), FIELDS),
    SurfaceCase("assets", "search_text", lambda c, t: c.search_assets(search_text=t), FIELDS),
    SurfaceCase("events", "search_text", lambda c, t: c.search_events(search_text=t), FIELDS),
    SurfaceCase("datasets", "exact_match", lambda c, t: c.search_datasets(exact_match=t), FIELDS),
    SurfaceCase("runs", "exact_match", lambda c, t: c.search_runs(exact_match=t), FIELDS),
    SurfaceCase("runs", "name_substring", lambda c, t: c.search_runs(name_substring=t), FIELDS),
    SurfaceCase("assets", "exact_substring", lambda c, t: c.search_assets(exact_substring=t), FIELDS),
    SurfaceCase("templates", "exact_match", lambda c, t: c.search_workbook_templates(exact_match=t), ("name",)),
    SurfaceCase(
        "templates",
        "search_text",
        lambda c, t: c.search_workbook_templates(search_text=t),
        ("name", "description"),
    ),
)

# Every target must be searchable by search_text so the fixture below can warm the index on it;
# this derives the warm-up search from SURFACE_CASES instead of duplicating a second list of lambdas.
_WARM_SEARCHES = {case.target: case.search for case in SURFACE_CASES if case.filter_name == "search_text"}
assert set(_WARM_SEARCHES) == {t.name for t in TARGETS}, "every target needs a search_text case to warm with"


_INDEX_POLL_SECONDS = 5.0
_INDEX_POLL_ATTEMPTS = 12  # up to a minute


def _wait_for_indexed(search: SearchFn, client: NominalClient, token: str, rid: str) -> None:
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
    resources: list[Archivable] = []
    try:
        for target in TARGETS:
            tokens = _field_tokens()
            resource = target.make(client, tokens)
            resources.append(resource)
            created[target.name] = Probe(tokens=tokens, rid=resource.rid)
        for target in TARGETS:
            probe = created[target.name]
            # Warming on the name token alone is enough: "name" is in matching_fields for every
            # SURFACE_CASES case, so each filter still gets its own positive-control cell below.
            _wait_for_indexed(_WARM_SEARCHES[target.name], client, probe.tokens["name"], probe.rid)
        yield created
    finally:
        _archive_all(resources)


@pytest.mark.parametrize("case", SURFACE_CASES, ids=lambda c: c.id)
@pytest.mark.parametrize("field", FIELDS)
def test_field_surface(
    client: NominalClient,
    probes: dict[str, Probe],
    case: SurfaceCase,
    field: str,
) -> None:
    """The filter matches a token living only in `field` exactly when `field` is in its surface."""
    probe = probes[case.target]
    found = probe.rid in _rids(case.search(client, probe.tokens[field]))
    assert found == (field in case.matching_fields)


LABELS_CASES = (
    ("runs", lambda c, labels: c.search_runs(labels=labels)),
    ("datasets", lambda c, labels: c.search_datasets(labels=labels)),
    ("templates", lambda c, labels: c.search_workbook_templates(labels=labels)),
)

PROPERTIES_CASES = (
    ("runs", lambda c, props: c.search_runs(properties=props)),
    ("datasets", lambda c, props: c.search_datasets(properties=props)),
    ("templates", lambda c, props: c.search_workbook_templates(properties=props)),
)


@pytest.mark.parametrize(("target_name", "search_labels"), LABELS_CASES, ids=[name for name, _ in LABELS_CASES])
def test_labels_filter_requires_all_labels(
    client: NominalClient,
    probes: dict[str, Probe],
    target_name: str,
    search_labels: Callable[[NominalClient, list[str]], Sequence[Archivable]],
) -> None:
    """A labels filter requires every given label, so adding an absent one excludes the resource."""
    probe = probes[target_name]
    present = probe.tokens["label"]
    absent = f"cherries{uuid4().hex}"

    assert probe.rid in _rids(search_labels(client, [present]))
    assert probe.rid not in _rids(search_labels(client, [present, absent]))


@pytest.mark.parametrize(("target_name", "search_props"), PROPERTIES_CASES, ids=[name for name, _ in PROPERTIES_CASES])
def test_properties_filter_matches_key_and_value(
    client: NominalClient,
    probes: dict[str, Probe],
    target_name: str,
    search_props: Callable[[NominalClient, dict[str, str]], Sequence[Archivable]],
) -> None:
    """A properties filter matches on key and value, so a wrong key or a wrong value excludes it."""
    probe = probes[target_name]
    value = probe.tokens["property"]

    assert probe.rid in _rids(search_props(client, {_PROPERTY_KEY: value}))
    assert probe.rid not in _rids(search_props(client, {_PROPERTY_KEY: f"dates{uuid4().hex}"}))
    assert probe.rid not in _rids(search_props(client, {f"{_PROPERTY_KEY}{uuid4().hex}": value}))


_T1 = datetime(2023, 3, 1, 12, 0, 0)
_T2 = _T1 + timedelta(hours=1)
_ONE_MICRO = timedelta(microseconds=1)


@dataclass(frozen=True)
class TimeProbes:
    """A run and an event both spanning exactly [_T1, _T2], each carrying its own probe token."""

    run: Probe
    event: Probe


@pytest.fixture(scope="session")
def time_probes(client: NominalClient) -> Iterator[TimeProbes]:
    """A run and an event spanning exactly [_T1, _T2], archived on every exit path."""
    run_tokens = _field_tokens()
    event_tokens = _field_tokens()
    resources: list[Archivable] = []
    try:
        run = client.create_run(run_tokens["name"], _T1, _T2, properties={_PROPERTY_KEY: run_tokens["property"]})
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
            properties={_PROPERTY_KEY: event_tokens["property"]},
        )
        resources.append(event)
        _wait_for_indexed(lambda c, t: c.search_runs(search_text=t), client, run_tokens["name"], run.rid)
        _wait_for_indexed(lambda c, t: c.search_events(search_text=t), client, event_tokens["name"], event.rid)

        yield TimeProbes(
            run=Probe(tokens=run_tokens, rid=run.rid),
            event=Probe(tokens=event_tokens, rid=event.rid),
        )
    finally:
        _archive_all(resources)


# Every case below also filters on properties={_PROPERTY_KEY: <token>}, narrowing the search to the probe
# resource instead of scanning the whole corpus. Filters are ANDed, and the expected=True cases are
# same-shape positive controls proving the property conjunct alone doesn't exclude the probe -- so an
# expected=False result here is attributable solely to the time bound.
RUN_TIME_BOUND_CASES = (
    (lambda c, t: c.search_runs(start=_T1, properties={_PROPERTY_KEY: t}), True),
    (lambda c, t: c.search_runs(start=_T1 + _ONE_MICRO, properties={_PROPERTY_KEY: t}), False),
    (lambda c, t: c.search_runs(end=_T2, properties={_PROPERTY_KEY: t}), True),
    (lambda c, t: c.search_runs(end=_T2 - _ONE_MICRO, properties={_PROPERTY_KEY: t}), False),
)


@pytest.mark.parametrize(
    ("search", "expected"),
    RUN_TIME_BOUND_CASES,
    ids=["start-at-boundary", "start-past-boundary", "end-at-boundary", "end-past-boundary"],
)
def test_search_runs_time_bounds_are_inclusive(
    client: NominalClient,
    time_probes: TimeProbes,
    search: SearchFn,
    expected: bool,
) -> None:
    """Run start and end bounds include a run sitting exactly on the boundary."""
    results = search(client, time_probes.run.tokens["property"])
    assert (time_probes.run.rid in _rids(results)) == expected


def test_search_runs_time_bounds_select_contained_runs(client: NominalClient, time_probes: TimeProbes) -> None:
    """A window strictly inside the run excludes it, so the bounds are containment not overlap."""
    results = client.search_runs(
        start=_T1 + _ONE_MICRO, end=_T2 - _ONE_MICRO, properties={_PROPERTY_KEY: time_probes.run.tokens["property"]}
    )
    assert time_probes.run.rid not in _rids(results)


def test_search_runs_exact_window_matches(client: NominalClient, time_probes: TimeProbes) -> None:
    """A window exactly equal to the run's span contains it."""
    results = client.search_runs(start=_T1, end=_T2, properties={_PROPERTY_KEY: time_probes.run.tokens["property"]})
    assert time_probes.run.rid in _rids(results)


EVENT_TIME_BOUND_CASES = (
    (lambda c, t: c.search_events(after=_T1, properties={_PROPERTY_KEY: t}), True),
    (lambda c, t: c.search_events(after=_T2, properties={_PROPERTY_KEY: t}), False),
    (lambda c, t: c.search_events(before=_T2, properties={_PROPERTY_KEY: t}), True),
    (lambda c, t: c.search_events(before=_T1, properties={_PROPERTY_KEY: t}), False),
)


@pytest.mark.parametrize(
    ("search", "expected"),
    EVENT_TIME_BOUND_CASES,
    ids=["after-overlaps", "after-at-end", "before-overlaps", "before-at-start"],
)
def test_search_events_time_bounds_are_exclusive(
    client: NominalClient,
    time_probes: TimeProbes,
    search: SearchFn,
    expected: bool,
) -> None:
    """Event bounds are exclusive and overlap based, unlike the containment bounds on runs."""
    results = search(client, time_probes.event.tokens["property"])
    assert (time_probes.event.rid in _rids(results)) == expected
