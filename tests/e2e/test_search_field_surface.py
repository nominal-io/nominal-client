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

from dataclasses import dataclass
from typing import Callable, Iterator, Protocol, Sequence, cast
from uuid import uuid4

import pytest

from nominal.core import NominalClient
from nominal.core._utils.api_tools import HasRid

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


def test_field_tokens_share_no_substring() -> None:
    """Each field's token shares no 8-character substring with any other, so a match names exactly one field."""
    tokens = _field_tokens()
    assert len(set(tokens)) == len(FIELDS)
    kgrams = {field: _kgrams(token) for field, token in tokens.items()}
    for field, grams in kgrams.items():
        others = [other for name, other in kgrams.items() if name != field]
        assert all(grams.isdisjoint(other) for other in others)


@dataclass(frozen=True)
class Target:
    """A searchable type: how to create a probe for it, and how to search it."""

    name: str
    make: Callable[[NominalClient, dict[str, str]], HasRid]
    fields: tuple[str, ...] = FIELDS


def _make_dataset(client: NominalClient, tokens: dict[str, str]) -> HasRid:
    return client.create_dataset(
        tokens["name"],
        description=tokens["description"],
        labels=[tokens["label"]],
        properties={"probe": tokens["property"]},
    )


def _make_run(client: NominalClient, tokens: dict[str, str]) -> HasRid:
    from tests.e2e import _create_random_start_end

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
    from nominal.core import EventType
    from tests.e2e import _create_random_start_end

    # The backend rejects event creation without an asset (Scout:MissingAssetRid), even though
    # `assets` is optional in the client signature. This throwaway asset exists only to satisfy
    # that requirement; it carries none of the probe's tokens and is archived immediately.
    asset = client.create_asset(f"event-asset-{uuid4().hex}")
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
    Target("datasets", _make_dataset),
    Target("runs", _make_run),
    Target("secrets", _make_secret),
    Target("videos", _make_video),
    Target("assets", _make_asset),
    Target("events", _make_event),
    Target("templates", _make_template),
)


@pytest.fixture(scope="session")
def probes(client: NominalClient) -> Iterator[dict[str, Probe]]:
    """One probe resource per target, archived on teardown."""
    created: dict[str, Probe] = {}
    resources: list[object] = []
    for target in TARGETS:
        tokens = _field_tokens()
        resource = target.make(client, tokens)
        resources.append(resource)
        created[target.name] = Probe(tokens=tokens, rid=resource.rid)

    yield created

    for archivable in resources:
        cast("HasArchive", archivable).archive()


def test_probes_fixture_builds(probes: dict[str, Probe]) -> None:
    """Every target creates a probe resource with a rid."""
    assert set(probes) == {t.name for t in TARGETS}
    assert all(p.rid for p in probes.values())
