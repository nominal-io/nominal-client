from __future__ import annotations

from datetime import timedelta
from typing import Callable
from unittest.mock import MagicMock

import pytest
from nominal_api import scout_catalog, scout_compute_api

from nominal.core.client import NominalClient
from nominal.core.dataset import Dataset, DatasetBounds, _dataset_from_conjure
from nominal.core.derived_dataset import (
    DerivedDataset,
    DerivedDatasetInput,
    TagFilter,
    _build_duration,
    _build_spec,
    _parse_duration,
    _parse_spec,
)


def _saved(rid: str) -> scout_compute_api.Dataset:
    return scout_compute_api.Dataset(
        saved=scout_compute_api.SavedDataset(rid=scout_compute_api.StringConstant(literal=rid))
    )


def _tag_in(key: str, *values: str) -> scout_compute_api.TagPredicate:
    return scout_compute_api.TagPredicate(
        in_=scout_compute_api.TagIn(
            key=scout_compute_api.StringConstant(literal=key),
            values=scout_compute_api.StringSetConstantV2(
                literal=[scout_compute_api.StringConstant(literal=value) for value in values]
            ),
        )
    )


def _tag_not_in(key: str, *values: str) -> scout_compute_api.TagPredicate:
    return scout_compute_api.TagPredicate(
        not_in=scout_compute_api.TagNotIn(
            key=scout_compute_api.StringConstant(literal=key),
            values=scout_compute_api.StringSetConstantV2(
                literal=[scout_compute_api.StringConstant(literal=value) for value in values]
            ),
        )
    )


def _and(*predicates: scout_compute_api.TagPredicate) -> scout_compute_api.TagPredicate:
    """Right-associated conjunction, the nesting a hand-authored definition is as likely to use as ours."""
    head, *rest = predicates
    if not rest:
        return head
    return scout_compute_api.TagPredicate(and_=scout_compute_api.TagAnd(left=head, right=_and(*rest)))


def _combine(*inputs: scout_compute_api.Dataset) -> scout_compute_api.Dataset:
    return scout_compute_api.Dataset(combine=scout_compute_api.CombinedDataset(inputs=list(inputs)))


def _filter(spec: scout_compute_api.Dataset, predicate: scout_compute_api.TagPredicate) -> scout_compute_api.Dataset:
    return scout_compute_api.Dataset(filter=scout_compute_api.FilteredDataset(input=spec, predicate=predicate))


def _shift(spec: scout_compute_api.Dataset, seconds: int) -> scout_compute_api.Dataset:
    return scout_compute_api.Dataset(
        time_shift=scout_compute_api.TimeShiftedDataset(
            input=spec,
            offset=scout_compute_api.Duration(
                seconds=scout_compute_api.DurationSeconds(seconds=scout_compute_api.IntegerConstant(literal=seconds))
            ),
        )
    )


def _tagged(spec: scout_compute_api.Dataset, key: str, value: str) -> scout_compute_api.Dataset:
    return scout_compute_api.Dataset(
        tag=scout_compute_api.TaggedDataset(
            input=spec,
            key=scout_compute_api.StringConstant(literal=key),
            value=scout_compute_api.StringConstant(literal=value),
        )
    )


def _filtered(rid: str, predicate: scout_compute_api.TagPredicate) -> scout_compute_api.Dataset:
    return scout_compute_api.Dataset(
        combine=scout_compute_api.CombinedDataset(
            inputs=[
                scout_compute_api.Dataset(
                    filter=scout_compute_api.FilteredDataset(input=_saved(rid), predicate=predicate)
                )
            ]
        )
    )


def _definition(spec: scout_compute_api.Dataset, commit_id: str = "ri.commit.1") -> scout_catalog.DerivedDefinition:
    commit = MagicMock()
    commit.id = commit_id
    return scout_catalog.DerivedDefinition(spec=spec, commit=commit)


@pytest.fixture
def client(mock_clients: MagicMock) -> NominalClient:
    """A NominalClient over the shared mock clients bunch."""
    return NominalClient(_clients=mock_clients)


@pytest.fixture
def mock_dataset(mock_clients: MagicMock) -> DerivedDataset:
    return DerivedDataset(
        rid="ri.catalog.ws.dataset.derived",
        name="Derived",
        description=None,
        bounds=DatasetBounds(start=0, end=1),
        properties={},
        labels=[],
        is_archived=False,
        _clients=mock_clients,
    )


# --- inputs ---


def test_create_resolves_a_dataset_to_its_rid() -> None:
    """An input built from a Dataset equals the one the server hands back, which only carries RIDs."""
    dataset = MagicMock()
    dataset.rid = "ri.a"
    assert DerivedDatasetInput.create(dataset, filters=[TagFilter.in_("vehicle", "A")]) == DerivedDatasetInput(
        "ri.a", [TagFilter.in_("vehicle", "A")]
    )


def test_create_without_transforms_means_the_whole_dataset() -> None:
    assert DerivedDatasetInput.create("ri.a") == DerivedDatasetInput("ri.a")


def test_filters_are_copied_out_of_the_callers_list() -> None:
    """The input is frozen, so the clauses it was built with cannot change underneath it afterwards."""
    clauses = [TagFilter.in_("vehicle", "A")]
    dataset_input = DerivedDatasetInput("ri.a", clauses)
    clauses.append(TagFilter.in_("run", "7"))
    assert dataset_input.filters == (TagFilter.in_("vehicle", "A"),)


def test_filter_values_are_frozen() -> None:
    """`values` is part of the hash, so it cannot stay a list the caller can still append to."""
    values = ["A"]
    clause = TagFilter("vehicle", values)
    values.append("B")
    assert clause.values == ("A",)
    with pytest.raises(TypeError):
        clause.values[0] = "B"  # type: ignore[index]


def test_an_offset_is_reduced_to_nanoseconds() -> None:
    """A timedelta and a nanosecond count naming the same shift produce equal inputs."""
    assert DerivedDatasetInput("ri.a", offset=timedelta(seconds=-5)) == DerivedDatasetInput(
        "ri.a", offset=-5_000_000_000
    )


def test_inputs_are_hashable_however_they_were_built() -> None:
    """Inputs come back in sequences users compare and dedupe, so equal inputs must hash alike."""
    built_two_ways = {
        DerivedDatasetInput.create("ri.a", filters=[TagFilter.in_("vehicle", "A")]),
        DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A")]),
    }
    assert len(built_two_ways) == 1
    assert len({DerivedDatasetInput("ri.a"), DerivedDatasetInput("ri.a", offset=5)}) == 2


# --- spec building ---


def test_build_spec_combines_inputs_with_no_transforms() -> None:
    """Inputs with nothing applied go on the wire as bare saved datasets under a combine."""
    spec = _build_spec([DerivedDatasetInput("ri.a"), DerivedDatasetInput("ri.b")])
    assert spec == _combine(_saved("ri.a"), _saved("ri.b"))


def test_build_spec_wraps_a_filtered_input_in_a_filter() -> None:
    spec = _build_spec([DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A")])])
    assert spec == _combine(_filter(_saved("ri.a"), _tag_in("vehicle", "A")))


def test_build_spec_ands_the_clauses_in_the_order_given() -> None:
    """Clause order is the caller's, so it survives the round trip rather than being sorted away."""
    spec = _build_spec([DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A"), TagFilter.not_in("run", "7")])])
    assert spec == _combine(_filter(_saved("ri.a"), _and(_tag_in("vehicle", "A"), _tag_not_in("run", "7"))))


def test_build_spec_carries_multiple_values_on_one_clause() -> None:
    spec = _build_spec([DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A", "B")])])
    assert spec == _combine(_filter(_saved("ri.a"), _tag_in("vehicle", "A", "B")))


def test_build_spec_nests_the_transforms_filter_then_tag_then_shift() -> None:
    """The app applies them in this order, so a definition written here reads back the same way there."""
    spec = _build_spec(
        [DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A")], ("source", "daq1"), timedelta(seconds=-5))]
    )
    assert spec == _combine(_shift(_tagged(_filter(_saved("ri.a"), _tag_in("vehicle", "A")), "source", "daq1"), -5))


def test_build_spec_omits_a_zero_offset_entirely() -> None:
    """An unshifted input must not gain a node, so an unrelated edit leaves its spec byte-identical."""
    assert _build_spec([DerivedDatasetInput("ri.a", offset=0)]) == _combine(_saved("ri.a"))


@pytest.mark.parametrize(
    ("nanoseconds", "unit", "count"),
    [
        (7, "nanoseconds", 7),
        (1_500_000_000, "milliseconds", 1_500),
        (-5_000_000_000, "seconds", -5),
        (5_400_000_000_000, "minutes", 90),
        (3_600_000_000_000, "hours", 1),
        (86_400_000_000_000, "days", 1),
    ],
)
def test_an_offset_is_carried_as_the_largest_exact_unit(nanoseconds: int, unit: str, count: int) -> None:
    """One row of `_DURATION_UNITS` per case, written out rather than derived from the table it checks.

    The app shows a shift as a number and a unit, so the unit chosen is visible to the user: nanoseconds
    everywhere would render as a wrong-looking edit. Asserting the count too — and parsing it back — is what
    catches a wrong size in the table, which a round-trip alone would hide by making the same error twice.
    """
    duration = _build_duration(nanoseconds)
    assert duration.type == unit
    assert getattr(getattr(duration, unit), unit).literal == count
    assert _parse_duration(duration) == nanoseconds


def test_build_spec_picks_the_duration_unit_for_an_input() -> None:
    """The unit choice above is what an input's shift is written with. Spelled out, not re-derived."""
    spec = _build_spec([DerivedDatasetInput("ri.a", offset=-5_000_000_000)])
    assert spec.combine is not None
    time_shift = spec.combine.inputs[0].time_shift
    assert time_shift is not None
    assert time_shift.offset == scout_compute_api.Duration(
        seconds=scout_compute_api.DurationSeconds(seconds=scout_compute_api.IntegerConstant(literal=-5))
    )


def test_build_spec_accepts_no_inputs() -> None:
    """A derived dataset can start out empty and be populated later."""
    assert _build_spec([]) == _combine()


# --- spec parsing ---


def test_parse_spec_round_trips_every_transform() -> None:
    """Parsing what the builder produced recovers the inputs, transforms and all."""
    inputs = (
        DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A", "B"), TagFilter.not_in("run", "7")]),
        DerivedDatasetInput("ri.b", (), ("source", "daq1"), timedelta(hours=1)),
        DerivedDatasetInput("ri.c"),
    )
    assert _parse_spec(_build_spec(inputs)) == inputs


def test_parse_spec_reads_a_hand_authored_definition() -> None:
    """A definition shaped the way another client would write it, not the way the builder does."""
    spec = _combine(
        _saved("ri.a"),
        _filter(_saved("ri.b"), _and(_tag_in("vehicle", "A"), _tag_not_in("run", "7"), _tag_in("site", "X", "Y"))),
    )
    assert _parse_spec(spec) == (
        DerivedDatasetInput("ri.a"),
        DerivedDatasetInput(
            "ri.b", [TagFilter.in_("vehicle", "A"), TagFilter.not_in("run", "7"), TagFilter.in_("site", "X", "Y")]
        ),
    )


def test_parse_spec_merges_nested_filters_outermost_last() -> None:
    """Clauses may arrive as separate filter nodes rather than one conjunction; both mean AND."""
    spec = _combine(_filter(_filter(_saved("ri.a"), _tag_in("vehicle", "A")), _tag_not_in("run", "7")))
    assert _parse_spec(spec) == (
        DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A"), TagFilter.not_in("run", "7")]),
    )


def test_parse_spec_sums_stacked_time_shifts() -> None:
    """Composed translations add, so summing them loses nothing the rebuild needs."""
    spec = _combine(_shift(_shift(_saved("ri.a"), 5), -2))
    assert _parse_spec(spec) == (DerivedDatasetInput("ri.a", offset=timedelta(seconds=3)),)


def test_parse_spec_keeps_one_key_constrained_twice() -> None:
    """Two clauses on one key are representable now, so they parse rather than being refused."""
    spec = _combine(_filter(_saved("ri.a"), _and(_tag_in("vehicle", "A"), _tag_in("vehicle", "B"))))
    assert _parse_spec(spec) == (
        DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A"), TagFilter.in_("vehicle", "B")]),
    )


@pytest.mark.parametrize(
    ("predicate", "clause"),
    [
        pytest.param(_tag_in("vehicle"), TagFilter.in_("vehicle"), id="in-nothing"),
        pytest.param(_tag_not_in("vehicle"), TagFilter.not_in("vehicle"), id="not-in-nothing"),
    ],
)
def test_parse_spec_accepts_an_empty_value_set(predicate: scout_compute_api.TagPredicate, clause: TagFilter) -> None:
    """`in []` matches nothing and `not in []` matches everything; both rebuild exactly as they arrived."""
    spec = _combine(_filter(_saved("ri.a"), predicate))
    parsed = _parse_spec(spec)
    assert parsed == (DerivedDatasetInput("ri.a", [clause]),)
    assert _build_spec(parsed) == spec


def test_parse_spec_refuses_a_filter_outside_the_tag_node() -> None:
    """A filter above the tag node tests the tag that node adds, which a flattened input cannot say.

    Parsing it would drop that ordering, and the rebuild would filter the source's own tags instead —
    a different set of series, committed silently. Refusing the read is what keeps the two inverses.
    """
    spec = _combine(_filter(_tagged(_saved("ri.a"), "source", "daq1"), _tag_in("source", "daq1")))
    with pytest.raises(ValueError, match="expected a saved dataset"):
        _parse_spec(spec)


def _variable_string() -> scout_compute_api.StringConstant:
    return scout_compute_api.StringConstant(variable="v")


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        pytest.param(_saved("ri.a"), "not a combination of input datasets", id="root-is-not-a-combine"),
        pytest.param(
            _combine(
                scout_compute_api.Dataset(
                    asset=scout_compute_api.Asset(rid=scout_compute_api.StringConstant(literal="ri.asset"))
                )
            ),
            "expected a saved dataset",
            id="input-is-an-asset",
        ),
        pytest.param(
            _combine(
                _filter(
                    scout_compute_api.Dataset(
                        asset=scout_compute_api.Asset(rid=scout_compute_api.StringConstant(literal="ri.asset"))
                    ),
                    _tag_in("vehicle", "A"),
                )
            ),
            "expected a saved dataset",
            id="unsupported-node-under-a-filter",
        ),
        pytest.param(
            _combine(_tagged(_tagged(_saved("ri.a"), "a", "1"), "b", "2")),
            "expected a saved dataset",
            id="two-added-tags",
        ),
        pytest.param(
            _combine(_filter(_tagged(_saved("ri.a"), "source", "daq1"), _tag_in("source", "daq1"))),
            "expected a saved dataset",
            id="filter-outside-the-tag-it-tests",
        ),
        pytest.param(
            _combine(scout_compute_api.Dataset(saved=scout_compute_api.SavedDataset(rid=_variable_string()))),
            "references the variable",
            id="variable-rid",
        ),
        pytest.param(
            _combine(
                _filter(
                    _saved("ri.a"),
                    scout_compute_api.TagPredicate(
                        in_=scout_compute_api.TagIn(
                            key=_variable_string(),
                            values=scout_compute_api.StringSetConstantV2(literal=[]),
                        )
                    ),
                )
            ),
            "references the variable",
            id="variable-tag-key",
        ),
        pytest.param(
            _combine(
                _filter(
                    _saved("ri.a"),
                    scout_compute_api.TagPredicate(
                        in_=scout_compute_api.TagIn(
                            key=scout_compute_api.StringConstant(literal="vehicle"),
                            values=scout_compute_api.StringSetConstantV2(variable="v"),
                        )
                    ),
                )
            ),
            "filters on the variable",
            id="variable-value-set",
        ),
        pytest.param(
            _combine(
                _filter(
                    _saved("ri.a"),
                    scout_compute_api.TagPredicate(
                        in_=scout_compute_api.TagIn(
                            key=scout_compute_api.StringConstant(literal="vehicle"),
                            values=scout_compute_api.StringSetConstantV2(literal=[_variable_string()]),
                        )
                    ),
                )
            ),
            "references the variable",
            id="variable-tag-value",
        ),
        pytest.param(
            _combine(
                scout_compute_api.Dataset(
                    time_shift=scout_compute_api.TimeShiftedDataset(
                        input=_saved("ri.a"),
                        offset=scout_compute_api.Duration(
                            negate=scout_compute_api.DurationNegate(
                                input=scout_compute_api.Duration(
                                    seconds=scout_compute_api.DurationSeconds(
                                        seconds=scout_compute_api.IntegerConstant(literal=5)
                                    )
                                )
                            )
                        ),
                    )
                )
            ),
            "expected a whole number of",
            id="arithmetic-duration",
        ),
        pytest.param(
            _combine(
                scout_compute_api.Dataset(
                    time_shift=scout_compute_api.TimeShiftedDataset(
                        input=_saved("ri.a"),
                        offset=scout_compute_api.Duration(
                            seconds=scout_compute_api.DurationSeconds(
                                seconds=scout_compute_api.IntegerConstant(variable="v")
                            )
                        ),
                    )
                )
            ),
            "references the variable",
            id="variable-duration",
        ),
    ],
)
def test_parse_spec_refuses_what_it_cannot_faithfully_rebuild(spec: scout_compute_api.Dataset, message: str) -> None:
    """An edit rebuilds the whole definition from the parse, so anything unrepresentable must stop the read."""
    with pytest.raises(ValueError, match=message):
        _parse_spec(spec)


# --- dataset methods ---


def test_get_definition_returns_the_raw_definition(mock_dataset: DerivedDataset, mock_clients: MagicMock) -> None:
    """The spec comes back as-is, so a definition the input model cannot parse is still readable."""
    definition = _definition(_saved("ri.a"))
    mock_clients.catalog.get_dataset_derived_definition.return_value = definition

    assert mock_dataset.get_definition() is definition
    assert mock_clients.catalog.get_dataset_derived_definition.call_args[0] == (
        "Bearer test-token",
        "ri.catalog.ws.dataset.derived",
        None,
    )


def test_get_definition_at_a_commit(mock_dataset: DerivedDataset, mock_clients: MagicMock) -> None:
    mock_dataset.get_definition("ri.commit.0")
    assert mock_clients.catalog.get_dataset_derived_definition.call_args[0][2] == "ri.commit.0"


def test_commit_definition_sends_the_spec_and_the_commit_it_was_based_on(
    mock_dataset: DerivedDataset, mock_clients: MagicMock
) -> None:
    spec = _shift(_saved("ri.a"), 10)
    committed = _definition(spec, commit_id="ri.commit.2")
    mock_clients.catalog.commit_derived_definition.return_value = committed

    assert mock_dataset.commit_definition(spec, "shift by 10s", latest_commit="ri.commit.1") is committed

    _, rid, request = mock_clients.catalog.commit_derived_definition.call_args[0]
    assert rid == "ri.catalog.ws.dataset.derived"
    assert request.spec == spec
    assert request.message == "shift by 10s"
    assert request.latest_commit == "ri.commit.1"


def test_commit_definition_without_a_base_commit(mock_dataset: DerivedDataset, mock_clients: MagicMock) -> None:
    mock_dataset.commit_definition(_saved("ri.a"), "overwrite")
    assert mock_clients.catalog.commit_derived_definition.call_args[0][2].latest_commit is None


def test_list_input_datasets_reads_the_latest_definition(mock_dataset: DerivedDataset, mock_clients: MagicMock) -> None:
    mock_clients.catalog.get_dataset_derived_definition.return_value = _definition(
        _build_spec([DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A")])])
    )
    assert mock_dataset.list_input_datasets() == (DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A")]),)
    assert mock_clients.catalog.get_dataset_derived_definition.call_args[0] == (
        "Bearer test-token",
        "ri.catalog.ws.dataset.derived",
        None,
    )


def test_add_input_dataset_appends_and_commits(mock_dataset: DerivedDataset, mock_clients: MagicMock) -> None:
    """The new input is appended to the existing ones and committed against the commit that was read."""
    mock_clients.catalog.get_dataset_derived_definition.return_value = _definition(
        _build_spec([DerivedDatasetInput("ri.a")]), commit_id="ri.commit.1"
    )
    sent = (DerivedDatasetInput("ri.a"), DerivedDatasetInput("ri.b", [TagFilter.in_("vehicle", "B")]))
    # Deliberately not what was sent: the caller must be told the state the server committed.
    server_state = (*sent, DerivedDatasetInput("ri.c"))
    mock_clients.catalog.commit_derived_definition.return_value = _definition(_build_spec(server_state))

    added = DerivedDatasetInput.create("ri.b", filters=[TagFilter.in_("vehicle", "B")])
    assert mock_dataset.add_input_dataset(added) == server_state

    _, rid, request = mock_clients.catalog.commit_derived_definition.call_args[0]
    assert rid == "ri.catalog.ws.dataset.derived"
    assert request.spec == _build_spec(sent)
    assert request.latest_commit == "ri.commit.1"
    assert request.message == "Add input dataset ri.b"


def test_add_input_dataset_accepts_a_dataset_and_a_message(
    mock_dataset: DerivedDataset, mock_clients: MagicMock
) -> None:
    mock_clients.catalog.get_dataset_derived_definition.return_value = _definition(_build_spec([]))
    mock_clients.catalog.commit_derived_definition.return_value = _definition(
        _build_spec([DerivedDatasetInput("ri.b")])
    )
    other = MagicMock()
    other.rid = "ri.b"

    mock_dataset.add_input_dataset(DerivedDatasetInput.create(other), message="pull in telemetry")

    _, _, request = mock_clients.catalog.commit_derived_definition.call_args[0]
    assert request.spec == _build_spec([DerivedDatasetInput("ri.b")])
    assert request.message == "pull in telemetry"


def test_add_input_dataset_carries_every_transform(mock_dataset: DerivedDataset, mock_clients: MagicMock) -> None:
    """Filters, an added tag and an offset all reach the committed spec."""
    mock_clients.catalog.get_dataset_derived_definition.return_value = _definition(_build_spec([]))
    added = DerivedDatasetInput("ri.b", [TagFilter.not_in("vehicle", "B")], ("source", "daq1"), timedelta(seconds=-5))
    mock_clients.catalog.commit_derived_definition.return_value = _definition(_build_spec([added]))

    assert mock_dataset.add_input_dataset(added) == (added,)

    _, _, request = mock_clients.catalog.commit_derived_definition.call_args[0]
    assert request.spec == _build_spec([added])


def test_an_unreadable_definition_is_never_written_back(mock_dataset: DerivedDataset, mock_clients: MagicMock) -> None:
    """The parse guards the write: a definition the model cannot represent must not be rebuilt and committed."""
    unreadable = _combine(
        scout_compute_api.Dataset(
            asset=scout_compute_api.Asset(rid=scout_compute_api.StringConstant(literal="ri.asset"))
        )
    )
    mock_clients.catalog.get_dataset_derived_definition.return_value = _definition(unreadable)

    with pytest.raises(ValueError, match="expected a saved dataset"):
        mock_dataset.add_input_dataset(DerivedDatasetInput("ri.b"))
    with pytest.raises(ValueError, match="expected a saved dataset"):
        mock_dataset.remove_input_dataset("ri.b")
    mock_clients.catalog.commit_derived_definition.assert_not_called()


def test_remove_input_dataset_drops_every_appearance(mock_dataset: DerivedDataset, mock_clients: MagicMock) -> None:
    """A dataset added twice under different tag filters is removed in both places."""
    mock_clients.catalog.get_dataset_derived_definition.return_value = _definition(
        _build_spec(
            [
                DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A")]),
                DerivedDatasetInput("ri.b"),
                DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "B")]),
            ]
        )
    )
    mock_clients.catalog.commit_derived_definition.return_value = _definition(
        _build_spec([DerivedDatasetInput("ri.b")])
    )

    assert mock_dataset.remove_input_dataset("ri.a") == (DerivedDatasetInput("ri.b"),)

    _, _, request = mock_clients.catalog.commit_derived_definition.call_args[0]
    assert request.spec == _build_spec([DerivedDatasetInput("ri.b")])
    assert request.message == "Remove input dataset ri.a"


def test_remove_input_dataset_drops_only_the_appearance_it_was_given(
    mock_dataset: DerivedDataset, mock_clients: MagicMock
) -> None:
    """Passing back one of the listed inputs removes that alignment alone, not the dataset's other ones."""
    kept = DerivedDatasetInput("ri.a", offset=timedelta(seconds=5))
    dropped = DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A")])
    mock_clients.catalog.get_dataset_derived_definition.return_value = _definition(_build_spec([kept, dropped]))
    mock_clients.catalog.commit_derived_definition.return_value = _definition(_build_spec([kept]))

    assert mock_dataset.remove_input_dataset(dropped) == (kept,)

    _, _, request = mock_clients.catalog.commit_derived_definition.call_args[0]
    assert request.spec == _build_spec([kept])
    assert request.message == "Remove input dataset ri.a"


def test_remove_input_dataset_rejects_an_appearance_that_is_not_there(
    mock_dataset: DerivedDataset, mock_clients: MagicMock
) -> None:
    """An input equal to none of the listed ones is an error, even though its dataset is an input."""
    mock_clients.catalog.get_dataset_derived_definition.return_value = _definition(
        _build_spec([DerivedDatasetInput("ri.a")])
    )
    with pytest.raises(ValueError, match="is not an input of derived dataset"):
        mock_dataset.remove_input_dataset(DerivedDatasetInput("ri.a", offset=timedelta(seconds=5)))
    mock_clients.catalog.commit_derived_definition.assert_not_called()


def test_remove_input_dataset_can_empty_the_definition(mock_dataset: DerivedDataset, mock_clients: MagicMock) -> None:
    """Removing the last input commits an empty combine rather than refusing."""
    mock_clients.catalog.get_dataset_derived_definition.return_value = _definition(
        _build_spec([DerivedDatasetInput("ri.a")])
    )
    mock_clients.catalog.commit_derived_definition.return_value = _definition(_build_spec([]))

    assert mock_dataset.remove_input_dataset("ri.a") == ()

    _, _, request = mock_clients.catalog.commit_derived_definition.call_args[0]
    assert request.spec == _build_spec([])


def test_remove_input_dataset_rejects_a_dataset_that_is_not_an_input(
    mock_dataset: DerivedDataset, mock_clients: MagicMock
) -> None:
    """Removing something that was never an input is an error, not a silent no-op commit."""
    mock_clients.catalog.get_dataset_derived_definition.return_value = _definition(
        _build_spec([DerivedDatasetInput("ri.a")])
    )
    with pytest.raises(ValueError, match="is not an input of derived dataset"):
        mock_dataset.remove_input_dataset("ri.b")
    mock_clients.catalog.commit_derived_definition.assert_not_called()


# --- construction ---


def test_a_dataset_with_a_derived_definition_is_built_as_a_derived_dataset(
    mock_clients: MagicMock, make_enriched_dataset: Callable[..., scout_catalog.EnrichedDataset]
) -> None:
    """Every dataset lookup builds through `_dataset_from_conjure`, so a derived one arrives typed as one."""
    enriched = make_enriched_dataset(derived_definition=_definition(_build_spec([])))
    assert isinstance(_dataset_from_conjure(mock_clients, enriched), DerivedDataset)


def test_an_ordinary_dataset_is_not(
    mock_clients: MagicMock, make_enriched_dataset: Callable[..., scout_catalog.EnrichedDataset]
) -> None:
    assert type(_dataset_from_conjure(mock_clients, make_enriched_dataset())) is Dataset


def test_refresh_keeps_a_derived_dataset_derived(
    mock_dataset: DerivedDataset,
    mock_clients: MagicMock,
    make_enriched_dataset: Callable[..., scout_catalog.EnrichedDataset],
) -> None:
    """`refresh()` rebuilds through `type(self)`, so it must update in place and stay the same class."""
    latest = make_enriched_dataset(
        "ri.catalog.ws.dataset.derived", name="Renamed", derived_definition=_definition(_build_spec([]))
    )
    mock_clients.catalog.get_enriched_datasets.return_value = [latest]

    refreshed = mock_dataset.refresh()

    assert refreshed is mock_dataset
    assert type(refreshed) is DerivedDataset
    assert refreshed.name == "Renamed"


# --- creation ---


def test_create_derived_dataset_sets_the_definition_on_the_create_request(
    client: NominalClient, mock_clients: MagicMock, make_enriched_dataset: Callable[..., scout_catalog.EnrichedDataset]
) -> None:
    """Inputs are bridged into the create request's derived definition rather than committed separately."""
    mock_clients.resolve_default_workspace_rid.return_value = "ri.workspace.w"
    mock_clients.catalog.create_dataset.return_value = make_enriched_dataset(
        derived_definition=_definition(_build_spec([]))
    )
    inputs = [DerivedDatasetInput("ri.a", [TagFilter.in_("vehicle", "A")]), DerivedDatasetInput("ri.b")]

    derived = client.create_derived_dataset("merged", inputs=inputs, labels=["a"], properties={"k": "v"})

    assert isinstance(derived, DerivedDataset)

    _, request = mock_clients.catalog.create_dataset.call_args[0]
    assert request.derived_definition.spec == _build_spec(inputs)
    assert request.derived_definition.message == "Initial derived definition"
    assert request.workspace == "ri.workspace.w"
    assert request.labels == ["a"]
    assert request.properties == {"k": "v"}


def test_create_derived_dataset_defaults_to_no_inputs(
    client: NominalClient, mock_clients: MagicMock, make_enriched_dataset: Callable[..., scout_catalog.EnrichedDataset]
) -> None:
    mock_clients.catalog.create_dataset.return_value = make_enriched_dataset()

    client.create_derived_dataset("merged", message="empty for now")

    _, request = mock_clients.catalog.create_dataset.call_args[0]
    assert request.derived_definition.spec == _build_spec([])
    assert request.derived_definition.message == "empty for now"
