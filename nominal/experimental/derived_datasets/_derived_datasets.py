from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from functools import reduce
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence

from nominal_api import scout_catalog, scout_compute_api
from typing_extensions import Self

from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core.client import NominalClient
from nominal.core.dataset import Dataset, _create_dataset_request, _get_dataset
from nominal.core.datasource import DataSource
from nominal.core.marking import Marking, _marking_rids
from nominal.ts import IntegralNanosecondsDuration, _to_seconds_nanos_duration


@dataclass(frozen=True)
class TagFilter:
    """One filter clause on an input dataset: keep, or drop, the series whose `key` tag is one of `values`.

    Build one with `TagFilter.in_` or `TagFilter.not_in`. Clauses on the same input are ANDed: a series is
    included only if every clause accepts it.
    """

    key: str
    """Tag key the clause tests."""
    values: Sequence[str]
    """Values the tag is tested against."""
    exclude: bool = False
    """Whether the clause drops the matching series rather than keeping them."""

    def __post_init__(self) -> None:
        """Freeze the values."""
        object.__setattr__(self, "values", tuple(self.values))

    @classmethod
    def in_(cls, key: str, values: str | Sequence[str]) -> Self:
        """Keep only the series whose `key` tag is `values`, or one of them when several are given.

        Raises:
            ValueError: If `values` is empty, which would select nothing.
        """
        return cls(key, _at_least_one(values))

    @classmethod
    def not_in(cls, key: str, values: str | Sequence[str]) -> Self:
        """Drop the series whose `key` tag is `values`, or one of them when several are given.

        Raises:
            ValueError: If `values` is empty, which would drop nothing.
        """
        return cls(key, _at_least_one(values), exclude=True)


def _at_least_one(values: str | Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, str):
        return (values,)
    if not values:
        raise ValueError("a tag filter needs at least one value")
    return tuple(values)


@dataclass(frozen=True)
class DerivedDatasetInput:
    """One input dataset of a derived dataset, with the per-input transforms applied to it.

    The transforms match those the app offers on a composition's input, and apply in this order: the filters
    select series, `add_tag` labels what survives, and `offset` shifts the timestamps.

    Use `DerivedDatasetInput.create` to build one from a `Dataset` rather than a RID.
    """

    dataset_rid: str
    """RID of the input dataset."""
    filters: Sequence[TagFilter] = ()
    """Clauses narrowing which series are included, ANDed together. Empty means the whole dataset."""
    add_tag: tuple[str, str] | None = None
    """A tag key and value to attach to this input's series, distinguishing them from the other inputs'."""
    offset: timedelta | IntegralNanosecondsDuration = 0
    """Shift every timestamp on this input by this much; positive moves it later. Stored as nanoseconds."""

    def __post_init__(self) -> None:
        """Freeze the filters and reduce the offset to nanoseconds, so inputs built either way compare equal."""
        object.__setattr__(self, "filters", tuple(self.filters))
        object.__setattr__(self, "offset", _offset_nanos(self.offset))

    @classmethod
    def create(
        cls,
        dataset: Dataset | str,
        *,
        filters: Sequence[TagFilter] = (),
        add_tag: tuple[str, str] | None = None,
        offset: timedelta | IntegralNanosecondsDuration = 0,
    ) -> Self:
        """An input naming `dataset`, with the given transforms applied to it."""
        return cls(rid_from_instance_or_string(dataset), filters, add_tag, offset)


def _edit_inputs(
    clients: DataSource._Clients,
    dataset_rid: str,
    edit: Callable[[Sequence[DerivedDatasetInput]], Sequence[DerivedDatasetInput]],
    message: str,
) -> Sequence[DerivedDatasetInput]:
    """Rewrite a derived dataset's inputs as a new commit."""
    definition = _get_definition(clients, dataset_rid)
    inputs = edit(_parse_spec(definition.spec))
    committed = _commit_definition(clients, dataset_rid, _build_spec(inputs), message, definition.commit.id)
    return _parse_spec(committed.spec)


def _get_definition(
    clients: DataSource._Clients, dataset_rid: str, commit: str | None = None
) -> scout_catalog.DerivedDefinition:
    return clients.catalog.get_dataset_derived_definition(clients.auth_header, dataset_rid, commit)


def _commit_definition(
    clients: DataSource._Clients,
    dataset_rid: str,
    spec: scout_compute_api.Dataset,
    message: str,
    latest_commit: str | None,
) -> scout_catalog.DerivedDefinition:
    """Replace a derived dataset's definition. `latest_commit` refuses the write if the definition moved on."""
    request = scout_catalog.CommitDerivedDefinitionRequest(
        spec=spec,
        message=message,
        latest_commit=latest_commit,
    )
    return clients.catalog.commit_derived_definition(clients.auth_header, dataset_rid, request)


def _build_spec(inputs: Sequence[DerivedDatasetInput]) -> scout_compute_api.Dataset:
    return scout_compute_api.Dataset(
        combine=scout_compute_api.CombinedDataset(inputs=[_build_input(item) for item in inputs])
    )


def _build_input(dataset_input: DerivedDatasetInput) -> scout_compute_api.Dataset:
    """Wrap the saved dataset in a node per transform, innermost first, skipping the ones it does not use."""
    spec = scout_compute_api.Dataset(saved=scout_compute_api.SavedDataset(rid=_literal(dataset_input.dataset_rid)))
    predicate = _build_predicate(dataset_input.filters)
    if predicate is not None:
        spec = scout_compute_api.Dataset(filter=scout_compute_api.FilteredDataset(input=spec, predicate=predicate))
    if dataset_input.add_tag is not None:
        key, value = dataset_input.add_tag
        spec = scout_compute_api.Dataset(
            tag=scout_compute_api.TaggedDataset(input=spec, key=_literal(key), value=_literal(value))
        )
    offset = _offset_nanos(dataset_input.offset)
    if offset:
        spec = scout_compute_api.Dataset(
            time_shift=scout_compute_api.TimeShiftedDataset(input=spec, offset=_build_duration(offset))
        )
    return spec


def _build_predicate(filters: Sequence[TagFilter]) -> scout_compute_api.TagPredicate | None:
    predicates = [_build_clause(clause) for clause in filters]
    if not predicates:
        return None
    return reduce(
        lambda left, right: scout_compute_api.TagPredicate(and_=scout_compute_api.TagAnd(left=left, right=right)),
        predicates,
    )


def _build_clause(clause: TagFilter) -> scout_compute_api.TagPredicate:
    key = _literal(clause.key)
    values = scout_compute_api.StringSetConstantV2(literal=[_literal(value) for value in clause.values])
    if clause.exclude:
        return scout_compute_api.TagPredicate(not_in=scout_compute_api.TagNotIn(key=key, values=values))
    return scout_compute_api.TagPredicate(in_=scout_compute_api.TagIn(key=key, values=values))


def _parse_spec(spec: scout_compute_api.Dataset) -> Sequence[DerivedDatasetInput]:
    if spec.combine is None:
        raise ValueError(
            f"derived definition is a {spec.type!r} dataset, not a combination of input datasets: it was authored "
            "outside of the input-dataset API and cannot be read or edited by it. Use `get_definition` and "
            "`commit_definition` to work with it as a compute spec"
        )
    return tuple(_parse_input(input_spec) for input_spec in spec.combine.inputs)


def _parse_input(spec: scout_compute_api.Dataset) -> DerivedDatasetInput:
    """Peel an input's transform nodes in the order `_build_input` nests them.

    Repeats within a layer collapse: shifts add, filters AND. A filter outside the tag node would test the
    tag that node adds, which a flat input cannot express, so that nesting is refused rather than reordered.
    """
    offset, spec = _peel_time_shifts(spec)
    add_tag, spec = _peel_tag(spec)
    filters, spec = _peel_filters(spec)
    if spec.saved is None:
        raise ValueError(f"derived definition input is a {spec.type!r} dataset, expected a saved dataset")
    return DerivedDatasetInput(_literal_value(spec.saved.rid), filters, add_tag, offset)


def _peel_time_shifts(spec: scout_compute_api.Dataset) -> tuple[int, scout_compute_api.Dataset]:
    offset = 0
    while spec.time_shift is not None:
        offset += _parse_duration(spec.time_shift.offset)
        spec = spec.time_shift.input
    return offset, spec


def _peel_tag(spec: scout_compute_api.Dataset) -> tuple[tuple[str, str] | None, scout_compute_api.Dataset]:
    if spec.tag is None:
        return None, spec
    return (_literal_value(spec.tag.key), _literal_value(spec.tag.value)), spec.tag.input


def _peel_filters(spec: scout_compute_api.Dataset) -> tuple[Sequence[TagFilter], scout_compute_api.Dataset]:
    filters: Sequence[TagFilter] = ()
    while spec.filter is not None:
        filters = (*_parse_predicate(spec.filter.predicate), *filters)
        spec = spec.filter.input
    return filters, spec


def _parse_predicate(predicate: scout_compute_api.TagPredicate) -> Sequence[TagFilter]:
    """The filter clauses a conjunction of `in`/`notIn` predicates stands for, left to right."""
    if predicate.and_ is not None:
        return (*_parse_predicate(predicate.and_.left), *_parse_predicate(predicate.and_.right))
    if predicate.in_ is not None:
        return (TagFilter(_literal_value(predicate.in_.key), _literal_values(predicate.in_.values)),)
    if predicate.not_in is not None:
        return (
            TagFilter(_literal_value(predicate.not_in.key), _literal_values(predicate.not_in.values), exclude=True),
        )
    raise ValueError(
        f"derived definition input uses a {predicate.type!r} tag predicate, expected 'in', 'notIn' or 'and'"
    )


_DURATION_UNITS: Sequence[tuple[str, int, type[Any]]] = (
    ("days", 86_400_000_000_000, scout_compute_api.DurationDays),
    ("hours", 3_600_000_000_000, scout_compute_api.DurationHours),
    ("minutes", 60_000_000_000, scout_compute_api.DurationMinutes),
    ("seconds", 1_000_000_000, scout_compute_api.DurationSeconds),
    ("milliseconds", 1_000_000, scout_compute_api.DurationMilliseconds),
    ("nanoseconds", 1, scout_compute_api.DurationNanoseconds),
)
"""Each unit's nanosecond size and the conjure variant carrying it, largest first.

`Duration` and every variant name their field after the unit, which is what lets one entry drive both.
"""


def _offset_nanos(offset: timedelta | IntegralNanosecondsDuration) -> int:
    seconds, nanos = _to_seconds_nanos_duration(offset)
    return seconds * 1_000_000_000 + nanos


def _build_duration(nanoseconds: int) -> scout_compute_api.Duration:
    """The offset in the largest unit that divides it exactly, which is how the app presents it back."""
    for unit, size, variant in _DURATION_UNITS:
        if nanoseconds % size == 0:
            constant = scout_compute_api.IntegerConstant(literal=nanoseconds // size)
            return scout_compute_api.Duration(**{unit: variant(**{unit: constant})})
    raise AssertionError("unreachable: every integer is a whole number of nanoseconds")


def _parse_duration(duration: scout_compute_api.Duration) -> int:
    """The offset in nanoseconds, refusing the arithmetic and variable forms rather than rewriting them."""
    for unit, size, _ in _DURATION_UNITS:
        value = getattr(duration, unit)
        if value is not None:
            return _literal_int(getattr(value, unit)) * size
    raise ValueError(
        f"derived definition input is shifted by a {duration.type!r} duration, expected a whole number of "
        "nanoseconds, milliseconds, seconds, minutes, hours or days"
    )


def _literal(value: str) -> scout_compute_api.StringConstant:
    return scout_compute_api.StringConstant(literal=value)


def _literal_value(constant: scout_compute_api.StringConstant) -> str:
    if constant.literal is None:
        raise ValueError(f"derived definition references the variable {constant.variable!r}, expected a literal")
    return constant.literal


def _literal_int(constant: scout_compute_api.IntegerConstant) -> int:
    if constant.literal is None:
        raise ValueError(f"derived definition references the variable {constant.variable!r}, expected a literal")
    return constant.literal


def _literal_values(values: scout_compute_api.StringSetConstantV2) -> Sequence[str]:
    if values.literal is None:
        raise ValueError(f"derived definition filters on the variable {values.variable!r}, expected a literal set")
    return tuple(_literal_value(value) for value in values.literal)


@dataclass(frozen=True)
class DerivedDataset:
    """A dataset whose contents are computed from a definition instead of ingested files.

    Returned by `create_derived_dataset`, and by `get_derived_dataset` for a dataset the server reports
    as derived. The `*_input_dataset*` methods read and edit a definition authored as a union of input
    datasets, each optionally filtered, tagged and time-shifted — the same composition the app builds.
    `get_definition` and `commit_definition` work with any definition as a raw compute spec.

    It is not a `Dataset`: nothing can be ingested into it. To read its data or attach it to an asset or a
    run, look it up as a regular dataset with `NominalClient.get_dataset` by `rid`.
    """

    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    is_archived: bool
    _clients: DataSource._Clients = field(repr=False)

    @classmethod
    def _from_conjure(cls, clients: DataSource._Clients, dataset: scout_catalog.EnrichedDataset) -> Self:
        return cls(
            rid=dataset.rid,
            name=dataset.name,
            description=dataset.description,
            properties=MappingProxyType(dataset.properties),
            labels=tuple(dataset.labels),
            is_archived=dataset.is_archived,
            _clients=clients,
        )

    def archive(self) -> None:
        """Archive this dataset. Archived datasets are not deleted, but are hidden from the UI."""
        self._clients.catalog.archive_dataset(self._clients.auth_header, self.rid)

    def get_definition(self, commit: str | None = None) -> scout_catalog.DerivedDefinition:
        """Fetch this dataset's derived definition: its compute spec and the commit that produced it.

        Args:
            commit: If provided, the definition at this commit rather than the latest.

        Returns:
            The derived definition at the requested commit, or the latest if none was given.
        """
        return _get_definition(self._clients, self.rid, commit)

    def commit_definition(
        self, spec: scout_compute_api.Dataset, message: str, *, latest_commit: str | None = None
    ) -> scout_catalog.DerivedDefinition:
        """Replace this dataset's derived definition with `spec`, as a new commit.

        Args:
            spec: The compute spec defining the new contents.
            message: Commit message describing the change.
            latest_commit: If provided, the commit this change was based on. The server refuses the write
                if the definition has moved on since.

        Returns:
            The newly committed derived definition.
        """
        return _commit_definition(self._clients, self.rid, spec, message, latest_commit)

    def list_input_datasets(self) -> Sequence[DerivedDatasetInput]:
        """List the input datasets this derived dataset is composed of, with the transforms applied to each.

        Returns:
            One `DerivedDatasetInput` per appearance in the definition, in definition order.

        Raises:
            ValueError: If this dataset's definition was authored as something other than a combination of
                input datasets with the per-input transforms this API models.
        """
        return _parse_spec(_get_definition(self._clients, self.rid).spec)

    def add_input_dataset(
        self, dataset_input: DerivedDatasetInput, *, message: str | None = None
    ) -> Sequence[DerivedDatasetInput]:
        """Add an input dataset to this derived dataset, as a new commit on its definition.

        Args:
            dataset_input: The dataset to add, with its transforms. Build one with `DerivedDatasetInput.create`,
                which takes a `Dataset` or a RID.
            message: Commit message describing the change. Defaults to naming the added dataset.

        Returns:
            The derived dataset's input datasets after the change.

        Raises:
            ValueError: If this dataset's definition was authored as something other than a combination of
                input datasets.
            conjure_python_client.ConjureHTTPError: If the definition was committed to by someone else since
                this call read it.
        """
        return _edit_inputs(
            self._clients,
            self.rid,
            lambda existing: (*existing, dataset_input),
            message or f"Add input dataset {dataset_input.dataset_rid}",
        )

    def remove_input_dataset(
        self, dataset: Dataset | str | DerivedDatasetInput, *, message: str | None = None
    ) -> Sequence[DerivedDatasetInput]:
        """Remove an input dataset from this derived dataset, as a new commit on its definition.

        Pass one of the inputs `list_input_datasets` returned to remove the appearances equal to it, which
        differ from the others by their transforms. Passing a dataset or a RID removes every appearance of
        it — a dataset added more than once, under different filters or at different offsets, is how a
        single source is aligned against itself.

        Args:
            dataset: The input to remove, or the dataset (or its RID) to remove every appearance of.
            message: Commit message describing the change. Defaults to naming the removed dataset.

        Returns:
            The derived dataset's input datasets after the change.

        Raises:
            ValueError: If nothing matching is an input of this one, or if this dataset's definition was
                authored as something other than a combination of input datasets.
            conjure_python_client.ConjureHTTPError: If the definition was committed to by someone else since
                this call read it.
        """
        if isinstance(dataset, DerivedDatasetInput):
            rid = dataset.dataset_rid

            def matches(item: DerivedDatasetInput) -> bool:
                return item == dataset

        else:
            rid = rid_from_instance_or_string(dataset)

            def matches(item: DerivedDatasetInput) -> bool:
                return item.dataset_rid == rid

        def without_the_target(existing: Sequence[DerivedDatasetInput]) -> Sequence[DerivedDatasetInput]:
            remaining = tuple(item for item in existing if not matches(item))
            if len(remaining) == len(existing):
                raise ValueError(f"dataset {rid!r} is not an input of derived dataset {self.rid!r}")
            return remaining

        return _edit_inputs(self._clients, self.rid, without_the_target, message or f"Remove input dataset {rid}")

    def set_input_datasets(
        self, inputs: Sequence[DerivedDatasetInput], *, message: str | None = None
    ) -> Sequence[DerivedDatasetInput]:
        """Replace this derived dataset's input datasets wholesale, as a new commit on its definition.

        Use this to change an input's transforms, reorder the inputs, or make several changes in one commit.
        Start from `list_input_datasets` and pass back the edited sequence.

        Args:
            inputs: The inputs the definition should have after the change, in order. May be empty.
            message: Commit message describing the change. Defaults to counting the inputs.

        Returns:
            The derived dataset's input datasets after the change.

        Raises:
            ValueError: If this dataset's definition was authored as something other than a combination of
                input datasets, which this method would otherwise overwrite.
            conjure_python_client.ConjureHTTPError: If the definition was committed to by someone else since
                this call read it.
        """
        return _edit_inputs(
            self._clients,
            self.rid,
            lambda _existing: tuple(inputs),
            message or f"Set {len(inputs)} input dataset(s)",
        )


def _create_derived_dataset(
    clients: DataSource._Clients,
    name: str,
    spec: scout_compute_api.Dataset,
    *,
    message: str,
    description: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
    markings: Sequence[Marking | str] | None = None,
) -> DerivedDataset:
    """Create a dataset whose contents are computed from `spec` rather than ingested from files.

    The single creation path behind both `create_derived_dataset` here and
    `nominal.experimental.compute_as_code.create_derived_dataset`, which differ only in how they build `spec`.
    """
    request = _create_dataset_request(
        name,
        description=description,
        labels=labels,
        properties=properties,
        workspace_rid=clients.resolve_default_workspace_rid(),
        marking_rids=_marking_rids(markings),
        derived_definition=scout_catalog.CreateDerivedDefinition(spec=spec, message=message),
    )
    return DerivedDataset._from_conjure(clients, clients.catalog.create_dataset(clients.auth_header, request))


def create_derived_dataset(
    client: NominalClient,
    name: str,
    *,
    inputs: Sequence[DerivedDatasetInput] = (),
    description: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
    markings: Sequence[Marking | str] | None = None,
    message: str = "Initial derived definition",
) -> DerivedDataset:
    """Create a derived dataset: a virtual dataset whose contents are the union of its input datasets.

    Args:
        client: The NominalClient to use for creating the derived dataset.
        name: Name of the derived dataset to create in Nominal.
        inputs: The datasets this one is composed of, each with its own transforms. May be empty, and
            populated later with `DerivedDataset.add_input_dataset`.
        description: Human readable description of the dataset.
        labels: Text labels to apply to the created dataset.
        properties: Key-value properties to apply to the created dataset.
        markings: If present, markings (or marking RIDs) applied to the dataset. Sent as part of
            the creation request rather than applied in a follow-up call.
        message: Commit message for the initial definition.

    Returns:
        Reference to the created derived dataset in Nominal.
    """
    return _create_derived_dataset(
        client._clients,
        name,
        _build_spec(inputs),
        message=message,
        description=description,
        labels=labels,
        properties=properties,
        markings=markings,
    )


def get_derived_dataset(client: NominalClient, dataset: Dataset | str) -> DerivedDataset:
    """Retrieve a derived dataset by RID, or as the derived counterpart of a `Dataset` looked up through core.

    Raises:
        ValueError: If the dataset is not derived.
    """
    rid = rid_from_instance_or_string(dataset)
    response = _get_dataset(client._clients.auth_header, client._clients.catalog, rid)
    if response.derived_definition is None:
        raise ValueError(f"dataset {rid!r} is not a derived dataset")
    return DerivedDataset._from_conjure(client._clients, response)
