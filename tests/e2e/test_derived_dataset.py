"""End-to-end tests for derived datasets: virtual datasets composed from tag-filtered input datasets.

Covers:
  - Creating a derived dataset with inputs, and with no inputs
  - Looking one up afterwards and getting a `DerivedDataset` back
  - Listing the inputs back, tags included
  - Adding and removing inputs, each as a new commit on the definition
  - Reading the raw definition back and replacing it with a commit, at and against a specific commit
  - Reading data through a derived dataset, to confirm the union, the tag filter, the added tag and the
    time shift all take effect server-side, by the values that come back rather than by row counts
  - Listing one dataset twice, which the definition keeps but a channel export reads as one series

The `tagged_datasets` fixture is session-scoped: two datasets are ingested once, each tagged with a
different `vehicle` value, and reused by every test here.
"""

from __future__ import annotations

from datetime import timedelta
from io import BytesIO
from typing import Callable, Iterator, Mapping, Sequence
from uuid import uuid4

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import IngestStatus
from nominal.core.derived_dataset import DerivedDataset, DerivedDatasetInput, TagFilter, _build_spec
from nominal.thirdparty.pandas import datasource_to_dataframe
from tests.e2e import POLL_INTERVAL

ArchiveFn = Callable[[object], None]


def _archive_all(datasets: Sequence[Dataset]) -> None:
    """Archive every dataset, so one failure cannot strand the rest, and report the ones that failed.

    Raising here turns a leaked dataset into a visible teardown error rather than a silent one left on the
    stack for someone to find later.
    """
    failures = []
    for dataset in datasets:
        try:
            dataset.archive()
        except Exception as error:  # noqa: BLE001 — teardown reports every failure rather than stopping at one
            failures.append(f"{dataset.rid}: {error!r}")
    if failures:
        raise RuntimeError("failed to archive e2e datasets: " + "; ".join(failures))


def _temperatures(dataset: Dataset, tags: Mapping[str, str] | None = None) -> list[float]:
    """Every temperature reading in a datasource, sorted — the values identify which input they came from.

    `tags` narrows the export to the series carrying them, which is how a tag applied server-side is read back.
    """
    frame = datasource_to_dataframe(dataset, channel_exact_match=["temperature"], tags=tags)
    return sorted(frame["temperature"].dropna().tolist())


def _expected_temperatures(*csvs: bytes) -> list[float]:
    return sorted(float(line.split(b",")[2]) for csv in csvs for line in csv.strip().splitlines()[1:])


@pytest.fixture(scope="session")
def tagged_datasets(client: NominalClient, csv_data: bytes, csv_data2: bytes) -> Iterator[tuple[Dataset, Dataset]]:
    """Two ingested datasets carrying `vehicle=A` and `vehicle=B` respectively, over disjoint time ranges."""
    datasets: list[Dataset] = []
    try:
        for vehicle, data in (("A", csv_data), ("B", csv_data2)):
            dataset = client.create_dataset(f"derived-input-{vehicle.lower()}-{uuid4().hex[:8]}")
            # Registered before the ingest it might not survive: a dataset that fails to ingest still exists.
            datasets.append(dataset)
            dataset_file = dataset.add_from_io(
                BytesIO(data), "timestamp", "iso_8601", tags={"vehicle": vehicle}
            ).poll_until_ingestion_completed(interval=POLL_INTERVAL)
            assert dataset_file.ingest_status == IngestStatus.SUCCESS
        yield datasets[0], datasets[1]
    finally:
        _archive_all(datasets)


@pytest.fixture(scope="session")
def mixed_dataset(client: NominalClient, csv_data: bytes, csv_data2: bytes) -> Iterator[Dataset]:
    """One dataset holding both tag values, so filtering it proves series selection rather than input exclusion."""
    dataset = client.create_dataset(f"derived-input-mixed-{uuid4().hex[:8]}")
    try:
        for vehicle, data in (("A", csv_data), ("B", csv_data2)):
            dataset_file = dataset.add_from_io(
                BytesIO(data), "timestamp", "iso_8601", tags={"vehicle": vehicle}
            ).poll_until_ingestion_completed(interval=POLL_INTERVAL)
            assert dataset_file.ingest_status == IngestStatus.SUCCESS
        yield dataset
    finally:
        _archive_all([dataset])


def test_create_derived_dataset_with_inputs(
    client: NominalClient, tagged_datasets: tuple[Dataset, Dataset], archive: ArchiveFn
) -> None:
    """Inputs given at creation come back from the server exactly as they were sent, tags included."""
    dataset_a, dataset_b = tagged_datasets
    inputs = (
        DerivedDatasetInput.create(dataset_a, filters=[TagFilter.in_("vehicle", "A")]),
        DerivedDatasetInput.create(dataset_b),
    )

    derived = client.create_derived_dataset(f"derived-{uuid4().hex[:8]}", inputs=inputs, labels=["e2e"])
    archive(derived)

    assert derived.labels == ("e2e",)
    assert derived.list_input_datasets() == inputs


def test_create_derived_dataset_with_no_inputs(client: NominalClient, archive: ArchiveFn) -> None:
    """A derived dataset can be created empty and populated afterwards."""
    derived = client.create_derived_dataset(f"derived-empty-{uuid4().hex[:8]}")
    archive(derived)

    assert derived.list_input_datasets() == ()


def test_a_derived_dataset_is_looked_up_as_a_derived_dataset(
    client: NominalClient, tagged_datasets: tuple[Dataset, Dataset], archive: ArchiveFn
) -> None:
    """A plain dataset lookup carries the derived definition, which is what types the result as derived."""
    dataset_a, _ = tagged_datasets
    derived = client.create_derived_dataset(
        f"derived-{uuid4().hex[:8]}", inputs=[DerivedDatasetInput.create(dataset_a)]
    )
    archive(derived)

    assert isinstance(client.get_dataset(derived.rid), DerivedDataset)
    assert not isinstance(client.get_dataset(dataset_a.rid), DerivedDataset)


def test_add_and_remove_input_datasets(
    client: NominalClient, tagged_datasets: tuple[Dataset, Dataset], archive: ArchiveFn
) -> None:
    """Inputs added and removed one at a time leave the definition in the expected state each step."""
    dataset_a, dataset_b = tagged_datasets
    derived = client.create_derived_dataset(f"derived-{uuid4().hex[:8]}")
    archive(derived)

    assert derived.add_input_dataset(
        DerivedDatasetInput.create(dataset_a, filters=[TagFilter.in_("vehicle", "A")])
    ) == (DerivedDatasetInput.create(dataset_a, filters=[TagFilter.in_("vehicle", "A")]),)
    assert derived.add_input_dataset(DerivedDatasetInput.create(dataset_b)) == (
        DerivedDatasetInput.create(dataset_a, filters=[TagFilter.in_("vehicle", "A")]),
        DerivedDatasetInput.create(dataset_b),
    )
    assert derived.remove_input_dataset(dataset_a) == (DerivedDatasetInput.create(dataset_b),)
    assert derived.list_input_datasets() == (DerivedDatasetInput.create(dataset_b),)


def test_get_and_commit_definition_round_trip_the_raw_spec(
    client: NominalClient, tagged_datasets: tuple[Dataset, Dataset], archive: ArchiveFn
) -> None:
    """The raw spec read back is the one written, and a commit against it replaces the definition."""
    dataset_a, dataset_b = tagged_datasets
    derived = client.create_derived_dataset(
        f"derived-{uuid4().hex[:8]}", inputs=[DerivedDatasetInput.create(dataset_a)]
    )
    archive(derived)

    definition = derived.get_definition()
    assert definition.spec == _build_spec([DerivedDatasetInput.create(dataset_a)])

    new_spec = _build_spec([DerivedDatasetInput.create(dataset_b)])
    committed = derived.commit_definition(new_spec, "swap input", latest_commit=definition.commit.id)
    assert committed.spec == new_spec
    assert committed.commit.parent_commit == definition.commit.id
    assert derived.list_input_datasets() == (DerivedDatasetInput.create(dataset_b),)
    assert derived.get_definition(definition.commit.id).spec == definition.spec


def test_remove_input_dataset_rejects_a_dataset_that_is_not_an_input(
    client: NominalClient, tagged_datasets: tuple[Dataset, Dataset], archive: ArchiveFn
) -> None:
    dataset_a, dataset_b = tagged_datasets
    derived = client.create_derived_dataset(
        f"derived-{uuid4().hex[:8]}", inputs=[DerivedDatasetInput.create(dataset_a)]
    )
    archive(derived)

    with pytest.raises(ValueError, match="is not an input of derived dataset"):
        derived.remove_input_dataset(dataset_b)


def test_derived_dataset_reads_the_union_of_its_inputs(
    client: NominalClient,
    tagged_datasets: tuple[Dataset, Dataset],
    csv_data: bytes,
    csv_data2: bytes,
    archive: ArchiveFn,
) -> None:
    """The union carries both inputs' readings. Asserted by value: the two CSVs share no temperature."""
    dataset_a, dataset_b = tagged_datasets

    both = client.create_derived_dataset(
        f"derived-union-{uuid4().hex[:8]}",
        inputs=[DerivedDatasetInput.create(dataset_a), DerivedDatasetInput.create(dataset_b)],
    )
    archive(both)

    assert _temperatures(both) == _expected_temperatures(csv_data, csv_data2)


def test_a_filter_selects_series_within_an_input(
    client: NominalClient, mixed_dataset: Dataset, csv_data: bytes, archive: ArchiveFn
) -> None:
    """Filtering one dataset that holds both tag values keeps only the matching series, not the whole input."""
    filtered = client.create_derived_dataset(
        f"derived-filtered-{uuid4().hex[:8]}",
        inputs=[DerivedDatasetInput.create(mixed_dataset, filters=[TagFilter.in_("vehicle", "A")])],
    )
    archive(filtered)

    assert _temperatures(filtered) == _expected_temperatures(csv_data)


def test_an_exclusion_filter_drops_the_matching_series(
    client: NominalClient, mixed_dataset: Dataset, csv_data2: bytes, archive: ArchiveFn
) -> None:
    """`not_in` is the complement of the same filter, and is only expressible through `filters`."""
    excluded = client.create_derived_dataset(
        f"derived-excluded-{uuid4().hex[:8]}",
        inputs=[DerivedDatasetInput.create(mixed_dataset, filters=[TagFilter.not_in("vehicle", "A")])],
    )
    archive(excluded)

    assert _temperatures(excluded) == _expected_temperatures(csv_data2)


def test_a_multi_value_filter_keeps_every_named_value(
    client: NominalClient, mixed_dataset: Dataset, csv_data: bytes, csv_data2: bytes, archive: ArchiveFn
) -> None:
    both_values = client.create_derived_dataset(
        f"derived-multi-{uuid4().hex[:8]}",
        inputs=[DerivedDatasetInput.create(mixed_dataset, filters=[TagFilter.in_("vehicle", "A", "B")])],
    )
    archive(both_values)

    assert _temperatures(both_values) == _expected_temperatures(csv_data, csv_data2)


def test_an_offset_shifts_the_input_in_time(
    client: NominalClient, tagged_datasets: tuple[Dataset, Dataset], archive: ArchiveFn
) -> None:
    """A time-shifted input reads back at moved timestamps, with its values untouched."""
    dataset_a, _ = tagged_datasets
    offset = timedelta(hours=1)

    unshifted = client.create_derived_dataset(
        f"derived-unshifted-{uuid4().hex[:8]}", inputs=[DerivedDatasetInput.create(dataset_a)]
    )
    archive(unshifted)
    shifted = client.create_derived_dataset(
        f"derived-shifted-{uuid4().hex[:8]}", inputs=[DerivedDatasetInput.create(dataset_a, offset=offset)]
    )
    archive(shifted)

    before = datasource_to_dataframe(unshifted, channel_exact_match=["temperature"])
    after = datasource_to_dataframe(shifted, channel_exact_match=["temperature"])
    assert _temperatures(shifted) == _temperatures(unshifted)
    assert after.index.min() - before.index.min() == offset


def test_an_added_tag_labels_the_inputs_series(
    client: NominalClient,
    tagged_datasets: tuple[Dataset, Dataset],
    csv_data: bytes,
    csv_data2: bytes,
    archive: ArchiveFn,
) -> None:
    """`add_tag` is what keeps two inputs' channels distinguishable after the union, server-side.

    The definition round-trip alone would pass even if the tag were never applied to any series, so the
    readings are exported back per tag: each value identifies the input it came from.
    """
    dataset_a, dataset_b = tagged_datasets
    inputs = (
        DerivedDatasetInput.create(dataset_a, add_tag=("source", "daq1")),
        DerivedDatasetInput.create(dataset_b, add_tag=("source", "daq2")),
    )

    labelled = client.create_derived_dataset(f"derived-labelled-{uuid4().hex[:8]}", inputs=inputs)
    archive(labelled)

    assert labelled.list_input_datasets() == inputs
    assert _temperatures(labelled, {"source": "daq1"}) == _expected_temperatures(csv_data)
    assert _temperatures(labelled, {"source": "daq2"}) == _expected_temperatures(csv_data2)
    assert _temperatures(labelled) == _expected_temperatures(csv_data, csv_data2)


def test_a_dataset_can_be_listed_twice(
    client: NominalClient, tagged_datasets: tuple[Dataset, Dataset], csv_data: bytes, archive: ArchiveFn
) -> None:
    """Listing one dataset twice is accepted and kept in the definition, which is what offsetting a source
    against itself relies on. The two appearances are not readable as separate series through a channel
    export, which aggregates a channel across tag sets — so the values come back once, not twice.
    """
    dataset_a, _ = tagged_datasets
    inputs = (
        DerivedDatasetInput.create(dataset_a, add_tag=("source", "one")),
        DerivedDatasetInput.create(dataset_a, add_tag=("source", "two")),
    )

    doubled = client.create_derived_dataset(f"derived-doubled-{uuid4().hex[:8]}", inputs=inputs)
    archive(doubled)

    assert doubled.list_input_datasets() == inputs
    assert _temperatures(doubled) == _expected_temperatures(csv_data)
