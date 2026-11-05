from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from nominal_api import scout_catalog

from nominal.core.dataset import Dataset
from nominal.core.retention import KeepForever, KeepUntilExpiry, RetentionExemption
from nominal.core.run import Run
from nominal.protos.run.v1 import run_service_pb2
from nominal.protos.storage.v1 import dataset_retention_pb2
from nominal.protos.types.time import time_pb2

_TYPE = scout_catalog.RetentionPolicyType
# Nanosecond-precision timestamps: float seconds or datetime conversion would lose the low digits.
_START_NANOS = 1_700_000_000_123_456_789
_END_NANOS = 1_700_000_100_987_654_321


def _dataset(make_enriched_dataset, clients: MagicMock, **kwargs) -> Dataset:
    return Dataset._from_conjure(clients, make_enriched_dataset(**kwargs))


def _proto_exemption(source_id: str = "run-1", **kwargs) -> dataset_retention_pb2.DatasetRetentionExemption:
    return dataset_retention_pb2.DatasetRetentionExemption(
        dataset_rid="ri.catalog.ws.dataset.abc",
        source_id=source_id,
        bounds=time_pb2.Range(
            start=time_pb2.Timestamp(seconds=1_700_000_000, nanos=123_456_789),
            end=time_pb2.Timestamp(seconds=1_700_000_100, nanos=987_654_321),
        ),
        **kwargs,
    )


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (scout_catalog.RetentionPolicy(type=_TYPE.KEEP_FOREVER), KeepForever()),
        (scout_catalog.RetentionPolicy(type=_TYPE.KEEP_UNTIL_EXPIRY, expiry_days=30), KeepUntilExpiry(30)),
        # A policy type from a newer server must not make every dataset read fail.
        (scout_catalog.RetentionPolicy(type=_TYPE.UNKNOWN), None),
        (scout_catalog.RetentionPolicy(type=_TYPE.KEEP_UNTIL_EXPIRY), None),
    ],
)
def test_dataset_reads_retention_policy(make_enriched_dataset, policy, expected) -> None:
    """The catalog policy converts to an SDK policy, or None when this client can't interpret it."""
    dataset = _dataset(make_enriched_dataset, MagicMock(), retention_policy=policy)

    assert dataset.retention_policy == expected


@pytest.mark.parametrize(
    ("policy", "expected_case"),
    [(KeepForever(), "keep_forever"), (KeepUntilExpiry(expiry_days=7), "keep_until_expiry")],
)
def test_set_retention_policy_sends_the_chosen_policy(make_enriched_dataset, policy, expected_case) -> None:
    """Each policy maps to its own branch of the request's oneof, carrying the expiry."""
    clients = MagicMock()
    dataset = _dataset(make_enriched_dataset, clients)

    dataset.set_retention_policy(policy)

    request = clients.dataset_retention.UpdateRetentionPolicy.call_args.args[0]
    assert request.dataset_rid == dataset.rid
    assert request.retention_policy.WhichOneof("policy") == expected_case
    if isinstance(policy, KeepUntilExpiry):
        assert request.retention_policy.keep_until_expiry.expiry_days == 7


def test_set_retention_policy_none_clears_rather_than_sets(make_enriched_dataset) -> None:
    """None leaves the policy absent on the wire, which deletes it, distinct from an explicit KeepForever."""
    clients = MagicMock()
    dataset = _dataset(make_enriched_dataset, clients)

    dataset.set_retention_policy(None)

    request = clients.dataset_retention.UpdateRetentionPolicy.call_args.args[0]
    assert not request.HasField("retention_policy")


def test_list_retention_exemptions_follows_pages_and_keeps_nanoseconds(make_enriched_dataset) -> None:
    """Every page is fetched with the same bounds, and timestamps keep nanosecond precision."""
    clients = MagicMock()
    clients.dataset_retention.ListDatasetRetentionExemptions.side_effect = [
        dataset_retention_pb2.ListDatasetRetentionExemptionsResponse(
            exemptions=[_proto_exemption("run-1", description="first")], next_page_token="page-2"
        ),
        dataset_retention_pb2.ListDatasetRetentionExemptionsResponse(exemptions=[_proto_exemption("run-2")]),
    ]
    dataset = _dataset(make_enriched_dataset, clients)

    exemptions = dataset.list_retention_exemptions(_START_NANOS, _END_NANOS)

    assert exemptions == [
        RetentionExemption(dataset.rid, "run-1", _START_NANOS, _END_NANOS, "first"),
        RetentionExemption(dataset.rid, "run-2", _START_NANOS, _END_NANOS, None),
    ]
    first, second = (call.args[0] for call in clients.dataset_retention.ListDatasetRetentionExemptions.call_args_list)
    assert not first.HasField("next_page_token")
    assert second.next_page_token == "page-2"
    assert second.bounds.start.nanos == 123_456_789
    assert second.bounds.end.seconds == 1_700_000_100


def test_upsert_retention_exemption_omits_unset_description(make_enriched_dataset) -> None:
    """An omitted description is absent on the wire rather than sent as an empty string."""
    clients = MagicMock()
    clients.dataset_retention.UpsertDatasetRetentionExemption.return_value = (
        dataset_retention_pb2.UpsertDatasetRetentionExemptionResponse(exemption=_proto_exemption("manual"))
    )
    dataset = _dataset(make_enriched_dataset, clients)

    exemption = dataset.upsert_retention_exemption("manual", _START_NANOS, _END_NANOS)

    request = clients.dataset_retention.UpsertDatasetRetentionExemption.call_args.args[0]
    assert (request.dataset_rid, request.source_id) == (dataset.rid, "manual")
    assert not request.HasField("description")
    assert (request.bounds.start.nanos, request.bounds.end.nanos) == (123_456_789, 987_654_321)
    assert exemption == RetentionExemption(dataset.rid, "manual", _START_NANOS, _END_NANOS, None)


@pytest.mark.parametrize(
    ("lock_status", "expected"),
    [
        (run_service_pb2.LOCK_STATUS_LOCKED, True),
        (run_service_pb2.LOCK_STATUS_UNLOCKED, False),
        (run_service_pb2.LOCK_STATUS_UNSPECIFIED, False),
    ],
)
def test_run_reads_lock_status(lock_status, expected) -> None:
    """Only an explicit LOCKED status marks a run locked; an unspecified status reads as unlocked."""
    run = Run._from_proto(MagicMock(), run_service_pb2.Run(rid="ri.scout.run.1", lock_status=lock_status))

    assert run.is_locked is expected


@pytest.mark.parametrize(
    ("method", "expected"),
    [("lock", run_service_pb2.LOCK_STATUS_LOCKED), ("unlock", run_service_pb2.LOCK_STATUS_UNLOCKED)],
)
def test_run_lock_and_unlock_send_the_matching_status(method, expected) -> None:
    """lock() and unlock() request the matching lock status for this run."""
    clients = MagicMock()
    run = Run._from_proto(clients, run_service_pb2.Run(rid="ri.scout.run.1"))

    getattr(run, method)()

    request = clients.run.UpdateLockStatus.call_args.args[0]
    assert (request.rid, request.lock_status) == (run.rid, expected)
