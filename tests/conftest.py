from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest
from nominal_api import api, scout_catalog

from nominal.core.channel import Channel, ChannelDataType


class _FakeRpcError(grpc.RpcError):
    """A grpc.RpcError with a controllable status code and details, for exercising gRPC error translation."""

    def __init__(self, code: grpc.StatusCode, details: str = "fake rpc error") -> None:
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


@pytest.fixture
def mock_clients():
    """A mock _ClientsBunch with a preset auth header."""
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    return clients


@pytest.fixture
def make_channel(mock_clients):
    """Factory fixture that creates Channel instances sharing the same mock clients."""

    def _make(
        name: str,
        data_type: ChannelDataType | None = ChannelDataType.DOUBLE,
        data_source: str = "ds-1",
    ) -> Channel:
        return Channel(
            name=name,
            data_source=data_source,
            data_type=data_type,
            unit=None,
            description=None,
            _clients=mock_clients,
        )

    return _make


@pytest.fixture
def make_enriched_dataset():
    """Factory fixture for an EnrichedDataset, the Catalog row every dataset lookup is built from."""

    def _make(
        rid: str = "ri.catalog.ws.dataset.abc",
        *,
        name: str = "A dataset",
        derived_definition: scout_catalog.DerivedDefinition | None = None,
    ) -> scout_catalog.EnrichedDataset:
        return scout_catalog.EnrichedDataset(
            rid=rid,
            name=name,
            display_name=name,
            uuid="00000000-0000-0000-0000-000000000000",
            properties={},
            typed_properties={},
            labels=[],
            is_archived=False,
            allow_streaming=False,
            channel_search_split_tag_keys=[],
            granularity=api.Granularity.NANOSECONDS,
            ingest_date="2026-01-01T00:00:00Z",
            last_ingest_status=api.IngestStatusV2(success=api.SuccessResult()),
            origin_metadata=scout_catalog.DatasetOriginMetadata(),
            retention_policy=scout_catalog.RetentionPolicy(type=scout_catalog.RetentionPolicyType.KEEP_FOREVER),
            timestamp_type=scout_catalog.WeakTimestampType.ABSOLUTE,
            derived_definition=derived_definition,
        )

    return _make


@pytest.fixture
def make_series_count_response():
    """Factory fixture that builds a mock BatchGetSeriesCountResponse from a list of counts.

    Pass `None` in a slot to simulate a channel on an external datasource (series_count absent).
    """

    def _make(counts: list[int | None]):
        response = MagicMock()
        response.responses = [MagicMock(series_count=count) for count in counts]
        return response

    return _make


@pytest.fixture
def fake_rpc_error():
    """Factory fixture for grpc.RpcError instances with a chosen status code (and optional details)."""
    return _FakeRpcError
