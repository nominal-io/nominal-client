from __future__ import annotations

import itertools
import warnings
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, Mapping, Sequence

from nominal_api import (
    scout_datasource_connection_api,
)

from nominal.core._batch_processor import process_batch_legacy
from nominal.core.datasource import DataSource
from nominal.core.stream import WriteStream


@dataclass(frozen=True)
class Connection(DataSource):
    name: str
    description: str | None
    _tags: Mapping[str, Sequence[str]]

    @classmethod
    def _from_conjure(
        cls, clients: DataSource._Clients, response: scout_datasource_connection_api.Connection
    ) -> Connection | StreamingConnection:
        """Factory method to create the appropriate Connection subclass based on connection details"""
        if response.connection_details.nominal is not None:
            return StreamingConnection(
                rid=response.rid,
                name=response.display_name,
                description=response.description,
                _tags=response.available_tag_values,
                _clients=clients,
                nominal_data_source_rid=response.connection_details.nominal.nominal_data_source_rid,
            )
        return cls(
            rid=response.rid,
            name=response.display_name,
            description=response.description,
            _tags=response.available_tag_values,
            _clients=clients,
        )

    def archive(self) -> None:
        """Archive this connection.
        Archived connections are not deleted, but are hidden from the UI.
        """
        self._clients.connection.archive_connection(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive this connection, making it visible in the UI."""
        self._clients.connection.unarchive_connection(self._clients.auth_header, self.rid)


@dataclass(frozen=True)
class StreamingConnection(Connection):
    nominal_data_source_rid: str

    # Deprecated methods for backward compatibility
    def get_nominal_write_stream(self, batch_size: int = 50_000, max_wait_sec: int = 1) -> WriteStream:
        warnings.warn(
            "get_nominal_write_stream is deprecated and will be removed in a future version. "
            "use get_write_stream instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.get_write_stream(batch_size, timedelta(seconds=max_wait_sec))

    def get_write_stream(
        self,
        batch_size: int = 50_000,
        max_wait: timedelta = timedelta(seconds=1),
        data_format: Literal["json", "protobuf"] = "json",
    ) -> WriteStream:
        """Stream to write non-blocking messages to a datasource.

        Args:
        ----
            batch_size (int): How big the batch can get before writing to Nominal. Default 10
            max_wait (timedelta): How long a batch can exist before being flushed to Nominal. Default 5 seconds
            data_format (Literal["json", "protobuf"]): Send data as protobufs or as json. Default json

        Examples:
        --------
            Standard Usage:
            ```py
            with connection.get_write_stream() as stream:
                stream.enqueue("my_channel_name", "2021-01-01T00:00:00Z", 42.0)
                stream.enqueue("my_channel_name2", "2021-01-01T00:00:01Z", 43.0, {"tag1": "value1"})
                ...
            ```

            Without a context manager:
            ```py
            stream = connection.get_write_stream()
            stream.enqueue("my_channel_name", "2021-01-01T00:00:00Z", 42.0)
            stream.enqueue("my_channel_name2", "2021-01-01T00:00:01Z", 43.0, {"tag1": "value1"})
            ...
            stream.close()
            ```

        """
        if data_format == "json":
            return WriteStream.create(
                batch_size,
                max_wait,
                lambda batch: process_batch_legacy(
                    batch, self.nominal_data_source_rid, self._clients.auth_header, self._clients.storage_writer
                ),
            )

        try:
            from nominal.core._batch_processor_proto import process_batch
        except ImportError:
            raise ImportError("nominal-api-protos is required to use get_write_stream with use_protos=True")

        return WriteStream.create(
            batch_size,
            max_wait,
            lambda batch: process_batch(
                batch=batch,
                nominal_data_source_rid=self.nominal_data_source_rid,
                auth_header=self._clients.auth_header,
                proto_write=self._clients.proto_write,
            ),
        )


def _get_connections(
    clients: Connection._Clients, connection_rids: Sequence[str]
) -> Sequence[scout_datasource_connection_api.Connection]:
    return [clients.connection.get_connection(clients.auth_header, rid) for rid in connection_rids]


def _tag_product(tags: Mapping[str, Sequence[str]]) -> list[dict[str, str]]:
    # {color: [red, green], size: [S, M, L]} -> [{color: red, size: S}, {color: red, size: M}, ...,
    #                                            {color: green, size: L}]
    return [dict(zip(tags.keys(), values)) for values in itertools.product(*tags.values())]
