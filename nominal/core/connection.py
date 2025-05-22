from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, Sequence

from nominal_api import scout_datasource_connection_api

from nominal.core.datasource import DataSource, _get_write_stream
from nominal.core.write_stream_base import WriteStreamBase


@dataclass(frozen=True)
class Connection(DataSource):
    name: str
    description: str | None

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
                _clients=clients,
                nominal_data_source_rid=response.connection_details.nominal.nominal_data_source_rid,
            )
        return cls(
            rid=response.rid,
            name=response.display_name,
            description=response.description,
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
    """A `StreamingConnection` is used to stream telemetry data to Nominal.

    This method of streaming is being phased out in favor of streaming to a dataset.
    However, it is still available while we complete adding the same level of tag
    support to datasets, and for backwards compatibility.
    """

    nominal_data_source_rid: str

    def get_write_stream(
        self,
        batch_size: int = 50_000,
        max_wait: timedelta = timedelta(seconds=1),
        data_format: Literal["json", "protobuf", "experimental"] = "json",
    ) -> WriteStreamBase:
        """Stream to write non-blocking messages to a datasource.

        Args:
        ----
            batch_size: How big the batch can get before writing to Nominal.
            max_wait: How long a batch can exist before being flushed to Nominal.
            data_format: Serialized data format to use during upload.
                NOTE: selecting 'protobuf' requires that `nominal` was installed with `protos` extras.

        Returns:
        --------
            Write stream object configured to send data to nominal. This may be used as a context manager
            (so that resources are automatically released upon exiting the context), or if not used as a context
            manager, should be explicitly `close()`-ed once no longer needed.
        """
        return _get_write_stream(
            batch_size=batch_size,
            max_wait=max_wait,
            data_format=data_format,
            write_rid=self.nominal_data_source_rid,
            clients=self._clients,
        )


def _get_connections(
    clients: Connection._Clients, connection_rids: Sequence[str]
) -> Sequence[scout_datasource_connection_api.Connection]:
    return [clients.connection.get_connection(clients.auth_header, rid) for rid in connection_rids]
