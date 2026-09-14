from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from nominal_api import scout_datasource_connection_api

from nominal.core.datasource import DataSource


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

    @property
    def _write_rid(self) -> str:
        """Streaming connections write to their backing datasource, not to the connection itself."""
        return self.nominal_data_source_rid


def _get_connections(
    clients: Connection._Clients, connection_rids: Sequence[str]
) -> Sequence[scout_datasource_connection_api.Connection]:
    return [clients.connection.get_connection(clients.auth_header, rid) for rid in connection_rids]


def _get_connection(clients: Connection._Clients, connection_rid: str) -> scout_datasource_connection_api.Connection:
    return clients.connection.get_connection(clients.auth_header, connection_rid)
