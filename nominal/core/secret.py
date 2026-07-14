from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol, Sequence

from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.protos.secrets.v1 import secrets_pb2, secrets_pb2_grpc
from nominal.protos.types import types_pb2
from nominal.ts import IntegralNanosecondsUTC


@dataclass(frozen=True)
class Secret(HasRid, RefreshableGrpcMixin[secrets_pb2.Secret]):
    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    created_at: IntegralNanosecondsUTC
    _clients: _Clients = field(repr=False)

    def _get_latest_api(self) -> secrets_pb2.Secret:
        with translate_grpc_errors():
            return self._clients.secrets.Get(secrets_pb2.GetRequest(rid=self.rid)).secret

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Update the secret in-place.

        Args:
            name: New name of the secret
            description: New description of the secret
            properties: New properties for the secret
            labels: New labels for the secret

        Returns:
            Updated secret metadata.

        Note:
            Fields left as None are unchanged.
        """
        request = secrets_pb2.UpdateRequest(
            rid=self.rid,
            request=secrets_pb2.UpdateSecretRequest(
                name=name,
                description=description,
                labels=None if labels is None else types_pb2.LabelUpdateWrapper(labels=list(labels)),
                properties=None if properties is None else types_pb2.PropertyUpdateWrapper(properties=dict(properties)),
            ),
        )
        with translate_grpc_errors():
            response = self._clients.secrets.Update(request)
        return self._refresh_from_api(response.secret)

    def archive(self) -> None:
        """Archive the secret, disallowing it to appear from users."""
        with translate_grpc_errors():
            self._clients.secrets.Archive(secrets_pb2.ArchiveRequest(rid=self.rid))

    def unarchive(self) -> None:
        """Unarchive the secret, allowing it to appear to users."""
        with translate_grpc_errors():
            self._clients.secrets.Unarchive(secrets_pb2.UnarchiveRequest(rid=self.rid))

    def delete(self) -> None:
        """Permanently delete the secret, removing it from the database entirely."""
        with translate_grpc_errors():
            self._clients.secrets.Delete(secrets_pb2.DeleteRequest(rid=self.rid))

    class _Clients(HasScoutParams, Protocol):
        @property
        def secrets(self) -> secrets_pb2_grpc.SecretServiceStub: ...

    @classmethod
    def _from_proto(cls, clients: _Clients, raw_secret: secrets_pb2.Secret) -> Self:
        return cls(
            rid=raw_secret.rid,
            name=raw_secret.name,
            description=raw_secret.description,
            properties=dict(raw_secret.properties),
            labels=list(raw_secret.labels),
            created_at=raw_secret.created_at.ToNanoseconds(),
            _clients=clients,
        )
