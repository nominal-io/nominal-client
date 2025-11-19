from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol, Sequence

from nominal_api import secrets_api
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableMixin
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True)
class Secret(HasRid, RefreshableMixin[secrets_api.Secret]):
    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    created_at: IntegralNanosecondsUTC
    _clients: _Clients = field(repr=False)

    def _get_latest_api(self) -> secrets_api.Secret:
        return self._clients.secrets.get(self._clients.auth_header, self.rid)

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
            description: New name of the secret
            properties: New properties for the secret
            labels: New labels for the secret

        Returns:
            Updated secret metadata.
        """
        request = secrets_api.UpdateSecretRequest(
            name=name,
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(**properties),
        )
        updated_secret = self._clients.secrets.update(self._clients.auth_header, request, self.rid)
        return self._refresh_from_api(updated_secret)

    def archive(self) -> None:
        """Archive the secret, disallowing it to appear from users."""
        self._clients.secrets.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive the secret, allowing it to appear to users."""
        self._clients.secrets.unarchive(self._clients.auth_header, self.rid)

    def delete(self) -> None:
        """Permanently delete the secret, removing it from the database entirely."""
        self._clients.secrets.delete(self._clients.auth_header, self.rid)

    class _Clients(HasScoutParams, Protocol):
        @property
        def secrets(self) -> secrets_api.SecretService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, raw_secret: secrets_api.Secret) -> Self:
        return cls(
            rid=raw_secret.rid,
            name=raw_secret.name,
            description=raw_secret.description,
            properties=raw_secret.properties,
            labels=raw_secret.labels,
            created_at=_SecondsNanos.from_flexible(raw_secret.created_at).to_nanoseconds(),
            _clients=clients,
        )
