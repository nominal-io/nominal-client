from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest
from google.protobuf import timestamp_pb2

from nominal.core.client import NominalClient
from nominal.core.exceptions import NominalNotFoundError
from nominal.core.secret import Secret
from nominal.protos.secrets.v1 import secrets_pb2


def _proto_secret(rid: str, **kwargs) -> secrets_pb2.Secret:
    return secrets_pb2.Secret(rid=rid, created_at=timestamp_pb2.Timestamp(seconds=1), **kwargs)


def _secret(clients: MagicMock, rid: str = "ri.secret.test") -> Secret:
    return Secret._from_proto(clients, _proto_secret(rid, name="original", labels=["keep"]))


def test_update_leaves_omitted_fields_absent_on_the_wire() -> None:
    """Fields not passed to update() are absent from the request, so the backend leaves them unchanged."""
    clients = MagicMock()
    secret = _secret(clients)
    clients.secrets.Update.return_value = secrets_pb2.UpdateResponse(
        secret=_proto_secret(secret.rid, name="renamed", labels=["keep"])
    )

    secret.update(name="renamed")

    request = clients.secrets.Update.call_args.args[0]
    assert request.rid == secret.rid
    assert request.request.name == "renamed"
    assert not request.request.HasField("description")
    assert not request.request.HasField("labels")
    assert not request.request.HasField("properties")
    assert secret.name == "renamed"


def test_update_sends_empty_collections_as_explicit_clears() -> None:
    """Passing empty labels/properties sends present-but-empty wrappers (clear), distinct from omission."""
    clients = MagicMock()
    secret = _secret(clients)
    clients.secrets.Update.return_value = secrets_pb2.UpdateResponse(secret=_proto_secret(secret.rid))

    secret.update(labels=[], properties={})

    request = clients.secrets.Update.call_args.args[0]
    assert request.request.HasField("labels")
    assert list(request.request.labels.labels) == []
    assert request.request.HasField("properties")
    assert dict(request.request.properties.properties) == {}


def test_get_secret_translates_not_found(fake_rpc_error) -> None:
    """A NOT_FOUND status from the secrets service surfaces as NominalNotFoundError, not grpc.RpcError."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    clients.secrets.Get.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)

    with pytest.raises(NominalNotFoundError):
        client.get_secret("ri.secret.missing")


def test_secret_refresh_updates_fields_in_place() -> None:
    """refresh() re-fetches via Get and updates the same instance."""
    clients = MagicMock()
    secret = _secret(clients)
    clients.secrets.Get.return_value = secrets_pb2.GetResponse(
        secret=_proto_secret(secret.rid, name="renamed", labels=["new-label"])
    )

    returned = secret.refresh()

    assert returned is secret
    assert secret.name == "renamed"
    assert secret.labels == ["new-label"]
    assert clients.secrets.Get.call_args.args[0].rid == secret.rid


def test_search_secrets_follows_pagination_cursors_and_ands_filters() -> None:
    """search_secrets accumulates results across pages and ANDs the provided filters into the query."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    page1 = secrets_pb2.SearchSecretsResponse(results=[_proto_secret("ri.secret.a")], next_page_token="tok")
    page2 = secrets_pb2.SearchSecretsResponse(results=[_proto_secret("ri.secret.b")], next_page_token="")
    clients.secrets.Search.side_effect = [page1, page2]

    results = client.search_secrets(search_text="text", labels=["label"])

    assert [s.rid for s in results] == ["ri.secret.a", "ri.secret.b"]
    assert clients.secrets.Search.call_count == 2
    first_request = clients.secrets.Search.call_args_list[0].args[0]
    expected_query = secrets_pb2.SearchSecretsQuery(
        **{
            "and": [
                secrets_pb2.SearchSecretsQuery(search_text="text"),
                secrets_pb2.SearchSecretsQuery(label="label"),
            ]
        }
    )
    assert first_request.query == expected_query
    second_request = clients.secrets.Search.call_args_list[1].args[0]
    assert second_request.token == "tok"
