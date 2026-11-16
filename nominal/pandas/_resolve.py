"""Find or create the Nominal client, asset and dataset that ``df.nom.to_nominal`` uploads to."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from nominal.config import get_client
from nominal.core import NominalClient

if TYPE_CHECKING:
    import nominal.core

DEFAULT = "nominal-pandas"
"""Name of the asset and dataset used when none is given."""


def client_of(obj: Any) -> nominal.core.NominalClient:
    """Return a client on the connection a nominal-client object (asset, dataset, ...) was fetched with.

    nominal-client objects keep that connection privately (``_clients``); nominal-client
    has no public way to get a client back from an object.
    """
    return NominalClient(_clients=obj._clients)


def resolve(
    asset: nominal.core.Asset | str | None,
    dataset: nominal.core.Dataset | str | None,
    client: nominal.core.NominalClient | None,
    profile: str | None = None,
) -> tuple[nominal.core.Asset, nominal.core.Dataset]:
    """Return the asset and dataset to upload to, finding or creating them as needed.

    ``asset``: an `Asset`, or a name (the asset with exactly that name, created if there is
    none), or ``None`` for `DEFAULT`. ``dataset``: a `Dataset`, attached to the asset if it
    isn't already (under its name, or its RID if the asset uses that name), or a name (the
    asset's dataset by that name, created if there is none), or ``None`` for `DEFAULT`.
    ``profile`` names the client's profile, in place of ``client``. An `Asset` object already has its own
    connection, so ``client`` and ``profile`` can't be combined with one.
    """
    if client is not None and profile is not None:
        raise ValueError("pass profile= or client=, not both")
    if (client is not None or profile is not None) and asset is not None and not isinstance(asset, str):
        raise ValueError(
            "an Asset already has its own connection, so client= and profile= can't be used with it; "
            "pass the asset's name instead, or drop client= and profile="
        )
    if profile is not None:
        client = get_client(profile)
    if client is None:
        source = next((o for o in (asset, dataset) if o is not None and not isinstance(o, str)), None)
        if source is not None:
            client = client_of(source)
        else:
            client = get_client()
    if asset is None or isinstance(asset, str):
        asset = _asset_named(client, asset or DEFAULT)
    if dataset is None or isinstance(dataset, str):
        name = dataset or DEFAULT
        dataset = asset.get_or_create_dataset(name, name=name)
    else:
        scopes = dict(asset.list_datasets())
        if all(found.rid != dataset.rid for found in scopes.values()):
            # under its name, unless another of the asset's datasets has it; Nominal rejects duplicates
            asset.add_dataset(dataset.name if dataset.name not in scopes else dataset.rid, dataset)
    return asset, dataset


def _asset_named(client: nominal.core.NominalClient, name: str) -> nominal.core.Asset:
    """Return the asset with exactly this name, creating it if there is none."""
    matches = [a for a in client.search_assets(exact_substring=name) if a.name == name]
    if len(matches) > 1:
        rids = ", ".join(a.rid for a in matches)
        raise ValueError(f"{len(matches)} assets are named {name!r} ({rids}); pass the Asset itself")
    return matches[0] if matches else client.create_asset(name)
