"""Convert ``nominal_compute`` graphs into the ``scout_compute_api`` conjure types the catalog API expects."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from conjure_python_client._serde.decoder import ConjureDecoder
from nominal_api import scout_compute_api

if TYPE_CHECKING:
    import nominal_compute

_DECODER = ConjureDecoder()


def to_conjure_dataset(dataset: nominal_compute.Dataset) -> scout_compute_api.Dataset:
    """Decode a ``nominal_compute.Dataset`` into the ``scout_compute_api.Dataset`` the catalog API expects.

    ``nominal_compute`` is an optional dependency: it is not declared in the SDK's requirements, so importing
    this module never pulls it in. Building a derived dataset does require it, so we import it lazily here and
    raise a clear error if it is missing. ``nominal_compute`` serializes graphs to the conjure wire format via
    ``to_json()`` (its only export path), so we round-trip that JSON into the conjure type.
    """
    try:
        import nominal_compute  # noqa: F401  # imported only to verify the optional dependency is installed
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "Building derived datasets requires the optional `nominal-compute` package, which is not installed. "
            "Install it with `pip install nominal-compute`."
        ) from exc
    # to_json() is present on every nominal_compute node at runtime but is absent from its type stub, so the
    # call is made through Any; ``dataset`` is typed for callers who have nominal_compute installed.
    conjure_json = cast(Any, dataset).to_json()
    return cast(scout_compute_api.Dataset, _DECODER.read_from_string(conjure_json, scout_compute_api.Dataset))
