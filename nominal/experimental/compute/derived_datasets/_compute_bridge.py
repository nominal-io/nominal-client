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

    ``nominal_compute`` is an optional dependency that is only needed to *construct* the ``dataset`` argument,
    so this module never imports it at runtime (the annotation is resolved under ``TYPE_CHECKING`` only). Any
    caller with a ``dataset`` to pass necessarily has it installed. ``nominal_compute`` serializes graphs to
    the conjure wire format via ``to_json()`` (its only export path), so we round-trip that JSON into conjure.
    """
    # to_json() exists on every nominal_compute node at runtime but is absent from its type stub, so the call
    # is made through Any.
    conjure_json = cast(Any, dataset).to_json()
    return cast(scout_compute_api.Dataset, _DECODER.read_from_string(conjure_json, scout_compute_api.Dataset))
