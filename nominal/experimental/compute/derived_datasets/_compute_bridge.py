"""Convert ``nominal_compute`` graphs into the ``scout_compute_api`` conjure types the catalog API expects."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

from conjure_python_client._serde.decoder import ConjureDecoder
from nominal_api import scout_compute_api

if TYPE_CHECKING:
    import nominal_compute

_DECODER = ConjureDecoder()


def to_conjure_dataset(dataset: nominal_compute.Dataset) -> scout_compute_api.Dataset:
    """Decode a ``nominal_compute.Dataset`` into the ``scout_compute_api.Dataset`` the catalog API expects."""
    conjure_json = cast(Any, dataset).to_json()
    return cast(scout_compute_api.Dataset, _DECODER.do_decode(json.loads(conjure_json), scout_compute_api.Dataset))
