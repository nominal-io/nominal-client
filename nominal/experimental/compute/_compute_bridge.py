"""Convert ``nominal_compute`` graphs into the ``scout_compute_api`` conjure types the platform expects."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, TypeVar, cast

from conjure_python_client._serde.decoder import ConjureDecoder
from nominal_api import scout_compute_api

if TYPE_CHECKING:
    import nominal_compute

_DECODER = ConjureDecoder()
_T = TypeVar("_T")


def _decode(node: object, conjure_type: type[_T]) -> _T:
    """Bridge a ``nominal_compute`` node into ``conjure_type`` via its ``to_json()`` wire format."""
    return cast(_T, _DECODER.do_decode(json.loads(cast(Any, node).to_json()), conjure_type))


def to_conjure_dataset(dataset: nominal_compute.Dataset) -> scout_compute_api.Dataset:
    """Decode a ``nominal_compute.Dataset`` into the ``scout_compute_api.Dataset`` the catalog API expects."""
    return _decode(dataset, scout_compute_api.Dataset)
