from __future__ import annotations

from typing import Mapping, cast

from conjure_python_client import ConjureDecoder, ConjureEncoder
from nominal_api import scout_spatial, scout_spatial_api


class SpatialService(scout_spatial.SpatialService):
    def create_in_channel(
        self,
        auth_header: str,
        request: scout_spatial_api.CreateSpatialRequest,
        dataset_rid: str,
        channel: str,
        tags: Mapping[str, str],
    ) -> scout_spatial_api.Spatial:
        payload = ConjureEncoder().default(request)
        payload["channel"] = {"datasetRid": dataset_rid, "channel": channel, "tags": dict(tags)}
        response = self._request(
            "POST",
            self._uri + "/spatial/v1/spatials",
            params={},
            headers={"Accept": "application/json", "Content-Type": "application/json", "Authorization": auth_header},
            json=payload,
        )
        body = response.json()
        if body.get("channel") != payload["channel"]:
            raise RuntimeError("The server did not register the spatial channel; spatial channel support is required")
        return cast(
            scout_spatial_api.Spatial,
            ConjureDecoder().decode(body, scout_spatial_api.Spatial, self._return_none_for_unknown_union_types),
        )
