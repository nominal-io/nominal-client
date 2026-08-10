from __future__ import annotations

from nominal_api import api, storage_series_api

from nominal.core.channel import ChannelDataType

# ChannelDataType members whose NominalDataType counterpart is spelled differently.
# The conjure side suffixes integer widths; the SDK mirrors api.SeriesDataType, which does not.
_CONJURE_RENAMES: dict[ChannelDataType, storage_series_api.NominalDataType] = {
    ChannelDataType.INT: storage_series_api.NominalDataType.INT64,
    ChannelDataType.UINT: storage_series_api.NominalDataType.UINT64,
}

# api.SeriesDataType variants the SDK deliberately does not model. These resolve to UNKNOWN.
_UNMODELED_API_TYPES = frozenset({api.SeriesDataType.SPATIAL})


def _expected_conjure_type(data_type: ChannelDataType) -> storage_series_api.NominalDataType:
    """The NominalDataType a member should convert to: the same name, unless explicitly renamed."""
    if data_type in _CONJURE_RENAMES:
        return _CONJURE_RENAMES[data_type]
    return storage_series_api.NominalDataType[data_type.name]


def test_to_conjure_covers_every_member() -> None:
    """Every ChannelDataType member converts to its own NominalDataType, not the fallback.

    This guards the `case _` arm: a member added without its own arm silently converts to
    NominalDataType.UNKNOWN, which the backend records as an untyped channel.
    """
    for data_type in ChannelDataType:
        assert data_type._to_conjure() == _expected_conjure_type(data_type), (
            f"{data_type.name} falls through to the `case _` arm of _to_conjure"
        )


def test_to_conjure_uses_width_suffixed_integers() -> None:
    """The conjure integer variants are width-suffixed even though the SDK members are not."""
    assert ChannelDataType.INT._to_conjure() == storage_series_api.NominalDataType.INT64
    assert ChannelDataType.UINT._to_conjure() == storage_series_api.NominalDataType.UINT64


def test_from_conjure_resolves_every_api_series_data_type() -> None:
    """Every api.SeriesDataType variant resolves to the same-named member, or UNKNOWN if unmodeled.

    Driven off the api enum rather than a hardcoded list, so a variant added to a future
    nominal-api release fails here instead of silently arriving as UNKNOWN.
    """
    for api_type in api.SeriesDataType:
        resolved = ChannelDataType._from_conjure(api_type)
        if api_type in _UNMODELED_API_TYPES:
            assert resolved is ChannelDataType.UNKNOWN, f"{api_type.name} is not meant to be modeled yet"
        else:
            assert resolved.name == api_type.name, f"{api_type.name} has no matching ChannelDataType member"


def test_spatial_is_deliberately_not_modeled() -> None:
    """SPATIAL is excluded by design and must keep resolving to UNKNOWN."""
    assert "SPATIAL" not in ChannelDataType.__members__
    assert ChannelDataType._from_conjure(api.SeriesDataType.SPATIAL) is ChannelDataType.UNKNOWN
