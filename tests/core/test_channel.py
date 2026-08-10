from __future__ import annotations

from nominal_api import storage_series_api

from nominal.core.channel import ChannelDataType

# ChannelDataType members whose NominalDataType counterpart is spelled differently.
# The conjure side suffixes integer widths; the SDK mirrors api.SeriesDataType, which does not.
_CONJURE_RENAMES: dict[ChannelDataType, storage_series_api.NominalDataType] = {
    ChannelDataType.INT: storage_series_api.NominalDataType.INT64,
}


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
