from __future__ import annotations

import pytest

from nominal.core.elements import Color, Symbol
from nominal.protos.scout.elements.v1 import elements_pb2


@pytest.mark.parametrize(
    ("symbol", "oneof_name", "value"),
    [
        (Symbol.icon("castle"), "icon", "castle"),
        (Symbol.emoji(":castle:"), "emoji", ":castle:"),
        (Symbol.image("https://example.com/x.png"), "image", "https://example.com/x.png"),
    ],
)
def test_symbol_sets_the_matching_proto_oneof_arm(symbol: Symbol, oneof_name: str, value: str) -> None:
    """Each constructor targets its own oneof arm, so the kind survives a round trip."""
    proto = symbol._to_proto()

    assert proto.WhichOneof("symbol") == oneof_name
    assert getattr(proto, oneof_name) == value
    assert Symbol._from_proto(proto) == symbol


def test_symbol_from_unset_proto_is_none() -> None:
    """An unset symbol oneof maps to None, matching the optional field on Marking."""
    assert Symbol._from_proto(elements_pb2.Symbol()) is None


def test_color_round_trips_through_proto() -> None:
    color = Color("#cc0000")

    assert color._to_proto().hex_code == "#cc0000"
    assert Color._from_proto(color._to_proto()) == color


def test_color_from_unset_proto_is_none() -> None:
    assert Color._from_proto(elements_pb2.Color()) is None


def test_color_normalizes_server_casing() -> None:
    """The server's pattern is lowercase-only; tolerate case drift on read rather than raising."""
    assert Color._from_proto(elements_pb2.Color(hex_code="#CC0000")) == Color("#cc0000")


@pytest.mark.parametrize("bad", ["cc0000", "#ccc", "#gg0000", "#cc00000", "red", ""])
def test_color_rejects_values_the_server_would_reject(bad: str) -> None:
    """Guard client-side: users cannot see the backend's validation rule."""
    with pytest.raises(ValueError, match="hex color"):
        Color(bad)
