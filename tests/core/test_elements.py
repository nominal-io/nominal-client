from __future__ import annotations

from typing import Any, Callable

import pytest

from nominal.core.elements import Symbol, _color_from_proto, _normalize_hex_color
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


@pytest.mark.parametrize(
    ("read", "empty_proto"),
    [(Symbol._from_proto, elements_pb2.Symbol()), (_color_from_proto, elements_pb2.Color())],
    ids=["symbol", "color"],
)
def test_an_unset_oneof_reads_back_as_none(read: Callable[[Any], object], empty_proto: Any) -> None:
    """An absent symbol or color becomes None, matching the optional attributes on Marking."""
    assert read(empty_proto) is None


@pytest.mark.parametrize("bad", ["cc0000", "#ccc", "#gg0000", "#cc00000", "red", ""])
def test_normalize_hex_color_rejects_anything_but_a_six_digit_hex(bad: str) -> None:
    """Mirrors the server's own `^#[0-9a-f]{6}$` constraint, failing locally instead of over the wire."""
    with pytest.raises(ValueError, match="hex color"):
        _normalize_hex_color(bad)


@pytest.mark.parametrize("given", ["#cc0000", "#CC0000", "#Cc0000"])
def test_normalize_hex_color_lowercases_so_writes_are_case_insensitive(given: str) -> None:
    """Case carries no meaning in a hex color, so any case is accepted and sent as lowercase."""
    assert _normalize_hex_color(given) == "#cc0000"
