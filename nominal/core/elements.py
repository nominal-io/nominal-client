from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

from typing_extensions import Self, assert_never

from nominal.protos.scout.elements.v1 import elements_pb2


class SymbolKind(Enum):
    """The kind of symbol carried by a `Symbol`."""

    ICON = "ICON"
    """A named icon, e.g. `castle`."""
    EMOJI = "EMOJI"
    """An emoji name, e.g. `:castle:`."""
    IMAGE = "IMAGE"
    """A URL pointing at an image."""


@dataclass(frozen=True)
class Symbol:
    """A symbol used to identify a resource in the Nominal app.

    Construct with `Symbol.icon`, `Symbol.emoji`, or `Symbol.image` rather than calling
    the constructor directly.
    """

    kind: SymbolKind
    value: str

    @classmethod
    def icon(cls, name: str) -> Self:
        """A symbol from a named icon, e.g. `castle`."""
        return cls(SymbolKind.ICON, name)

    @classmethod
    def emoji(cls, name: str) -> Self:
        """A symbol from an emoji name, e.g. `:castle:`."""
        return cls(SymbolKind.EMOJI, name)

    @classmethod
    def image(cls, url: str) -> Self:
        """A symbol from the URL of an image."""
        return cls(SymbolKind.IMAGE, url)

    def _to_proto(self) -> elements_pb2.Symbol:
        match self.kind:
            case SymbolKind.ICON:
                return elements_pb2.Symbol(icon=self.value)
            case SymbolKind.EMOJI:
                return elements_pb2.Symbol(emoji=self.value)
            case SymbolKind.IMAGE:
                return elements_pb2.Symbol(image=self.value)
            case _:
                assert_never(self.kind)

    @classmethod
    def _from_proto(cls, symbol: elements_pb2.Symbol) -> Self | None:
        """The symbol carried by a proto, or None if the oneof is unset."""
        match symbol.WhichOneof("symbol"):
            case "icon":
                return cls(SymbolKind.ICON, symbol.icon)
            case "emoji":
                return cls(SymbolKind.EMOJI, symbol.emoji)
            case "image":
                return cls(SymbolKind.IMAGE, symbol.image)
            case _:
                return None


_HEX_COLOR = re.compile(r"^#[0-9a-f]{6}$")


@dataclass(frozen=True)
class Color:
    """A color used to identify a resource in the Nominal app.

    Construct with `Color.create` rather than calling the constructor directly: only `create`
    checks that the hex code is one Nominal accepts.
    """

    hex_code: str
    """A lowercase six-digit hex color, e.g. `#cc0000`.

    Validated by `Color.create`; the constructor itself does not check it.
    """

    @classmethod
    def create(cls, hex_code: str) -> Self:
        """A color from a lowercase six-digit hex code, e.g. `#cc0000`.

        Args:
            hex_code: The hex code, including the leading `#`.

        Raises:
            ValueError: If `hex_code` is not a lowercase six-digit hex color, which Nominal
                would reject.
        """
        if _HEX_COLOR.match(hex_code) is None:
            raise ValueError(f"expected a lowercase six-digit hex color such as '#cc0000', got {hex_code!r}")
        return cls(hex_code=hex_code)

    def _to_proto(self) -> elements_pb2.Color:
        return elements_pb2.Color(hex_code=self.hex_code)

    @classmethod
    def _from_proto(cls, color: elements_pb2.Color) -> Self | None:
        """The color carried by a proto, or None if the oneof is unset."""
        if color.WhichOneof("color") != "hex_code":
            return None
        return cls(hex_code=color.hex_code)
