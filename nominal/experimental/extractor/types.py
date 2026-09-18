"""Immutable parameter converters with safe, author-controlled diagnostics."""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass

__all__ = ["BadParameter", "Choice", "IntRange", "FloatRange"]


class BadParameter(ValueError):
    """Reject a parameter with a message safe to include in extractor diagnostics.

    The message is displayed to the user. Do not include raw parameter values or secrets.
    """


@dataclass(frozen=True, init=False)
class Choice:
    """Accept an exact, case-sensitive member of a nonempty collection of unique strings."""

    choices: tuple[str, ...]

    def __init__(self, choices: Iterable[str]) -> None:
        """Snapshot the allowed strings so subsequent collection changes have no effect."""
        if isinstance(choices, str):
            raise TypeError("choices must be a collection of strings")
        values = tuple(choices)
        if not values or any(not isinstance(value, str) for value in values):
            raise ValueError("choices must contain strings and must not be empty")
        if len(set(values)) != len(values):
            raise ValueError("choices must be unique")
        object.__setattr__(self, "choices", values)

    def _validate(self, value: object) -> None:
        if not isinstance(value, str) or value not in self.choices:
            raise BadParameter(f"must be one of {', '.join(repr(choice) for choice in self.choices)}")

    def __call__(self, value: str) -> str:
        self._validate(value)
        return value


def _check_range(value: int | float, minimum: int | float | None, maximum: int | float | None) -> None:
    if minimum is not None and value < minimum:
        raise BadParameter(f"must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise BadParameter(f"must be at most {maximum}")


@dataclass(frozen=True)
class IntRange:
    """Convert an integer with optional inclusive minimum and maximum bounds."""

    min: int | None = None
    max: int | None = None

    def __post_init__(self) -> None:
        """Validate bounds before the converter is used by an extractor."""
        for bound in (self.min, self.max):
            if bound is not None and (not isinstance(bound, int) or isinstance(bound, bool)):
                raise TypeError("integer bounds must be integers")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError("minimum must not exceed maximum")

    def _validate(self, value: object) -> None:
        if not isinstance(value, int) or isinstance(value, bool):
            raise BadParameter("must be an integer")
        _check_range(value, self.min, self.max)

    def __call__(self, value: str) -> int:
        try:
            converted = int(value)
        except (ValueError, TypeError):
            raise BadParameter("must be an integer") from None
        self._validate(converted)
        return converted


@dataclass(frozen=True)
class FloatRange:
    """Convert a finite number with optional inclusive minimum and maximum bounds."""

    min: float | None = None
    max: float | None = None

    def __post_init__(self) -> None:
        """Validate bounds before the converter is used by an extractor."""
        for bound in (self.min, self.max):
            if bound is not None:
                _validate_number(bound)
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError("minimum must not exceed maximum")

    def _validate(self, value: object) -> None:
        _validate_number(value)
        assert isinstance(value, (int, float))
        _check_range(value, self.min, self.max)

    def __call__(self, value: str) -> float:
        try:
            converted = float(value)
        except (ValueError, TypeError):
            raise BadParameter("must be a finite number") from None
        self._validate(converted)
        return converted


def _validate_number(value: object) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise BadParameter("must be a finite number")
    # Integers are finite without conversion to float, including arbitrary precision values.
    if isinstance(value, float) and not math.isfinite(value):
        raise BadParameter("must be a finite number")


def _boolean(value: str) -> bool:
    if value.lower() in ("true", "1", "yes", "on"):
        return True
    if value.lower() in ("false", "0", "no", "off"):
        return False
    raise BadParameter("must be a boolean")


def _parameter_converter(converter: Callable[[str], object], default: object) -> Callable[[str], object]:
    """Select conversion policy once and validate an already-converted default.

    None means no concrete default. Arbitrary callable converters
    keep ownership of their result types and are never invoked for defaults.
    """
    if not callable(converter):
        raise TypeError("parameter type must be callable")
    if converter is int:
        converter = IntRange()
    elif converter is float:
        converter = FloatRange()
    if default is not None:
        if isinstance(converter, (Choice, IntRange, FloatRange)):
            converter._validate(default)
        elif converter is str and not isinstance(default, str):
            raise TypeError("string parameter default must be a string")
        elif converter is bool and not isinstance(default, bool):
            raise TypeError("boolean parameter default must be a boolean")
    return _boolean if converter is bool else converter
