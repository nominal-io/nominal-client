"""Collect and validate declarations, then describe a frozen extractor definition."""

from __future__ import annotations

import inspect
import keyword
import re
from dataclasses import dataclass, field
from enum import Enum
from functools import update_wrapper
from typing import TYPE_CHECKING, Callable, Generic, Mapping, ParamSpec

from nominal import ts
from nominal.core.container_image import FileOutputFormat
from nominal.experimental.extractor._env import _INPUT_DIR_ENV, _OUTPUT_DIR_ENV

if TYPE_CHECKING:
    from nominal.experimental.extractor._arguments import _Input, _Parameter
    from nominal.experimental.extractor._errors import _ErrorMapping

_P = ParamSpec("_P")


class _Missing(Enum):
    """Distinguish an omitted decorator option from an explicit None default.

    An enum member provides singleton identity and type narrowing for decorator signatures,
    so inference and requiredness do not need casts or a permissive object-typed option.
    """

    VALUE = "missing"


_MISSING = _Missing.VALUE


@dataclass
class _DeclaredCallback(Generic[_P]):
    """Collect and validate stacked declarations while preserving the callback signature.

    Decorators mutate this builder until an outer extractor decorator snapshots its arguments
    and error mappings. Keeping the metadata on a typed callable avoids attaching ad-hoc
    attributes to user functions and allows ordinary functools.wraps decorators to compose.
    """

    __wrapped__: Callable[_P, None]
    arguments: tuple[_Input | _Parameter, ...] = ()
    errors: dict[type[Exception], _ErrorMapping] = field(default_factory=dict)

    def __post_init__(self) -> None:
        update_wrapper(self, self.__wrapped__, updated=())

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
        self.__wrapped__(*args, **kwargs)

    def validate(self) -> None:
        """Validate declared bindings against the callback signature."""
        if not self.arguments:
            return  # Keep legacy callback acceptance unchanged.
        signature = inspect.signature(self.__wrapped__)
        parameters = list(signature.parameters.values())
        if not parameters or parameters[0].kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            raise TypeError("extractor callback requires a first positional context argument")
        targets: set[str] = set()
        variables: set[str] = set()
        names: set[tuple[type, str]] = set()
        for declaration in self.arguments:
            target = signature.parameters.get(declaration.argument)
            if (
                target is None
                or target is parameters[0]
                or target.kind not in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
            ):
                raise TypeError(f"invalid callback argument {declaration.argument!r}")
            if target.default is not inspect.Parameter.empty:
                raise TypeError(f"declare the default for {declaration.argument!r} in its decorator")
            variable = declaration.spec.environment_variable
            name_key = (type(declaration), declaration.spec.name)
            if declaration.argument in targets or variable in variables or name_key in names:
                raise ValueError(f"duplicate argument, envvar or display name for {declaration.argument!r}")
            targets.add(declaration.argument)
            variables.add(variable)
            names.add(name_key)
        signature.bind(object(), **dict.fromkeys(targets))


@dataclass(frozen=True)
class _Definition:
    """Snapshot the declarations shared by execution and registration exports.

    The runner supplies immutable argument and error collections, isolating this entrypoint
    from later decorators applied to the source callback. Registration derives its metadata
    from these same bindings; missing image defaults remain valid for runtime-only use.
    """

    callback: Callable[..., None]
    arguments: tuple[_Input | _Parameter, ...]
    errors: Mapping[type[Exception], _ErrorMapping]
    output_format: FileOutputFormat | None
    timestamp_column: str | None
    timestamp_type: ts._AnyTimestampType | None


def _declare(fn: Callable[_P, None]) -> _DeclaredCallback[_P]:
    # The runner consumes declarations, so import locally to avoid a dependency cycle.
    from nominal.experimental.extractor.runner import Extractor

    existing = inspect.unwrap(fn, stop=lambda value: isinstance(value, (Extractor, _DeclaredCallback)))
    if isinstance(existing, Extractor):
        raise TypeError("the extractor decorator must be outermost, above @input, @parameter and @error")
    if isinstance(existing, _DeclaredCallback):
        return existing if existing is fn else _DeclaredCallback(fn, existing.arguments, dict(existing.errors))
    return _DeclaredCallback(fn)


def _names(argument: str, envvar: str | None, name: str | None) -> tuple[str, str]:
    if not argument.isidentifier() or keyword.iskeyword(argument):
        raise ValueError(f"invalid argument name {argument!r}")
    environment_variable = argument.upper() if envvar is None else envvar
    if (
        re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", environment_variable) is None
        or environment_variable.startswith("_NOMINAL_")
        or environment_variable in {_OUTPUT_DIR_ENV, _INPUT_DIR_ENV}
    ):
        raise ValueError(f"invalid or reserved envvar {environment_variable!r}")
    display_name = argument if name is None else name
    if not display_name.strip():
        raise ValueError("name must not be empty")
    return environment_variable, display_name
