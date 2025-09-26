import warnings
from typing import cast

from nominal.core import exceptions


def __getattr__(name: str) -> type[exceptions.NominalError]:
    warnings.warn(
        "nominal.exceptions is deprecated and will be removed in a future version. "
        "Use nominal.core.exceptions instead.",
        UserWarning,
        stacklevel=3,
    )
    return cast(type[exceptions.NominalError], getattr(exceptions, name))
