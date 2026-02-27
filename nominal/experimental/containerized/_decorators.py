from __future__ import annotations

import functools
import inspect
import logging
import os
from collections.abc import Mapping
from typing import Any, Callable, ParamSpec, Type, TypeVar, get_origin, get_type_hints

logger = logging.getLogger(__name__)

Param = ParamSpec("Param")
T = TypeVar("T")

_FILE_ENV_PREFIX = "__nominal_file_"
_SECRET_ENV_PREFIX = "__nominal_secret_"
_PARAMETER_ENV_PREFIX = "__nominal_parameter_"


def _validate_param_in_signature(
    func: Callable[Param, T],
    kwarg_name: str,
    kwarg_type: Type[Any],
    *,
    localns: dict[str, Any] | None = None,
) -> None:
    """Validate that `func` has a parameter `kwarg_name` with the expected annotation.

    Required behavior:
      - The parameter must exist.
      - It must be passable as a keyword argument.
      - The annotation must be exactly `kwarg_type`, or a parameterized version of it (for
        example `list[str]` for `list`).

    Raises:
      TypeError: if the signature/annotation does not match requirements.
    """
    target = inspect.unwrap(func)

    try:
        signature = inspect.signature(target)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{target!r} has no inspectable signature") from exc

    parameter = signature.parameters.get(kwarg_name)
    if parameter is None:
        raise TypeError(f"{target.__name__} is missing required parameter {kwarg_name!r}")

    if parameter.kind not in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    ):
        raise TypeError(
            f"{target.__name__} parameter {kwarg_name!r} is not usable as a keyword argument "
            f"(kind={parameter.kind})"
        )

    # Prefer get_type_hints to resolve string annotations from __future__ import annotations.
    try:
        type_hints = get_type_hints(
            target,
            globalns=getattr(target, "__globals__", None),
            localns=localns,
        )
        annotation = type_hints.get(kwarg_name, None)
    except Exception:
        annotation = None

    # Fallback for better error reporting when annotations exist but could not be resolved.
    if annotation is None:
        raw_annotations = inspect.get_annotations(target, eval_str=False)
        if kwarg_name in raw_annotations:
            raise TypeError(
                f"{target.__name__} parameter {kwarg_name!r} annotation could not be resolved "
                f"(raw={raw_annotations[kwarg_name]!r}). Ensure {kwarg_type.__name__} is importable at runtime."
            )
        raise TypeError(f"{target.__name__} parameter {kwarg_name!r} must have a type annotation")

    if annotation is kwarg_type:
        return

    if get_origin(annotation) is kwarg_type:
        return

    raise TypeError(
        f"{target.__name__} parameter {kwarg_name!r} must be annotated as {kwarg_type.__name__} "
        f"or {kwarg_type.__name__}[...]; got {annotation!r}"
    )


def _collect_containerized_env_inputs(
    env: Mapping[str, str] | None = None,
) -> tuple[list[str], dict[str, str], dict[str, str]]:
    """Collect containerized runtime inputs from environment variables."""
    environment = os.environ if env is None else env

    files: list[str] = []
    secrets: dict[str, str] = {}
    parameters: dict[str, str] = {}

    for key in sorted(environment):
        value = environment[key]
        if key.startswith(_FILE_ENV_PREFIX):
            files.append(value)
        elif key.startswith(_SECRET_ENV_PREFIX):
            secret_name = key.removeprefix(_SECRET_ENV_PREFIX)
            secrets[secret_name] = value
        elif key.startswith(_PARAMETER_ENV_PREFIX):
            parameter_name = key.removeprefix(_PARAMETER_ENV_PREFIX)
            parameters[parameter_name] = value

    return files, secrets, parameters


def containerized_env_inputs(func: Callable[Param, T]) -> Callable[..., T]:
    """Auto-wire files, secrets, and parameters from environment variables.

    NOTE: wrapped functions must accept the following keyword arguments:
      - files: list
      - secrets: dict
      - parameters: dict
    """
    _validate_param_in_signature(func, "files", list)
    _validate_param_in_signature(func, "secrets", dict)
    _validate_param_in_signature(func, "parameters", dict)

    @functools.wraps(func)
    def wrapped_function(*args: Param.args, **kwargs: Param.kwargs) -> T:
        files, secrets, parameters = _collect_containerized_env_inputs()
        logger.debug(
            "Collected %d files, %d secrets, and %d parameters from environment",
            len(files),
            len(secrets),
            len(parameters),
        )

        kwargs["files"] = files
        kwargs["secrets"] = secrets
        kwargs["parameters"] = parameters
        return func(*args, **kwargs)

    return wrapped_function
