from __future__ import annotations

import functools
import logging
import pathlib
import pdb  # noqa: T100
import typing

import click

from nominal.core.client import NominalClient
from nominal.experimental.logging import install_click_log_handler

logger = logging.getLogger(__name__)

Param = typing.ParamSpec("Param")
T = typing.TypeVar("T")


def verbosity_switch(func: typing.Callable[Param, T]) -> typing.Callable[..., T]:
    """Decorator to add click options to a click command to control log verbosity and styling in a uniform way.

    This adds the --verbose and --no-color options to the provided command, and processes any user input
    automatically behind the scenes. When multiple verbose flags are provided, increasing levels of log
    verbosity are unlocked. In addition, logs are routed to an installed ClickLogHandler, which stylizes
    and colorizes log messages based on their severity. To disable this colorization, users may use the
    --no-color option.

    NOTE: this must be invoked prior to any log messages being routed through a logger from the logging module.
    """
    verbosity_option = click.option(
        "-v",
        "--verbose",
        default=0,
        count=True,
        help="Verbosity to use within the CLI. Pass -v to allow info-level logs, or -vv for debug-level.",
    )

    color_option = click.option("--no-color", is_flag=True, help="If provided, don't color terminal log output")

    @functools.wraps(func)
    def wrapped_function(*args: Param.args, **kwargs: Param.kwargs) -> T:
        verbose: int = kwargs.pop("verbose")  # type: ignore[assignment]
        no_color: bool = kwargs.pop("no_color")  # type: ignore[assignment]

        log_level = logging.NOTSET
        if verbose == 0:
            log_level = logging.WARNING
        elif verbose == 1:
            log_level = logging.INFO
        elif verbose >= 2:
            log_level = logging.DEBUG

        # Store parameters for later use
        ctx = click.get_current_context()
        if ctx.obj is None:
            ctx.obj = {}

        ctx.obj["log_level"] = log_level
        ctx.obj["no_color"] = no_color

        install_click_log_handler(level=log_level, no_color=no_color)
        return func(*args, **kwargs)

    return color_option(verbosity_option(wrapped_function))


def debug_switch(func: typing.Callable[Param, T]) -> typing.Callable[..., T]:
    """Decorator to add click options to a click command for dynamically spawning a post-mortem debugger at the
    site of any raised exceptions that would have otherwise crashed the program.

    This is primarily directed at developers, and as such, the --debug flag that is added to click commands
    is witheld from the emitted --help text.
    """
    debug_option = click.option(
        "--debug",
        is_flag=True,
        hidden=True,
        help="Spawn a python debugger upon any exception being raised",
    )

    @functools.wraps(func)
    def wrapped_function(*args: Param.args, **kwargs: Param.kwargs) -> T:
        debug: bool = kwargs.pop("debug", False)  # type: ignore[assignment]
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if debug:
                pdb.post_mortem()

            raise e

    return debug_option(wrapped_function)


def global_options(func: typing.Callable[Param, T]) -> typing.Callable[..., T]:
    """Helper decorator that combines together debug_switch and verbosity_switch to provide commonly and globally
    used utility options across all CLI endpoints within the nominal python client.
    """
    return functools.wraps(func)(debug_switch(verbosity_switch(func)))


def client_options(func: typing.Callable[Param, T]) -> typing.Callable[..., T]:
    """Decorator to add click options to a click command for dynamically creating and injecting an instance of the
    NominalClient into commands based on user-provided flags to configure its creation.

    This will add an option --profile which perform the aforementioned configurations before spawning a NominalClient.

    NOTE: any click command utilizing this decorator MUST accept a key-value argument pair named client of type
        NominalClient.
    """
    profile_option = click.option(
        "--profile",
        required=True,
        help=(
            "If provided, use the given named config profile for instantiating a Nominal Client. "
            "This is the preferred mechanism for instantiating a client today-- see `nom config profile add` "
            "to create a configuration profile. If provided, takes precedence over --token, --token-path, and "
            "--base-url."
        ),
    )
    trust_store_option = click.option(
        "--trust-store-path",
        type=click.Path(dir_okay=False, exists=True, resolve_path=True, path_type=pathlib.Path),
        help=(
            "Path to a trust store CA root file to initiate SSL connections. "
            "If not provided, defaults to certifi's trust store."
        ),
    )

    @functools.wraps(func)
    def wrapped_function(
        *args: Param.args,
        **kwargs: Param.kwargs,
    ) -> T:
        profile: str = kwargs.pop("profile")  # type: ignore[assignment]
        trust_store_path: pathlib.Path | None = kwargs.pop("trust_store_path")  # type: ignore[assignment]

        trust_store_str = str(trust_store_path) if trust_store_path else None

        logger.info("Instantiating client from profile '%s'", profile)
        client = NominalClient.from_profile(profile, trust_store_path=trust_store_str)
        kwargs["client"] = client
        return func(*args, **kwargs)

    return profile_option(trust_store_option(wrapped_function))
