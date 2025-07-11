from __future__ import annotations

import functools
import logging
import pathlib
import pdb  # noqa: T100
import typing

import click
import typing_extensions

from nominal._config import _DEFAULT_NOMINAL_CONFIG_PATH, get_token
from nominal.core.client import NominalClient
from nominal.experimental.logging import install_click_log_handler

logger = logging.getLogger(__name__)

Param = typing_extensions.ParamSpec("Param")
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

    This will add three options, --profile, --base-url, and --token, which perform the three aforementioned
    configurations before spawning a NominalClient.

    NOTE: any click command utilizing this decorator MUST accept a key-value argument pair named client of type
        NominalClient.
    """
    profile_option = click.option(
        "--profile",
        help=(
            "If provided, use the given named config profile for instantiating a Nominal Client. "
            "This is the preferred mechanism for instantiating a client today-- see `nom config profile add` "
            "to create a configuration profile. If provided, takes precedence over --token, --token-path, and "
            "--base-url."
        ),
    )
    url_option = click.option(
        "--base-url",
        default="https://api.gov.nominal.io/api",
        show_default=True,
        help="Base URL of the Nominal API to hit. Useful for hitting other clusters, e.g., staging for internal users.",
    )
    token_path_option = click.option(
        "--token-path",
        default=_DEFAULT_NOMINAL_CONFIG_PATH,
        type=click.Path(dir_okay=False, resolve_path=True, path_type=pathlib.Path),
        show_default=True,
        help="Path to the yaml file containing the Nominal access token for authenticating with the API",
    )
    token_option = click.option(
        "--token",
        help=(
            "API Access token to use when creating the nominal client. "
            "If provided, takes precedence over --token-path and --base-url"
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
        profile: str | None = kwargs.pop("profile")  # type: ignore[assignment]
        base_url: str = kwargs.pop("base_url", "")  # type: ignore[assignment]
        token: str | None = kwargs.pop("token")  # type: ignore[assignment]
        token_path: pathlib.Path = kwargs.pop("token_path")  # type: ignore[assignment]
        trust_store_path: pathlib.Path | None = kwargs.pop("trust_store_path")  # type: ignore[assignment]

        trust_store_str = str(trust_store_path) if trust_store_path else None

        if profile is not None:
            logger.info("Instantiating client from profile '%s'", profile)
            client = NominalClient.from_profile(profile, trust_store_path=trust_store_str)
            kwargs["client"] = client
            return func(*args, **kwargs)

        logger.warning(
            "Creating a Nominal Client without using '--profile' is deprecated! "
            "See 'https://docs.nominal.io/python/profile-migration' for details..."
        )

        if token is None:
            if token_path.exists():
                token = get_token(base_url, token_path)
            else:
                raise ValueError(
                    f"Cannot instantiate client: no token provided and token path {token_path} does not exist."
                )

        client = NominalClient.create(base_url, token=token, trust_store_path=trust_store_str)
        kwargs["client"] = client
        return func(*args, **kwargs)

    return profile_option(token_option(token_path_option(url_option(trust_store_option(wrapped_function)))))
