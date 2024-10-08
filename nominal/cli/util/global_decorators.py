from __future__ import annotations

import functools
import logging
import pathlib
import pdb
import typing

import click

from ..._config import get_token
from ...core.client import NominalClient
from .click_log_handler import install_log_handler


def verbosity_switch(func: typing.Callable[..., None]) -> typing.Callable[..., typing.Any]:
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
    def wrapped_function(
        *args: tuple[typing.Any, ...], verbose: int, no_color: bool, **kwargs: dict[str, typing.Any]
    ) -> typing.Any:
        log_level = logging.NOTSET
        if verbose == 0:
            log_level = logging.WARNING
        elif verbose == 1:
            log_level = logging.INFO
        else:
            log_level = logging.DEBUG

        install_log_handler(level=log_level, no_color=no_color)
        return func(*args, **kwargs)

    return color_option(verbosity_option(wrapped_function))


def debug_switch(func: typing.Callable[..., None]) -> typing.Callable[..., None]:
    """Decorator to add click options to a click command for dynamically spawning a post-mortem debugger at the
    site of any raised exceptions that would have otherwise crashed the program.

    This is primarily directed at developers, and as such, the --debug flag that is added to click commands
    is witheld from the emitted --help text.
    """
    debug_option = click.option(
        "--debug",
        is_flag=True,
        default=False,
        show_default=True,
        hidden=True,
        help="If true, spawn a python debugger upon any exception being raised",
    )

    @functools.wraps(func)
    def wrapped_function(*args: tuple[typing.Any, ...], debug: bool, **kwargs: dict[str, typing.Any]) -> typing.Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if debug:
                pdb.post_mortem()

            raise e

    return debug_option(wrapped_function)


def global_options(func: typing.Callable[..., None]) -> typing.Callable[..., None]:
    """Helper decorator that combines together debug_switch and verbosity_switch to provide commonly and globally
    used utility options across all CLI endpoints within the nominal python client.
    """
    return functools.wraps(func)(debug_switch(verbosity_switch(func)))


def client_options(func: typing.Callable[..., None]) -> typing.Callable[..., None]:
    """Decorator to add click options to a click command for dynamically creating and injecting an instance of the
    NominalClient into commands based on user-provided flags containing the base API url and a path to a configuration
    file containing an API Access Key.

    This will add two options, --base-url and --token, which perform the two aforementioned configurations before spawning
    a NominalClient.

    NOTE: any click command utilizing this decorator MUST accept a key-value argument pair named client of type NominalClient.
    """
    url_option = click.option(
        "--base-url",
        default="https://api.gov.nominal.io/api",
        show_default=True,
        help="Base URL of the Nominal API to hit. Useful for hitting other clusters, e.g., staging for internal users.",
    )
    token_option = click.option(
        "--token",
        default=str(pathlib.Path("~/.nominal.yml").expanduser().resolve()),
        show_default=True,
        help="Path to the yaml file containing the Nominal access token for authenticating with the API",
    )

    @functools.wraps(func)
    def wrapped_function(
        *args: tuple[typing.Any, ...], base_url: str, token: str | None, **kwargs: dict[str, typing.Any]
    ) -> typing.Any:
        if token is None:
            token = get_token(base_url)

        client = NominalClient.create(base_url, token)
        return func(*args, client=client, **kwargs)

    return url_option(token_option(wrapped_function))
