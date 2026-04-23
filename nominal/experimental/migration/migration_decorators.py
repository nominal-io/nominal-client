from __future__ import annotations

import functools
import logging
import pathlib
import typing

import click

from nominal.core.client import NominalClient

logger = logging.getLogger(__name__)

Param = typing.ParamSpec("Param")
T = typing.TypeVar("T")


def migration_client_options(
    func: typing.Callable[Param, T],
) -> typing.Callable[Param, T]:
    """Decorator to add click options to a click command for dynamically creating and injecting instances of the
    NominalClient into commands based on user-provided flags to configure its creation.

    This will add options --source-profile and --destination-profile which perform the aforementioned
    configurations before spawning a NominalClient.
    """
    source_profile_option = click.option(
        "--source-profile",
        required=True,
        help=(
            "If provided, use the given named config profile for instantiating a Nominal Client "
            "for the source tenant. This is the preferred mechanism for instantiating a client today-- "
            "see `nom config profile add` to create a configuration profile. If provided, takes precedence "
            "over --token, --token-path, and --base-url."
        ),
    )

    destination_profile_option = click.option(
        "--destination-profile",
        required=True,
        help=(
            "If provided, use the given named config profile for instantiating a Nominal Client "
            "for the destination tenant. This is the preferred mechanism for instantiating a client today-- "
            "see `nom config profile add` to create a configuration profile. If provided, takes precedence "
            "over --token, --token-path, and --base-url."
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
        source_profile: str = kwargs.pop("source_profile")  # type: ignore[assignment]
        destination_profile: str = kwargs.pop("destination_profile")  # type: ignore[assignment]
        trust_store_path: pathlib.Path | None = kwargs.pop("trust_store_path")  # type: ignore[assignment]

        trust_store_str = str(trust_store_path) if trust_store_path else None

        logger.info("Instantiating client from profile '%s'", source_profile)
        source_client = NominalClient.from_profile(source_profile, trust_store_path=trust_store_str)
        logger.info("Instantiating client from profile '%s'", destination_profile)
        destination_client = NominalClient.from_profile(destination_profile, trust_store_path=trust_store_str)
        kwargs["clients"] = (source_client, destination_client)
        return func(*args, **kwargs)

    return destination_profile_option(source_profile_option(trust_store_option(wrapped_function)))
