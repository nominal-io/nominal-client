"""Deprecated channel-type imports; use ``nominal.core`` instead."""

import warnings

from nominal.core.channel import BatchAddChannelsResult as BatchAddChannelsResult
from nominal.core.channel import CreateChannelRequest as CreateChannelRequest

warnings.warn(
    "Importing channel types from nominal.core.datasource is deprecated; "
    "import CreateChannelRequest and BatchAddChannelsResult from nominal.core instead.",
    DeprecationWarning,
    stacklevel=2,
)
