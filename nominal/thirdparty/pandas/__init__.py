"""Deprecated: use `nominal.pandas`, which this re-exports."""

import warnings

from nominal.pandas._pandas import (
    channel_to_dataframe_decimated,
    channel_to_series,
    datasource_to_dataframe,
    upload_dataframe,
    upload_dataframe_to_dataset,
)

warnings.warn(
    "nominal.thirdparty.pandas is deprecated and will be removed in a future release; "
    "import from nominal.pandas instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "channel_to_dataframe_decimated",
    "channel_to_series",
    "datasource_to_dataframe",
    "upload_dataframe",
    "upload_dataframe_to_dataset",
]
