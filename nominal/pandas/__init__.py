"""Work with Nominal data in pandas: the ``df.nom`` accessor, and upload and export helpers.

Importing this module registers the ``df.nom`` accessor (`NomAccessor`) on every DataFrame in the
long-form shape that `nominal.io` readers produce::

    import nominal.io
    import nominal.pandas

    df = nominal.io.read("capture.avro")
    df.nom.select("daq.ai_0").nom.between("2025-10-09 08:53:21Z", "10s").nom.wide().plot()
    df.nom.to_nominal()

The functions below, which used to live in ``nominal.thirdparty.pandas``, upload DataFrames
and export channel data to DataFrames and Series.
"""

from nominal.pandas._pandas import (
    channel_to_dataframe_decimated,
    channel_to_series,
    datasource_to_dataframe,
    upload_dataframe,
    upload_dataframe_to_dataset,
)
from nominal.pandas._resolve import DEFAULT
from nominal.pandas.accessor import NomAccessor

__all__ = [
    "DEFAULT",
    "NomAccessor",
    "channel_to_dataframe_decimated",
    "channel_to_series",
    "datasource_to_dataframe",
    "upload_dataframe",
    "upload_dataframe_to_dataset",
]
