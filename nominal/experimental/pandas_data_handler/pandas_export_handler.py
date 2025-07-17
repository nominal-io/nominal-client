from __future__ import annotations

from typing import Iterable, Iterator

import pandas as pd


class PandasExportHandler:
    """Manages streaming data out of Nominal using pandas dataframes."""

    def export(self) -> Iterator[pd.DataFrame]:
        """Yield dataframe slices"""
        yield pd.DataFrame()

    def start(self) -> None:
        """Start background processes and prepare the handler for export."""

    def stop(self) -> None:
        """Gracefully signal stops to background processes and shutdown handler."""

    def teardown(self) -> None:
        """Immediately teardown background processes and shutdown handler."""
