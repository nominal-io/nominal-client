from __future__ import annotations


class PandasExportHandler:
    """Manages streaming data out of Nominal using pandas dataframes."""

    def start(self) -> None:
        """Start background processes and prepare the handler for export."""

    def stop(self) -> None:
        """Gracefully signal stops to background processes and shutdown handler."""

    def terminate(self) -> None:
        """Immediately terminate background processes and shutdown handler."""
