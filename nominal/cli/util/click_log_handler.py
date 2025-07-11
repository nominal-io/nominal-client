from __future__ import annotations

import logging

from typing_extensions import deprecated

from nominal.experimental.logging import ClickLogHandler, install_click_log_handler


@deprecated(
    "install_log_handler() is deprecated and will be removed in a future release. "
    "Use nominal.experimental.logging.install_click_log_handler instead."
)
def install_log_handler(level: int = logging.WARNING, no_color: bool = False) -> None:
    """Install and configure a ClickLogHandler as the default root-level logging handler.

    Args:
    ----
        level: Minimum log severity level for log messages to be allowed to be rendered and emitted.
        no_color: If true, prevents log messages from being stylized by severity level

    """
    install_click_log_handler(level=level, no_color=no_color)


__all__ = ["ClickLogHandler", "install_log_handler"]
