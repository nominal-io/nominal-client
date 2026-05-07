from __future__ import annotations

import logging
import typing

import click


class ClickLogHandler(logging.StreamHandler):  # type: ignore[type-arg]
    """Logging stream handler that routes and styles log messages through click
    instead of directly routing to stderr.

    This has several advantages, namely, differing log levels can be printed differently
    to provide additional visual amplification of errors and warnings. Furthermore,
    there are numerous improvements to support within windows.

    See: https://click.palletsprojects.com/en/8.1.x/utils/#printing-to-stdout
    """

    LEVEL_TO_COLOR_MAP = {
        logging.DEBUG: "blue",
        logging.INFO: "cyan",
        logging.WARNING: "yellow",
        logging.ERROR: "red",
        logging.FATAL: "bright_red",
    }

    def __init__(self, stream: typing.IO[str] | None = None, no_color: bool = False):
        """Instantiate a ClickLogHandler

        Args:
        ----
            stream: TextIO stream to pipe filtered and rendered log messages to
            no_color: If True, don't colorize/style log messages by level during rendering

        """
        if stream is None:
            stream = click.get_text_stream("stderr")

        super().__init__(stream)

        self._no_color = no_color

    def format(self, record: logging.LogRecord) -> str:
        """Add colors when formatting log records"""
        msg = super().format(record)
        if not self._no_color:
            msg = click.style(msg, fg=self.LEVEL_TO_COLOR_MAP.get(record.levelno, "white"))

        return msg


def install_click_log_handler(level: int = logging.WARNING, no_color: bool = False) -> ClickLogHandler:
    """Install and configure a ClickLogHandler as the default root-level logging handler.

    Args:
        level: Minimum log severity level for log messages to be allowed to be rendered and emitted.
        no_color: If true, prevents log messages from being stylized by severity level

    Returns:
        Attached ClickLogHandler

    """
    logging.basicConfig(level=level)

    global_logger = logging.getLogger()
    click_handler = ClickLogHandler(no_color=no_color)
    click_handler.setLevel(level)
    click_handler.formatter = global_logger.handlers[0].formatter
    global_logger.handlers = [click_handler]

    return click_handler
