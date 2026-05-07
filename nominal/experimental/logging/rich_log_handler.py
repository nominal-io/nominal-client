from __future__ import annotations

import atexit
import logging
import logging.handlers
import queue
from typing import Any

from rich.console import Console
from rich.logging import RichHandler


class _QueueListener(logging.handlers.QueueListener):
    pass


def configure_rich_logging(console: Console, level: int = logging.INFO) -> _QueueListener:
    """Configure root logging via QueueHandler → RichHandler.

    Returns the QueueListener so we can stop it cleanly on exit.
    """
    log_queue: queue.Queue[Any] = queue.Queue(-1)

    # QueueHandler routes all records (including from background threads)
    qh = logging.handlers.QueueHandler(log_queue)
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers[:] = [qh]

    # RichHandler actually renders to the console, but is called by the listener thread
    rich_handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        show_time=True,
        markup=True,
        enable_link_path=True,
    )
    rich_handler.setLevel(level)

    listener = _QueueListener(log_queue, rich_handler, respect_handler_level=True)
    listener.start()

    # Ensure clean shutdown
    atexit.register(listener.stop)
    return listener
