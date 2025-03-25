from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import BinaryIO, Iterator, TypeVar

from typing_extensions import ParamSpec

logger = logging.getLogger(__name__)


Param = ParamSpec("Param")
T = TypeVar("T")


@contextmanager
def reader_writer() -> Iterator[tuple[BinaryIO, BinaryIO]]:
    rd, wd = os.pipe()
    r = open(rd, "rb")
    w = open(wd, "wb")
    try:
        yield r, w
    finally:
        w.close()
        r.close()
