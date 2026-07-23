from __future__ import annotations

import logging
import math
import pathlib
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Callable, Iterable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _PartBounds:
    """Byte range of one multipart part. `part_number` is 1-indexed (S3 requires 1..N)."""

    part_number: int
    offset: int
    size: int


@dataclass(frozen=True)
class _PlannedUpload:
    """A file whose upload has been initiated: object key, upload id, and part layout."""

    path: pathlib.Path
    key: str
    upload_id: str
    total_size: int
    part_size: int

    def parts(self) -> Iterable[_PartBounds]:
        # An empty file still yields exactly one (zero-byte) part so completion has a part to list.
        num_parts = max(1, math.ceil(self.total_size / self.part_size))
        for i in range(num_parts):
            offset = i * self.part_size
            size = min(self.part_size, self.total_size - offset)
            yield _PartBounds(part_number=i + 1, offset=offset, size=size)


class _FileUpload:
    """Owns one file's Future and settles it exactly once.

    `complete`/`abort` are injected so this stays pure coordination — no pool, no client.
    The owning uploader populates `part_futures` after construction, then wires each
    part-future's done-callback to `on_part_done`.
    """

    def __init__(
        self,
        future: "Future[str]",
        num_parts: int,
        complete: Callable[[], str],
        abort: Callable[[BaseException], None],
    ) -> None:
        self.future = future
        self.part_futures: list[Future[None]] = []
        self._remaining = num_parts
        self._complete = complete
        self._abort = abort
        self._settled = False
        self._lock = threading.Lock()

    def on_part_done(self, fut: "Future[None]") -> None:
        # Decide the transition under the lock; run the (network) effect outside it.
        with self._lock:
            if self._settled:
                return  # absorbs cancelled/extra siblings after settling
            exc = fut.exception()
            if exc is None:
                self._remaining -= 1
                if self._remaining > 0:
                    return
                self._settled = True
                failed = False
            else:
                self._settled = True
                failed = True

        if failed:
            self._fail(exc)
        else:
            self._finish()

    def _finish(self) -> None:
        try:
            self.future.set_result(self._complete())
        except Exception as ce:  # completion itself failed
            self.future.set_exception(ce)
            self._safe_abort(ce)

    def _fail(self, exc: BaseException) -> None:
        for pf in self.part_futures:
            pf.cancel()
        self.future.set_exception(exc)
        self._safe_abort(exc)

    def _safe_abort(self, exc: BaseException) -> None:
        try:
            self._abort(exc)
        except Exception:
            logger.warning("best-effort multipart abort failed", exc_info=True)
