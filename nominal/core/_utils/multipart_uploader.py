from __future__ import annotations

import logging
import math
import multiprocessing
import pathlib
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from types import TracebackType
from typing import Callable, Iterable, Type

import requests
from nominal_api import upload_api
from typing_extensions import Self

from nominal.core._utils.filenames import validate_upload_filename
from nominal.core._utils.multipart import (
    DEFAULT_CHUNK_SIZE,
    _abort,
    _complete_multipart_upload,
    _initiate_multipart_upload,
    _sign_and_put_part,
    path_upload_name,
)
from nominal.core._utils.networking import HeaderProvider, create_multipart_request_session
from nominal.core.filetype import FileType

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


@dataclass(frozen=True)
class _PendingUpload:
    """A file described and validated at enqueue time, before its upload is initiated."""

    path: pathlib.Path
    file_type: FileType
    name: str
    part_size: int
    total_size: int


@dataclass
class MultipartUploader:
    """Uploads many files to S3 over one shared thread pool + HTTP session.

    Enqueue files with `enqueue_file`; track completion via the returned futures. Each file's
    whole multipart lifecycle (initiate -> sign+PUT parts -> complete/abort) is collapsed behind
    a single `Future[str]` that resolves to the object's S3 location or raises.

    Invariant: no pool task ever blocks waiting on another pool task (that would deadlock the
    single bounded pool). initiate/part/complete/abort are non-blocking submissions or callbacks.
    """

    max_workers: int
    timeout: float
    max_part_retries: int

    _upload_client: upload_api.UploadService = field(repr=False)
    _auth_header: str = field(repr=False)
    _workspace_rid: str | None = field(repr=False)
    _session: requests.Session = field(repr=False)
    _pool: ThreadPoolExecutor = field(repr=False)
    _closed: bool = field(default=False, repr=False)

    @classmethod
    def create(
        cls,
        *,
        upload_client: upload_api.UploadService,
        auth_header: str,
        workspace_rid: str | None,
        max_workers: int | None = None,
        timeout: float = 30.0,
        max_part_retries: int = 3,
        header_provider: HeaderProvider | None = None,
    ) -> Self:
        if max_workers is None:
            max_workers = multiprocessing.cpu_count()
            logger.info("Inferring core count as %d", max_workers)
        session = create_multipart_request_session(pool_size=max_workers, header_provider=header_provider)
        pool = ThreadPoolExecutor(max_workers=max_workers)
        return cls(
            max_workers,
            timeout,
            max_part_retries,
            _upload_client=upload_client,
            _auth_header=auth_header,
            _workspace_rid=workspace_rid,
            _session=session,
            _pool=pool,
            _closed=False,
        )

    # ---- lifecycle ----

    def close(self) -> None:
        if not self._closed:
            try:
                self._pool.shutdown(wait=True)
            finally:
                self._session.close()
                self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()

    # ---- public API ----

    def enqueue_file(
        self,
        path: pathlib.Path,
        *,
        file_type: FileType | None = None,
        name: str | None = None,
        part_size: int = DEFAULT_CHUNK_SIZE,
    ) -> "Future[str]":
        """Schedule a file upload and return a future for its S3 location. Non-blocking.

        Obvious errors (missing file, invalid object name) surface here synchronously; upload
        failures surface via the returned future.
        """
        file_type = file_type if file_type is not None else FileType.from_path(path)
        name = name if name is not None else path_upload_name(path, file_type)
        validate_upload_filename(name)
        total_size = path.stat().st_size  # raises FileNotFoundError synchronously if missing

        pending = _PendingUpload(path=path, file_type=file_type, name=name, part_size=part_size, total_size=total_size)
        future: Future[str] = Future()
        self._pool.submit(self._run_upload, pending, future)
        return future

    # ---- internals (run on pool threads) ----

    def _run_upload(self, pending: _PendingUpload, future: "Future[str]") -> None:
        try:
            safe_filename = f"{pending.name}{pending.file_type.extension}"
            key, upload_id = _initiate_multipart_upload(
                self._upload_client,
                self._auth_header,
                safe_filename,
                pending.file_type.mimetype,
                self._workspace_rid,
            )
            plan = _PlannedUpload(
                path=pending.path,
                key=key,
                upload_id=upload_id,
                total_size=pending.total_size,
                part_size=pending.part_size,
            )
            bounds = list(plan.parts())
            file_upload = _FileUpload(
                future=future,
                num_parts=len(bounds),
                complete=partial(_complete_multipart_upload, self._upload_client, self._auth_header, key, upload_id),
                abort=partial(_abort, self._upload_client, self._auth_header, key, upload_id),
            )
            # Submit all parts first, THEN wire callbacks — so a failure's sibling-cancel sees
            # the full list and no part is submitted into an already-settled coordinator.
            for b in bounds:
                file_upload.part_futures.append(self._pool.submit(self._upload_part, plan, b))
            for pf in file_upload.part_futures:
                pf.add_done_callback(file_upload.on_part_done)
        except Exception as e:
            # Broad on purpose: any failure before the coordinator is wired MUST settle the future
            # here, or the enqueuer's future would hang forever (the pool swallows task exceptions).
            # An initiate failure has nothing to abort; a post-initiate failure here is only
            # reachable if the pool was shut down mid-enqueue (unsupported concurrent enqueue/close)
            # and may orphan the initiated upload — acceptable under the non-atomic failure model.
            if not future.done():
                future.set_exception(e)

    def _upload_part(self, plan: _PlannedUpload, bounds: _PartBounds) -> None:
        with plan.path.open("rb") as f:
            f.seek(bounds.offset)
            data = f.read(bounds.size)
        _sign_and_put_part(
            self._upload_client,
            self._session,
            self._auth_header,
            plan.key,
            plan.upload_id,
            bounds.part_number,
            data,
            num_retries=self.max_part_retries,
            timeout=self.timeout,
        )
