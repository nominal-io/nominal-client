from __future__ import annotations

import logging
import warnings
from datetime import timedelta
from typing import TYPE_CHECKING, Literal, TypeAlias

from nominal.core._stream.batch_processor_proto import process_batch
from nominal.core._stream.write_stream import DataStream, WriteStream
from nominal.core._types import PathLike

if TYPE_CHECKING:
    from nominal.core.dataset import Dataset

logger = logging.getLogger(__name__)


def create_dataset_stream(
    dataset_rid: str,
    clients: Dataset._Clients,
    *,
    batch_size: int,
    max_wait: timedelta,
    implementation: _AnyImplementation | None,
    data_format: _AnyImplementation | None,
    file_fallback: PathLike | None,
    log_level: str | None,
    num_workers: int | None,
    track_metrics: bool,
) -> DataStream:
    """Select and construct a dataset stream, preserving legacy backend options."""
    requested = _resolve_implementation(implementation, data_format)

    rust_only_options = [
        key
        for key, value in {
            "file_fallback": file_fallback,
            "log_level": log_level,
            "num_workers": num_workers,
        }.items()
        if value is not None
    ]
    if track_metrics:
        rust_only_options.append("track_metrics")

    if requested is not None and requested != "rust":
        for key in rust_only_options:
            logger.warning("Argument %s has no effect unless `implementation='rust'`", key)

    match requested:
        case "experimental":
            from nominal.experimental.stream_v2._serializer import BatchSerializer
            from nominal.experimental.stream_v2._write_stream import WriteStreamV2

            return WriteStreamV2.create(
                clients=clients,
                serializer=BatchSerializer.create(max_workers=None),
                nominal_data_source_rid=dataset_rid,
                max_batch_size=batch_size,
                max_wait=max_wait,
                max_queue_size=0,
                track_metrics=True,
                max_workers=None,
            )
        case "rust" | None:
            try:
                from nominal.core._stream.rust_write_stream import RustWriteStream
            except ImportError as ex:
                # Falling back would drop rust-only arguments. `file_fallback` in particular is a durability
                # feature, and silently turning it into a no-op is worse than refusing.
                if requested == "rust" or rust_only_options:
                    reason = (
                        f"Using {', '.join(rust_only_options)} requires the Rust streaming backend. "
                        "nominal-streaming could not be imported."
                        if rust_only_options
                        else "implementation='rust' requires nominal-streaming, which could not be imported."
                    )
                    raise ImportError(
                        f"{reason} It ships pre-compiled binaries for a subset of platforms and interpreters; "
                        "install it with: pip install nominal-streaming"
                    ) from ex

                logger.info(
                    "nominal-streaming is unavailable, falling back to `implementation='python'` streaming. "
                    "Install nominal-streaming for higher-throughput streaming."
                )

            else:
                return RustWriteStream._from_dataset(
                    dataset_rid,
                    clients,
                    batch_size=batch_size,
                    max_wait=max_wait,
                    file_fallback=file_fallback,
                    log_level=log_level,
                    num_workers=num_workers,
                    track_metrics=track_metrics,
                )
        case "python":
            pass

    return WriteStream.create(
        batch_size,
        max_wait,
        lambda batch: process_batch(
            batch=batch,
            nominal_data_source_rid=dataset_rid,
            auth_header=clients.auth_header,
            proto_write=clients.proto_write,
        ),
    )


# Every spelling `implementation` accepts, including the superseded ones. The public signatures spell
# the literals out so they render in the docs; the plumbing below uses this.
_AnyImplementation: TypeAlias = Literal["python", "rust", "json", "protobuf", "experimental", "rust_experimental"]


# Each deprecated spelling names its retained backend and migration guidance.
_DEPRECATED_IMPLEMENTATIONS: dict[str, tuple[Literal["python", "rust", "experimental"], str]] = {
    "json": ("python", "use implementation='python'"),
    "protobuf": ("python", "use implementation='python'"),
    "rust_experimental": ("rust", "use implementation='rust'"),
    "experimental": ("experimental", "use implementation='rust' with track_metrics=True to keep runtime metrics"),
}

# _resolve_implementation -> create_dataset_stream -> get_write_stream -> the
# @warn_on_deprecated_argument wrapper -> the caller we want to blame.
#
# Update this when changing the call chain.
_DEPRECATION_STACKLEVEL = 5


def _resolve_implementation(
    implementation: _AnyImplementation | None,
    data_format: _AnyImplementation | None,
) -> Literal["python", "rust", "experimental"] | None:
    """Map what the caller asked for onto a surviving implementation, warning about old spellings.

    Returns `None` when the caller named nothing, which means "pick one for me" -- that choice
    depends on what is installed, so `create_dataset_stream` makes it.
    """
    if data_format is not None:
        if implementation is not None:
            raise ValueError("Pass only `implementation`: `data_format` is its deprecated name")

        # The deprecation itself is warned about by @warn_on_deprecated_argument on get_write_stream.
        implementation = data_format

    if implementation is not None and implementation in _DEPRECATED_IMPLEMENTATIONS:
        backend, guidance = _DEPRECATED_IMPLEMENTATIONS[implementation]
        warnings.warn(
            f"implementation={implementation!r} is deprecated: {guidance}.",
            UserWarning,
            stacklevel=_DEPRECATION_STACKLEVEL,
        )
        return backend

    match implementation:
        case None | "python" | "rust":
            return implementation
        case _:
            raise ValueError(
                f"Expected `implementation` to be one of {{python, rust, experimental}}, received {implementation!r}"
            )
