"""The container entrypoint that drives an extractor function from the environment."""

from __future__ import annotations

import functools
import logging
import os
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Generic, Mapping, TypeVar

from nominal.core.container_image import FileOutputFormat
from nominal.core.exceptions import ExtractorError
from nominal.experimental.extractor._context import (
    ExtractorContext,
    ManifestExtractorContext,
    SingleFileExtractorContext,
)
from nominal.experimental.extractor._env import (
    _DEFAULT_INPUT_DIR,
    _INPUT_DIR_ENV,
    _OUTPUT_DIR_ENV,
    _OUTPUT_FORMAT_ENV,
    _parse_input_specs,
    _parse_param_specs,
)

logger = logging.getLogger(__name__)

_CtxT = TypeVar("_CtxT", bound=ExtractorContext)


@dataclass
class Extractor(Generic[_CtxT]):
    """A containerized-extractor entrypoint produced by :func:`single_file_extractor` or :func:`manifest_extractor`.

    Call :meth:`run` as the container's entrypoint to drive it from the environment. In tests,
    drive it with :meth:`run` (``env=...``, ``exit=False``) rather than constructing a context by
    hand. Carries the wrapped function's metadata (``__name__``, ``__doc__``, ...) like any well-behaved
    decorator.
    """

    _fn: Callable[[_CtxT], None]
    _context_cls: type[_CtxT]

    def __post_init__(self) -> None:
        functools.update_wrapper(self, self._fn)

    def __call__(self, ctx: _CtxT) -> None:
        self._fn(ctx)

    @property
    def _is_manifest(self) -> bool:
        return issubclass(self._context_cls, ManifestExtractorContext)

    def run(self, *, env: Mapping[str, str] | None = None, exit: bool = True) -> _CtxT:
        """Run the extractor against the environment and finalize its outputs.

        Intended as the container entrypoint (``if __name__ == "__main__": my_extractor.run()``).
        On success returns the context; on failure prints a traceback and, when ``exit`` is
        True (the default), exits with a non-zero status so the ingest job fails. Pass
        ``exit=False`` to re-raise instead -- useful in tests.
        """
        environ = os.environ if env is None else env
        if env is None:
            # As the container entrypoint, make the runtime's log lines visible in the job's
            # captured output; a no-op when the author already configured logging.
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        try:
            self._check_registered_format(environ)
            ctx = self._build_context(environ)
            logger.info(
                "running %s extractor %s with %d input(s)",
                "manifest" if self._is_manifest else "single-file",
                self._fn.__name__,
                len(ctx.inputs),
            )
            self._validate_registered_contract(ctx)
            self._fn(ctx)
            count = ctx._finalize()
            logger.info("extractor %s completed with %d output(s)", self._fn.__name__, count)
            return ctx
        except BaseException:  # deliberately broad: any failure (incl. SystemExit/KeyboardInterrupt
            # from user code) must fail the ingest job cleanly, not just Exception subclasses.
            if exit:
                traceback.print_exc()
                sys.exit(1)
            raise

    def _validate_registered_contract(self, ctx: _CtxT) -> None:
        """Check the registered contract against the environment once, at startup.

        Advisory only: a registered-required parameter left unset, or a registered input whose
        mounted path is missing, earns a warning at the top of the job log -- but the run
        proceeds, since only code that actually reads the affected name is impacted.
        """
        for param_spec in ctx._param_specs or []:
            if param_spec.required and ctx._env.get(param_spec.environment_variable) is None:
                logger.warning(
                    "required parameter %s (%r) has no value set; ctx.param will fail if it is read",
                    param_spec.environment_variable,
                    param_spec.name,
                )
        for input_spec in ctx._input_specs or []:
            if not Path(input_spec.path).is_file():
                logger.warning(
                    "input %s (%r) is not present at %s",
                    input_spec.environment_variable,
                    input_spec.name,
                    input_spec.path,
                )

    def _check_registered_format(self, env: Mapping[str, str]) -> None:
        """Assert the decorator's contract against the injected registered output format.

        When ``_NOMINAL_OUTPUT_FORMAT`` is absent (a local run) the decorator's word is law.
        """
        registered = env.get(_OUTPUT_FORMAT_ENV)
        if not registered:
            return
        registered_manifest = registered == FileOutputFormat.MANIFEST.value
        if registered_manifest == self._is_manifest:
            return
        declared, alternative = (
            ("@manifest_extractor", "@single_file_extractor")
            if self._is_manifest
            else ("@single_file_extractor", "@manifest_extractor")
        )
        raise ExtractorError(
            f"{declared} disagrees with the image's registered output format {registered!r} "
            f"(_NOMINAL_OUTPUT_FORMAT); re-register the image or switch to {alternative} so the "
            "code and the registration agree"
        )

    def _build_context(self, env: Mapping[str, str]) -> _CtxT:
        output_dir = env.get(_OUTPUT_DIR_ENV)
        if not output_dir:
            raise ExtractorError(f"{_OUTPUT_DIR_ENV} is not set; this code must run inside a Nominal extractor")
        input_dir = env.get(_INPUT_DIR_ENV, _DEFAULT_INPUT_DIR)
        return self._context_cls(
            output_dir=Path(output_dir),
            _env=env,
            _input_dir=Path(input_dir),
            _input_specs=_parse_input_specs(env),
            _param_specs=_parse_param_specs(env),
        )


def single_file_extractor(fn: Callable[[SingleFileExtractorContext], None]) -> Extractor[SingleFileExtractorContext]:
    """Turn ``def fn(ctx: SingleFileExtractorContext) -> None`` into a single-file extractor entrypoint.

    For images registered with a single-file output format (``PARQUET``, ``CSV``, ...): the ingest
    pipeline ingests exactly one output file, parsed per the registered format. Declare it with
    :meth:`SingleFileExtractorContext.set_output`. If the image's registered format turns out to be
    ``MANIFEST``, :meth:`Extractor.run` fails at startup with a clear error.

    Example::

        from nominal.experimental.extractor import SingleFileExtractorContext, single_file_extractor

        @single_file_extractor
        def convert(ctx: SingleFileExtractorContext) -> None:
            table = read_input(ctx.input())
            out = ctx.output_dir / "converted.parquet"
            write_parquet(table, out)
            ctx.set_output(out)

        if __name__ == "__main__":
            convert.run()
    """
    return Extractor(fn, SingleFileExtractorContext)


def manifest_extractor(fn: Callable[[ManifestExtractorContext], None]) -> Extractor[ManifestExtractorContext]:
    """Turn ``def fn(ctx: ManifestExtractorContext) -> None`` into a manifest extractor entrypoint.

    For images registered with the ``MANIFEST`` output format: declare each output file (and its
    per-file ingest type, tag columns, channel prefix, and optional epoch or relative timestamp
    metadata) with :meth:`ManifestExtractorContext.add_output`, and any video with
    :meth:`ManifestExtractorContext.add_video`; ``manifest.json`` is written automatically when the
    function returns. If the image's registered format is not ``MANIFEST``, :meth:`Extractor.run`
    fails at startup with a clear error.

    Example::

        from nominal.experimental.extractor import IngestType, ManifestExtractorContext, manifest_extractor

        @manifest_extractor
        def split(ctx: ManifestExtractorContext) -> None:
            table = read_parquet(ctx.input())
            for i, chunk in enumerate(chunks_of(table, int(ctx.get_param("PARTS", "2")))):
                out = ctx.output_dir / f"part_{i}.parquet"
                write_parquet(chunk, out)
                ctx.add_output(out, ingest_type=IngestType.TABULAR)

            footage = ctx.output_dir / "camera.mp4"
            write_video(footage)
            ctx.add_video(footage, channel="camera/front", start=recording_started_at)

        if __name__ == "__main__":
            split.run()
    """
    return Extractor(fn, ManifestExtractorContext)
