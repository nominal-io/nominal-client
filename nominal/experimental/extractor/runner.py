"""The container entrypoint that drives an extractor function from the environment."""

from __future__ import annotations

import functools
import logging
import os
import sys
import traceback
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Generic, Mapping, TypeVar

from nominal import ts
from nominal.core.container_image import (
    REGISTERABLE_OUTPUT_FORMATS,
    FileOutputFormat,
)
from nominal.core.exceptions import ExtractorError
from nominal.experimental.extractor._definition import _declare, _Definition
from nominal.experimental.extractor._env import (
    _DEFAULT_INPUT_DIR,
    _DEFAULT_TERMINATION_LOG_PATH,
    _INPUT_DIR_ENV,
    _OUTPUT_DIR_ENV,
    _OUTPUT_FORMAT_ENV,
    _parse_input_specs,
    _parse_param_specs,
)
from nominal.experimental.extractor._errors import _resolve_error
from nominal.experimental.extractor._registration import _catalog_manifest, _registration_kwargs, _RegistrationKwargs
from nominal.experimental.extractor.context import (
    ExtractorContext,
    ManifestExtractorContext,
)

__all__ = ["Extractor"]


logger = logging.getLogger(__name__)

_CtxT = TypeVar("_CtxT", bound=ExtractorContext)


class Extractor(Generic[_CtxT]):
    """A containerized-extractor entrypoint produced by :func:`single_file_extractor` or :func:`manifest_extractor`.

    Declare callback inputs and parameters with ``@input`` and ``@parameter`` beneath the
    outer extractor decorator. They bind as keyword arguments after the output context.
    Use :meth:`registration_kwargs` to export the same contract for image registration.
    Call :meth:`run` as the container's entrypoint to drive it from the environment. In tests,
    drive it with :meth:`run` (``env=...``, ``exit=False``) rather than constructing a context by
    hand. Carries the wrapped function's metadata (``__name__``, ``__doc__``, ...) like any well-behaved
    decorator.
    """

    def __init__(
        self,
        _fn: Callable[..., None],
        _context_cls: type[_CtxT],
        _output_format: FileOutputFormat | None = None,
        _timestamp_column: str | None = None,
        _timestamp_type: ts._AnyTimestampType | None = None,
    ) -> None:
        """Assemble a callback's declarations; prefer the public extractor decorators."""
        self._context_cls = _context_cls
        callback = _declare(_fn)
        callback.validate()
        if (_timestamp_column is None) != (_timestamp_type is None):
            raise ValueError("default_timestamp_column and default_timestamp_type must be supplied together")
        if _timestamp_column is not None and not _timestamp_column.strip():
            raise ValueError("default_timestamp_column must not be empty")
        if _output_format is not None and (
            _output_format not in REGISTERABLE_OUTPUT_FORMATS
            or (_output_format is FileOutputFormat.MANIFEST) != self._is_manifest
        ):
            raise ValueError("output_format must be a supported format matching the extractor decorator")
        self._definition = _Definition(
            callback.__wrapped__,
            callback.arguments,
            MappingProxyType(dict(callback.errors)),
            FileOutputFormat.MANIFEST if self._is_manifest else _output_format,
            _timestamp_column,
            _timestamp_type,
        )
        functools.update_wrapper(self, callback.__wrapped__)

    def __call__(self, ctx: _CtxT) -> None:
        """Bind declared arguments and invoke the callback without finalizing or reporting errors.

        Prefer :meth:`run` for container execution and complete local tests.
        """
        arguments = {}
        for argument in self._definition.arguments:
            logger.debug("binding argument %s from %s", argument.argument, argument.spec.environment_variable)
            arguments[argument.argument] = argument.resolve(ctx)
        logger.debug("invoking callback %s", self._definition.callback.__name__)
        self._definition.callback(ctx, **arguments)

    def registration_kwargs(self) -> _RegistrationKwargs:
        """Build image registration arguments exclusively from decorator declarations.

        Pass the result as ``**entrypoint.registration_kwargs()`` to
        ``ContainerizedExtractor.register_image(tarball, tag=..., ...)``. Returns fresh lists of
        FileExtractionInput and FileExtractionParameter objects, the output format, and the two
        default timestamp settings. List order follows the decorators from top to bottom.

        Declare ``default_timestamp_column`` and ``default_timestamp_type`` on the outer
        decorator. Manifest output format is automatic; single-file extractors also need an
        explicit ``output_format``. Missing required registration settings raise ValueError.
        Timestamp settings describe image defaults; they do not populate local job metadata.

        Does not execute the callback or converters, inspect its body, read the environment,
        upload an image, or activate it. Parameter types/defaults and error mappings remain
        runtime-only. Legacy lookups add no metadata: no argument declarations yields empty
        inputs/parameters. Keep complete manual registration metadata for partially migrated
        extractors with additional undeclared dependencies.
        """
        logger.debug("exporting registration metadata for %s", self._definition.callback.__name__)
        return _registration_kwargs(self._definition)

    def catalog_manifest(self, *, id: str, version: str, display_name: str, description: str) -> dict[str, Any]:
        """Export a first-party catalog manifest from declarations and release identity.

        Returns fresh JSON/YAML-serializable data for ``extractor.yaml``, including error
        fallbacks. Does not execute extraction, read the environment, build an image, write
        a file, or publish a release. The publisher owns versioning and artifact identity.

        Catalog export requires at least one input with suffix filters, uppercase environment
        variables, epoch or ISO 8601 timestamp defaults, and a ``message`` on each error
        declaration. Error codes must be non-reserved catalog identifiers. Identical error
        fallbacks sharing an exit code are combined; conflicting fallbacks are rejected.
        Input display metadata, converters and defaults have no catalog schema fields.
        Incomplete or unrepresentable metadata raises ValueError before publication.
        """
        logger.debug("exporting catalog metadata for %s version %s", id, version)
        return _catalog_manifest(
            self._definition, id=id, version=version, display_name=display_name, description=description
        )

    @property
    def _is_manifest(self) -> bool:
        return issubclass(self._context_cls, ManifestExtractorContext)

    def run(
        self,
        *,
        env: Mapping[str, str] | None = None,
        exit: bool = True,
        termination_log_path: str | Path = _DEFAULT_TERMINATION_LOG_PATH,
    ) -> _CtxT:
        """Run the extractor against the environment and finalize its outputs.

        Intended as the container entrypoint (``if __name__ == "__main__": my_extractor.run()``).
        Builds a context, resolves all declared arguments, invokes the callback, and finalizes
        its outputs. ``env`` replaces the process environment when supplied; required values
        must be present there, while optional parameters use their declared defaults. Output
        directories must already exist. Binding failures prevent callback execution and use
        the same error handling as startup, extraction, and finalization failures.

        On success returns the context. With ``exit=True`` (the default), mapped exceptions
        write bounded structured JSON to the termination log and stderr, then print the full
        traceback to stderr before exiting with their mapped status. The closest mapped class in the
        exception's MRO wins. Other failures
        print a traceback and exit 1. Pass ``exit=False`` to re-raise the original exception
        without reporting -- useful in tests. ``termination_log_path`` is a trusted explicit
        output path (default ``/dev/termination-log``), never read from the environment. Use a
        temporary path when testing mapped exits locally.
        """
        environ = os.environ if env is None else env
        if env is None:
            # As the container entrypoint, make the runtime's log lines visible in the job's
            # captured output; a no-op when the author already configured logging.
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        try:
            logger.debug("checking registered output format and building context")
            self._check_registered_format(environ)
            ctx = self._build_context(environ)
            logger.info(
                "running %s extractor %s with %d input(s)",
                "manifest" if self._is_manifest else "single-file",
                self._definition.callback.__name__,
                len(ctx._resolve_inputs()),
            )
            self._validate_registered_contract(ctx)
            self(ctx)
            logger.debug("finalizing outputs")
            count = ctx._finalize()
            logger.info("extractor %s completed with %d output(s)", self._definition.callback.__name__, count)
            return ctx
        except BaseException as error:  # deliberately broad: any failure (incl. SystemExit/KeyboardInterrupt
            # from user code) must fail the ingest job cleanly, not just Exception subclasses.
            if exit:
                if (mapping := _resolve_error(self._definition.errors, error)) is not None:
                    logger.debug("mapped %s to code %s, exit %d", type(error).__name__, mapping.code, mapping.exit_code)
                    mapping.report(error, termination_log_path)
                    traceback.print_exc()
                    sys.exit(mapping.exit_code)
                logger.debug("unmapped %s; exiting with status 1", type(error).__name__)
                traceback.print_exc()
                sys.exit(1)
            logger.debug("propagating %s because exit=False", type(error).__name__)
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
        if (
            not self._is_manifest
            and self._definition.output_format is not None
            and registered != self._definition.output_format.value
        ):
            raise ExtractorError(
                f"declared output_format {self._definition.output_format.value!r} "
                f"disagrees with registered format {registered!r}"
            )
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
        ctx = self._context_cls(
            output_dir=Path(output_dir),
            _env=env,
            _input_dir=Path(input_dir),
            _input_specs=_parse_input_specs(env),
            _param_specs=_parse_param_specs(env),
        )
        logger.debug(
            "context lookup sources: inputs=%s, parameters=%s",
            "environment" if ctx._input_specs is None else "registered metadata",
            "environment" if ctx._param_specs is None else "registered metadata",
        )
        return ctx
