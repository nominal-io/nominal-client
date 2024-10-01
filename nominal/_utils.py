from __future__ import annotations

import logging
import mimetypes
import os
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Callable, Iterable, Iterator, Literal, NamedTuple, TypeVar

from typing_extensions import ParamSpec

from nominal._api.combined import (
    api,
    scout_api,
    scout_checks_api,
    scout_compute_api,
    scout_compute_representation_api,
    scout_run_api,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


def construct_user_agent_string() -> str:
    """Constructs a user-agent string with system & Python metadata.
    E.g.: nominal-python/1.0.0b0 (macOS-14.4-arm64-arm-64bit) cpython/3.12.4
    """
    import importlib.metadata
    import platform
    import sys

    try:
        v = importlib.metadata.version("nominal")
        p = platform.platform()
        impl = sys.implementation
        py = platform.python_version()
        return f"nominal-python/{v} ({p}) {impl.name}/{py}"
    except Exception as e:
        # I believe all of the above are cross-platform, but just in-case...
        logger.error("failed to construct user-agent string", exc_info=e)
        return "nominal-python/unknown"


def update_dataclass(self: T, other: T, fields: Iterable[str]) -> None:
    """Update dataclass attributes, copying from `other` into `self`.

    Uses __dict__ to update `self` to update frozen dataclasses.
    """
    for field in fields:
        self.__dict__[field] = getattr(other, field)


class FileType(NamedTuple):
    extension: str
    mimetype: str

    @classmethod
    def from_path(cls, path: Path | str, default_mimetype: str = "application/octect-stream") -> FileType:
        ext = "".join(Path(path).suffixes)
        mimetype, _encoding = mimetypes.guess_type(path)
        if mimetype is None:
            return cls(ext, default_mimetype)
        return cls(ext, mimetype)

    @classmethod
    def from_path_dataset(cls, path: Path | str) -> FileType:
        path_string = str(path) if isinstance(path, Path) else path
        if path_string.endswith(".csv"):
            return FileTypes.CSV
        if path_string.endswith(".csv.gz"):
            return FileTypes.CSV_GZ
        if path_string.endswith(".parquet"):
            return FileTypes.PARQUET
        raise ValueError(f"dataset path '{path}' must end in .csv, .csv.gz, or .parquet")


class FileTypes:
    CSV: FileType = FileType(".csv", "text/csv")
    CSV_GZ: FileType = FileType(".csv.gz", "text/csv")
    # https://issues.apache.org/jira/browse/PARQUET-1889
    PARQUET: FileType = FileType(".parquet", "application/vnd.apache.parquet")
    MP4: FileType = FileType(".mp4", "video/mp4")
    BINARY: FileType = FileType("", "application/octet-stream")


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


Priority = Literal["P0", "P1", "P2", "P3", "P4"]


def _priority_to_conjure_priority(priority: Priority) -> scout_checks_api.Priority:
    if priority == "P0":
        return scout_checks_api.Priority.P0
    elif priority == "P1":
        return scout_checks_api.Priority.P1
    elif priority == "P2":
        return scout_checks_api.Priority.P2
    elif priority == "P3":
        return scout_checks_api.Priority.P3
    elif priority == "P4":
        return scout_checks_api.Priority.P4
    else:
        raise ValueError(f"invalid priority: {priority}")


def _conjure_priority_to_priority(priority: scout_checks_api.Priority) -> Priority:
    if priority == scout_checks_api.Priority.P0:
        return "P0"
    elif priority == scout_checks_api.Priority.P1:
        return "P1"
    elif priority == scout_checks_api.Priority.P2:
        return "P2"
    elif priority == scout_checks_api.Priority.P3:
        return "P3"
    elif priority == scout_checks_api.Priority.P4:
        return "P4"
    else:
        raise ValueError(f"invalid priority: {priority}")


def _compute_node_to_compiled_node(node: scout_compute_api.ComputeNode) -> scout_compute_representation_api.Node:
    class ComputeNodeVisitor(scout_compute_api.ComputeNodeVisitor):
        def _enum(self, enum: scout_compute_api.EnumSeriesNode) -> scout_compute_representation_api.Node:
            return scout_compute_representation_api.Node(enumerated_series=enum)

        def _numeric(self, numeric: scout_compute_api.NumericSeriesNode) -> scout_compute_representation_api.Node:
            return scout_compute_representation_api.Node(numeric_series=numeric)

        def _ranges(self, ranges: scout_compute_api.RangesNode) -> scout_compute_representation_api.Node:
            return scout_compute_representation_api.Node(range_series=ranges)

        def _raw(self, raw: scout_compute_api.RawUntypedSeriesNode) -> scout_compute_representation_api.Node:
            raise ValueError("Raw nodes are not yet supported by the client library")

    val: scout_compute_representation_api.Node = node.accept(visitor=ComputeNodeVisitor())
    return val


def _compiled_node_to_compute_node(node: scout_compute_representation_api.Node) -> scout_compute_api.ComputeNode:
    class NodeVisitor(scout_compute_representation_api.NodeVisitor):
        def _enumerated_series(
            self, enumerated_series: scout_compute_api.EnumSeriesNode
        ) -> scout_compute_api.ComputeNode:
            return scout_compute_api.ComputeNode(enum=enumerated_series)

        def _numeric_series(self, numeric_series: scout_compute_api.NumericSeriesNode) -> scout_compute_api.ComputeNode:
            return scout_compute_api.ComputeNode(numeric=numeric_series)

        def _range_series(self, range_series: scout_compute_api.RangesNode) -> scout_compute_api.ComputeNode:
            return scout_compute_api.ComputeNode(ranges=range_series)

    val: scout_compute_api.ComputeNode = node.accept(visitor=NodeVisitor())
    return val


def _conjure_checklist_variable_to_name_graph__pair(
    checklist_variable: scout_checks_api.ChecklistVariable,
) -> tuple[str, scout_compute_representation_api.CompiledNode]:
    if checklist_variable.value.compute_node is None:
        raise ValueError("checklist variable is not a compute node")
    preprocessed = {
        key: _variable_locator_to_representation_variable(value)
        for key, value in checklist_variable.value.compute_node.context.variables.items()
    }

    compute_graph = scout_compute_representation_api.CompiledNode(
        node=_compute_node_to_compiled_node(checklist_variable.value.compute_node.series_node),
        context=scout_compute_representation_api.ComputeRepresentationContext(
            variables={key: value for key, value in preprocessed.items() if value is not None},
            function_variables={},
        ),
    )

    return checklist_variable.name, compute_graph


def _conjure_check_to_check_definition_graph_pair(
    conjure_check: scout_checks_api.ChecklistEntry,
) -> tuple[scout_checks_api.Check, scout_compute_representation_api.CompiledNode]:
    if conjure_check.type != "check" or conjure_check.check is None:
        raise ValueError("checklist entry is not a check")

    check_definition: scout_checks_api.Check = conjure_check.check
    if check_definition.condition is None:
        raise ValueError("check does not have a condition")

    check_condition: scout_checks_api.CheckCondition = check_definition.condition
    if check_condition.num_ranges_v3 is None:
        raise ValueError("check condition does not evaluate to a valid set of ranges")

    preprocessed = {
        key: _variable_locator_to_representation_variable(value)
        for key, value in check_condition.num_ranges_v3.variables.items()
    }

    compute_graph = scout_compute_representation_api.CompiledNode(
        node=scout_compute_representation_api.Node(range_series=check_condition.num_ranges_v3.ranges),
        context=scout_compute_representation_api.ComputeRepresentationContext(
            variables={key: value for key, value in preprocessed.items() if value is not None},
            function_variables={},
        ),
    )

    return check_definition, compute_graph


def _representation_variable_to_unresolved_variable_locator(
    variable: scout_compute_representation_api.ComputeRepresentationVariableValue,
) -> scout_checks_api.UnresolvedVariableLocator:
    class VariableValueVisitor(scout_compute_representation_api.ComputeRepresentationVariableValueVisitor):
        def _double(self, _double: float) -> scout_checks_api.UnresolvedVariableLocator:
            raise ValueError("double variables are not yet supported by the client library")

        def _duration(self, _duration: scout_run_api.Duration) -> scout_checks_api.UnresolvedVariableLocator:
            raise ValueError("Duration variables are not yet supported by the client library")

        def _integer(self, _integer: int) -> scout_checks_api.UnresolvedVariableLocator:
            raise ValueError("integer variables are not yet supported by the client library")

        def _string_set(self, _string_set: list[str]) -> scout_checks_api.UnresolvedVariableLocator:
            raise ValueError("string set variables are not yet supported by the client library")

        def _timestamp(self, _timestamp: api.Timestamp) -> scout_checks_api.UnresolvedVariableLocator:
            raise ValueError("timestamp variables are not yet supported by the client library")

        def _function_rid(self, function_rid: str) -> scout_checks_api.UnresolvedVariableLocator:
            raise ValueError("functions are not yet supported by the client library")

        def _series(
            self, series: scout_compute_representation_api.ChannelLocator
        ) -> scout_checks_api.UnresolvedVariableLocator:
            return scout_checks_api.UnresolvedVariableLocator(
                series=scout_api.ChannelLocator(channel=series.channel, data_source_ref=series.data_source_ref, tags={})
            )

        def _external_variable_reference(
            self, external_variable_reference: str
        ) -> scout_checks_api.UnresolvedVariableLocator:
            return scout_checks_api.UnresolvedVariableLocator(checklist_variable=external_variable_reference)

    var: scout_checks_api.UnresolvedVariableLocator = variable.accept(visitor=VariableValueVisitor())
    return var


def _variable_locator_to_representation_variable(
    variable: scout_checks_api.VariableLocator,
) -> scout_compute_representation_api.ComputeRepresentationVariableValue | None:
    class VariableLocatorVisitor(scout_checks_api.VariableLocatorVisitor):
        def _series(
            self, series: scout_api.ChannelLocator
        ) -> scout_compute_representation_api.ComputeRepresentationVariableValue | None:
            return scout_compute_representation_api.ComputeRepresentationVariableValue(
                series=scout_compute_representation_api.ChannelLocator(
                    channel=series.channel, data_source_ref=series.data_source_ref
                )
            )

        def _checklist_variable(
            self, checklist_variable: str
        ) -> scout_compute_representation_api.ComputeRepresentationVariableValue | None:
            return scout_compute_representation_api.ComputeRepresentationVariableValue(
                external_variable_reference=checklist_variable
            )

        def _compute_node(
            self, compute_node: scout_checks_api.ComputeNodeWithContext
        ) -> scout_compute_representation_api.ComputeRepresentationVariableValue | None:
            return None

        def _function_rid(
            self, function_rid: str
        ) -> scout_compute_representation_api.ComputeRepresentationVariableValue | None:
            return None

        def _timestamp(
            self, timestamp: scout_checks_api.TimestampLocator
        ) -> scout_compute_representation_api.ComputeRepresentationVariableValue | None:
            return None

    val: scout_compute_representation_api.ComputeRepresentationVariableValue | None = variable.accept(
        visitor=VariableLocatorVisitor()
    )
    return val


def _remove_newlines(s: str) -> str:
    return s.replace("\n", "")


Param = ParamSpec("Param")


def deprecate_keyword_argument(new_name: str, old_name: str) -> Callable[[Callable[Param, T]], Callable[Param, T]]:
    def _deprecate_keyword_argument_decorator(f: Callable[Param, T]) -> Callable[Param, T]:
        def wrapper(*args: Param.args, **kwargs: Param.kwargs) -> T:
            if old_name in kwargs:
                import warnings

                warnings.warn(
                    f"The '{old_name}' keyword argument is deprecated and will be removed in a future version, use '{new_name}' instead.",
                    UserWarning,
                    stacklevel=2,
                )
                kwargs[new_name] = kwargs.pop(old_name)
            return f(*args, **kwargs)

        return wrapper

    return _deprecate_keyword_argument_decorator
