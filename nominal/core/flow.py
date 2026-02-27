from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Mapping, Sequence, Type, Union

from typing_extensions import Self

from nominal.core._ingest_flow import IngestFlowClient
from nominal.core._stream.write_stream import DataStream
from nominal.ts import IntegralNanosecondsUTC

if TYPE_CHECKING:
    from nominal.core.client import NominalClient
    from nominal.core.dataset_file import DatasetFile
    from nominal.core.run import Run
    from nominal.ts import _AnyTimestampType


class FlowBuilderError(Exception):
    pass


@dataclass(frozen=True)
class SelectStep:
    title: str
    description: str
    chosen_option: str


@dataclass(frozen=True)
class FormStep:
    title: str
    description: str
    values: Mapping[str, str]


@dataclass(frozen=True)
class UploadStep:
    title: str
    description: str


FlowStep = Union[SelectStep, FormStep, UploadStep]


@dataclass(frozen=True)
class FlowResult:
    dataset_file: DatasetFile
    run: Run | None


@dataclass(frozen=True)
class Flow:
    """A resolved path through an IngestFlow graph.

    Represents a single linear path with all
    selections made and form values filled in. Use add_tabular_data()
    or get_write_stream() to ingest data through the flow.

    Example::

        flow = (
            FlowBuilder("ri.ingest-flow.main.flow.abc123", client=nominal_client)
            .select("CSV Upload")
            .select("Vehicle Telemetry")
            .fill({"Run Name": "flight-042", "Environment": "prod"})
            .build()
        )

        result = flow.add_tabular_data(
            "data.csv", timestamp_column="time", timestamp_type="iso_8601"
        )

        # Or stream point-by-point:
        with flow.get_write_stream() as stream:
            stream.enqueue("temperature", "2025-01-01T00:00:00Z", 72.5)
    """

    rid: str
    steps: tuple[FlowStep, ...]
    _client: NominalClient = field(repr=False)
    _run_labels: list[str] = field(repr=False)
    _run_properties: dict[str, str] = field(repr=False)
    _tags: dict[str, str] = field(repr=False)
    _dataset_rid: str | None = field(repr=False, default=None)
    _asset_rid: str | None = field(repr=False, default=None)
    _refname: str | None = field(repr=False, default=None)

    def _get_dataset(self):
        if self._dataset_rid is None:
            raise FlowBuilderError("No dataset resolved in flow path")
        return self._client.get_dataset(self._dataset_rid)

    @property
    def _has_run_metadata(self) -> bool:
        return bool(self._run_labels or self._run_properties)

    def add_tabular_data(
        self,
        path: Path | str,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        *,
        run_name: str | None = None,
    ) -> FlowResult:
        dataset = self._get_dataset()
        dataset_file = dataset.add_tabular_data(
            path,
            timestamp_column,
            timestamp_type,
            tags=self._tags or None,
        )

        run = None
        if self._has_run_metadata:
            run = self._client.create_run(
                name=run_name or Path(path).stem,
                start=datetime.now(),
                end=None,
                properties=self._run_properties or None,
                labels=self._run_labels,
                asset=self._asset_rid,
            )
            run.add_dataset(self._refname or dataset.name, dataset)

        return FlowResult(dataset_file=dataset_file, run=run)

    def get_write_stream(
        self,
        batch_size: int = 50_000,
        max_wait: timedelta = timedelta(seconds=1),
    ) -> FlowWriteStream:
        dataset = self._get_dataset()
        inner = dataset.get_write_stream(batch_size=batch_size, max_wait=max_wait)
        return FlowWriteStream(inner, tags=self._tags)


class FlowWriteStream:
    def __init__(self, inner: DataStream, tags: Mapping[str, str]) -> None:
        self._inner = inner
        self._tags = tags

    def __enter__(self) -> Self:
        self._inner.__enter__()
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self._inner.__exit__(exc_type, exc_value, traceback)

    def enqueue(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: str | float | int,
    ) -> None:
        self._inner.enqueue(channel_name, timestamp, value, tags=self._tags)

    def enqueue_batch(
        self,
        channel_name: str,
        timestamps: Sequence[str | datetime | IntegralNanosecondsUTC],
        values: Sequence[str | float | int],
    ) -> None:
        for ts, val in zip(timestamps, values):
            self._inner.enqueue(channel_name, ts, val, tags=self._tags)

    def enqueue_from_dict(
        self,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        channel_values: Mapping[str, str | float | int],
    ) -> None:
        for channel, value in channel_values.items():
            self._inner.enqueue(channel, timestamp, value, tags=self._tags)

    def close(self, wait: bool = True) -> None:
        self._inner.close(wait=wait)


def _normalize_label(label: str) -> str:
    return label.lower().replace("_", " ").strip()


class FlowBuilder:
    def __init__(self, rid: str, client: NominalClient) -> None:
        token = client._clients.auth_header.removeprefix("Bearer ")
        ingest_client = IngestFlowClient(token)
        response = ingest_client.get(rid)
        self._rid = rid
        self._client = client
        self._state = response.ingest_flow.state
        self._current_uuid: str = self._state.start_step_uuid
        self._steps: list[FlowStep] = []
        self._path_uuids: list[str] = [self._current_uuid]

        self._run_labels: list[str] = []
        self._run_properties: dict[str, str] = {}
        self._tags: dict[str, str] = {}
        self._dataset_rid: str | None = None
        self._asset_rid: str | None = None
        self._refname: str | None = None

        if self._current_uuid not in self._state.steps:
            raise FlowBuilderError(
                f"Start step '{self._current_uuid}' not found in flow graph"
            )

    @property
    def _current_step(self):
        return self._state.steps[self._current_uuid]

    def select(self, option: str) -> Self:
        step = self._current_step
        if not step.HasField("select"):
            raise FlowBuilderError(
                f"Current step '{step.title}' is a {_step_type_name(step)} step, not a select step"
            )

        for opt in step.select.options:
            if opt.title == option:
                self._accumulate_option_actions(opt)
                self._steps.append(SelectStep(
                    title=step.title,
                    description=step.description,
                    chosen_option=option,
                ))
                self._advance(opt.next_step_uuid)
                return self

        available = [o.title for o in step.select.options]
        raise FlowBuilderError(
            f"Option '{option}' not found in step '{step.title}'. "
            f"Available: {available}"
        )

    def fill(self, values: Mapping[str, str]) -> Self:
        step = self._current_step
        if not step.HasField("form"):
            raise FlowBuilderError(
                f"Current step '{step.title}' is a {_step_type_name(step)} step, not a form step"
            )

        normalized_input = {_normalize_label(k): v for k, v in values.items()}
        resolved_values: dict[str, str] = {}

        for form_field in step.form.fields:
            norm_label = _normalize_label(form_field.label)
            user_value = normalized_input.get(norm_label)

            if form_field.is_required and user_value is None:
                raise FlowBuilderError(
                    f"Required field '{form_field.label}' not provided in step '{step.title}'"
                )

            if user_value is not None:
                resolved_values[form_field.label] = user_value
                self._accumulate_form_field_action(form_field, user_value)

        self._steps.append(FormStep(
            title=step.title,
            description=step.description,
            values=resolved_values,
        ))
        self._advance(step.next_step_uuid)
        return self

    def build(self) -> Flow:
        step = self._current_step
        if not step.HasField("upload"):
            raise FlowBuilderError(
                f"Cannot build: current step '{step.title}' is a {_step_type_name(step)} step, "
                f"not an upload step. The path must end at an upload step."
            )

        self._steps.append(UploadStep(
            title=step.title,
            description=step.description,
        ))

        self._validate_path()

        return Flow(
            rid=self._rid,
            steps=tuple(self._steps),
            _client=self._client,
            _run_labels=self._run_labels,
            _run_properties=self._run_properties,
            _tags=self._tags,
            _dataset_rid=self._dataset_rid,
            _asset_rid=self._asset_rid,
            _refname=self._refname,
        )

    def _advance(self, next_uuid: str) -> None:
        if next_uuid not in self._state.steps:
            raise FlowBuilderError(
                f"Next step '{next_uuid}' not found in flow graph"
            )
        self._current_uuid = next_uuid
        self._path_uuids.append(next_uuid)

    def _validate_path(self) -> None:
        state = self._state

        if self._path_uuids[0] != state.start_step_uuid:
            raise FlowBuilderError(
                f"Path does not start at the flow's start step. "
                f"Expected '{state.start_step_uuid}', got '{self._path_uuids[0]}'"
            )

        for i, uuid in enumerate(self._path_uuids):
            if uuid not in state.steps:
                raise FlowBuilderError(f"Step UUID '{uuid}' not found in flow graph")

            step = state.steps[uuid]
            is_last = i == len(self._path_uuids) - 1

            if is_last:
                if not step.HasField("upload"):
                    raise FlowBuilderError(
                        f"Path does not end at an upload step. "
                        f"Final step '{step.title}' is a {_step_type_name(step)} step."
                    )
                continue

            next_uuid = self._path_uuids[i + 1]

            if step.HasField("select"):
                valid_nexts = {o.next_step_uuid for o in step.select.options}
                if next_uuid not in valid_nexts:
                    raise FlowBuilderError(
                        f"Invalid edge: no option in select step '{step.title}' "
                        f"leads to step '{next_uuid}'"
                    )
            elif step.HasField("form"):
                if step.next_step_uuid != next_uuid:
                    raise FlowBuilderError(
                        f"Invalid edge: form step '{step.title}' leads to "
                        f"'{step.next_step_uuid}', but path recorded '{next_uuid}'"
                    )
            elif step.HasField("upload"):
                raise FlowBuilderError(
                    f"Upload step '{step.title}' must be terminal but has subsequent steps"
                )

    def _accumulate_option_actions(self, option) -> None:
        for action in option.actions:
            if action.HasField("add_run_label"):
                self._run_labels.append(action.add_run_label.label)
            elif action.HasField("add_run_property"):
                self._run_properties[action.add_run_property.key] = action.add_run_property.value
            elif action.HasField("add_tag"):
                self._tags[action.add_tag.key] = action.add_tag.value
            elif action.HasField("use_dataset"):
                self._dataset_rid = action.use_dataset.dataset_rid
            elif action.HasField("use_asset"):
                self._asset_rid = action.use_asset.asset_rid
            elif action.HasField("use_refname"):
                self._refname = action.use_refname.refname

    def _accumulate_form_field_action(self, form_field, value: str) -> None:
        if form_field.HasField("set_run_property"):
            self._run_properties[form_field.set_run_property.key] = value
        elif form_field.HasField("set_tag"):
            self._tags[form_field.set_tag.key] = value
        elif form_field.HasField("set_run_label"):
            self._run_labels.append(value)


def _step_type_name(step) -> str:
    for name in ("select", "form", "upload"):
        if step.HasField(name):
            return name
    return "unknown"
