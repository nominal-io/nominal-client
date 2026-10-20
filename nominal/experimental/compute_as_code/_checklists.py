"""Author reusable checklist failure conditions with compute source."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Iterable, Literal, Mapping, Self, Sequence
from uuid import UUID, uuid4, uuid5

from conjure_python_client import ConjureDecoder
from nominal_api import event, scout_api, scout_checks_api, scout_compute_api

import nominal.compute as nc
from nominal.compute.compiler import compile_expression
from nominal.core import Checklist, EventType, NominalClient, Priority, User, Workspace
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.experimental.checklist_utils.checklist_utils import _create_checklist_with_content

# Permanent namespace: changing this would change identities derived from published keys.
_CHECK_KEY_NAMESPACE = UUID("52e0d79d-a67f-4d15-8cb6-a294f8c64b4e")


@dataclass(frozen=True, kw_only=True)
class CheckDefinition:
    """A reusable failure condition written in the frontend compute language.

    Bind channels when adding this definition to a `ChecklistBuilder`. Use
    ``dataclasses.replace`` or ordinary Python functions to create variations.

    Args:
        title: Display name of the check.
        expression: Source ending in a range expression describing violations. Assignments
            are allowed; omit imports. Convert boolean conditions with ``.ranges()``.
        priority: Severity of violations, from P0 (most severe) to P4.
        event_type: Category of generated events. Omit to use the service default.
            This does not change the condition or turn absence of matching ranges into success.
            ``EventType.UNKNOWN`` is not a valid category for creation.
        event_labels: Labels applied to generated events, not checklist grouping labels.
            Omit to use the service default; an empty sequence requests no labels.
            Labels are copied when the definition is added to a builder.
        description: Description of the check.
        key: Optional stable identity, independent of title and source. The same key preserves
            lineage across revisions. Omit to generate an identity each time the check is added.
            Keys are case-sensitive, are not trimmed, and must not be blank or repeated in a builder.
    """

    title: str = field(kw_only=False)
    expression: str = field(kw_only=False)
    priority: Priority
    description: str = ""
    key: str | None = None
    event_type: EventType | None = None
    event_labels: Sequence[str] | None = None


def _expression_references(node: object) -> set[str]:
    """Collect named inputs from the serialized nominal-compute expression graph."""
    children: Iterable[object]
    match node:
        case (
            {"type": "raw", "raw": {"name": str(name)}}
            | {"type": "reference", "reference": {"name": str(name)}}
            | {"type": "variable", "variable": str(name)}
        ):
            return {name}
        case dict():
            children = node.values()
        case list():
            children = node
        case _:
            return set()
    references: set[str] = set()
    for child in children:
        references.update(_expression_references(child))
    return references


@dataclass(frozen=True)
class ChannelBinding:
    """A channel resolved relative to the run or asset where a checklist executes.

    Args:
        data_scope: Data source reference name on the run or data scope name on the asset.
        channel: Channel name within that scope.
        tags: Tag filters selecting a series within the channel. Defaults to no additional filter.
        series_type: Type supplied to the source compiler and frontend editor. Defaults to numeric.

    Note:
        The builder copies tags when adding a check. No channel lookup is performed locally;
        the service resolves the scope, channel, and tags when evaluating the checklist.
    """

    data_scope: str
    channel: str
    tags: Mapping[str, str] = field(default_factory=dict)
    series_type: Literal["numeric", "categorical"] = "numeric"

    def _to_conjure(self) -> scout_checks_api.UnresolvedVariableLocator:
        return scout_checks_api.UnresolvedVariableLocator(
            series=scout_api.ChannelLocator(
                data_source_ref=self.data_scope,
                channel=self.channel,
                tags=dict(self.tags),
            )
        )


class ChecklistBuilder:
    """Collect source checks and publish complete checklist revisions.

    Adding checks is local. Source and bindings are captured when added.
    ``create`` publishes a new checklist; ``commit`` replaces an existing checklist's definition.
    Source strings are compiled with nominal-compute and retained for the frontend code editor.

    Args:
        strict: Reject unused input bindings when True. Missing bindings always raise.

    Example:
        ```python
        check = CheckDefinition(
            "Overheating", "(temperature > 100).ranges()", priority=Priority.P1,
        )
        builder = ChecklistBuilder()
        builder.add(
            check, inputs={"temperature": ChannelBinding("telemetry", "engine.temperature")},
        )
        checklist = builder.create(client, "Engine acceptance")
        ```
    """

    def __init__(self, *, strict: bool = False) -> None:
        self._strict = strict
        self._checks: list[scout_checks_api.CreateCheckRequest] = []
        self._variables: list[scout_checks_api.UnresolvedChecklistVariable] = []

    def add(self, check: CheckDefinition, *, inputs: Mapping[str, ChannelBinding]) -> Self:
        """Compile and bind a reusable check, capturing its source and channels locally.

        Args:
            check: Failure condition and metadata. Source must compile to a range series.
            inputs: Map expression names to channels. Missing bindings always fail. Unused
                bindings are omitted, or rejected when the builder has ``strict=True``.

        Returns:
            This builder. A failed addition leaves it unchanged.

        Raises:
            TypeError: The expression is not a source string or does not produce a range series.
            ValueError: Compilation fails, bindings are missing, strict bindings are unused,
                a stable key is blank or duplicated, or the event type is UNKNOWN.

        Note:
            Compilation does not resolve live channels or establish streaming compatibility.
            Alignment, fill, and missing-data behavior follow service defaults. A check's
            ranges describe violations; absence of data does not establish a passing result.
        """
        if check.event_type is EventType.UNKNOWN:
            raise ValueError("EventType.UNKNOWN cannot be used for generated events")
        compiler_references = {
            name: nc.CategoricalSeries.Reference(name)
            if binding.series_type == "categorical"
            else nc.NumericSeries.Reference(name)
            for name, binding in inputs.items()
        }
        compiled = compile_expression(check.expression, references=compiler_references)
        if not isinstance(compiled.value, nc.RangeSeries):
            raise TypeError("Check source must produce a RangeSeries; convert booleans with .ranges()")
        key = check.key
        if key is not None and not key.strip():
            raise ValueError("A stable key must be nonempty")
        check_id = uuid4() if key is None else uuid5(_CHECK_KEY_NAMESPACE, key)
        if any(existing.check_lineage_rid == str(check_id) for existing in self._checks):
            raise ValueError(f"Duplicate key: {key!r}")
        wire = json.loads(compiled.value.to_json())  # type: ignore[attr-defined]
        # Explicit Reference constructors can introduce names absent from the compiler environment.
        graph_references = _expression_references(wire)
        references = graph_references | set(compiled.used_reference_keys)
        ranges: scout_compute_api.RangeSeries = ConjureDecoder.do_decode(wire, scout_compute_api.RangeSeries)
        missing = graph_references - inputs.keys()
        unused = inputs.keys() - references if self._strict else set()
        errors = []
        if missing:
            errors.append(f"missing input bindings: {', '.join(sorted(missing))}")
        if unused:
            errors.append(f"unused input bindings: {', '.join(sorted(unused))}")
        if errors:
            raise ValueError(f"Check {check.title!r}: {'; '.join(errors)}")
        checklist_variables = _source_variables(
            len(self._variables), check.title, check.expression, ranges, inputs, references
        )
        variables = {
            "failure": scout_checks_api.UnresolvedVariableLocator(checklist_variable=checklist_variables[-1].name)
        }
        failure_reference = scout_compute_api.RangeSeries(raw=scout_compute_api.Reference(name="failure"))
        self._checks.append(
            scout_checks_api.CreateCheckRequest(
                title=check.title,
                description=check.description,
                priority=scout_api.Priority(check.priority.name),
                check_lineage_rid=str(check_id),
                generated_event_type=None if check.event_type is None else event.EventType(check.event_type.value),
                generated_event_labels=None if check.event_labels is None else list(check.event_labels),
                condition=scout_checks_api.UnresolvedCheckCondition(
                    num_ranges_v3=scout_checks_api.UnresolvedNumRangesConditionV3(
                        ranges=failure_reference,
                        variables=variables,
                        function_spec={},
                        threshold=0,
                        operator=scout_compute_api.ThresholdOperator.GREATER_THAN,
                    )
                ),
            )
        )
        self._variables.extend(checklist_variables)
        return self

    def create(
        self,
        client: NominalClient,
        name: str,
        *,
        description: str = "",
        assignee: User | str | None = None,
        commit_message: str = "",
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        workspace: Workspace | str | None = None,
    ) -> Checklist:
        """Create and publish a new checklist without executing it.

        Args:
            client: Client used for creation.
            name: Display name of the checklist.
            description: Description of the checklist.
            assignee: User or user RID assigned to the checklist. Defaults to the authenticated user.
            commit_message: Message on the initial checklist commit.
            labels: Labels applied to the checklist.
            properties: String properties applied to the checklist.
            workspace: Workspace or workspace RID for creation. Defaults to the client's default workspace.

        Returns:
            The published checklist. Use its ``execute`` method to initiate a data review.

        Raises:
            ValueError: If no checks have been added.

        Note:
            This creates a new resource on every call; it does not update an existing checklist
            or consume the builder. Reuse the returned checklist with ``commit`` for later revisions.
        """
        if not self._checks:
            raise ValueError("A checklist needs at least one check")
        return _create_checklist_with_content(
            client,
            name,
            description=description,
            assignee_rid=None if assignee is None else rid_from_instance_or_string(assignee),
            commit_message=commit_message,
            checks=[scout_checks_api.CreateChecklistEntryRequest(create_check=check) for check in self._checks],
            checklist_variables=list(self._variables),
            labels=list(labels),
            properties=properties,
            is_published=True,
            workspace=workspace,
        )

    def commit(self, checklist: Checklist, *, commit_message: str) -> Checklist:
        """Replace a published checklist's definition with a new commit on main.

        Args:
            checklist: Loaded checklist to update, supplying the expected main commit.
            commit_message: Description of this revision.

        Returns:
            The same checklist, refreshed from the response with its new ``commit_id``.
            Save the previous ID before committing if you need to execute the earlier version.

        Raises:
            ValueError: If no checks have been added or the checklist has no loaded commit ID.
            conjure_python_client.ConjureHTTPError: If someone else committed to the checklist since
                it was loaded. The checklist object is not refreshed in that case.

        Note:
            This replaces all checks and checklist variables, including content authored in the UI.
            Omitted checks disappear from the new version; earlier versions remain available.
            Reusing a stable key preserves check lineage through renames and expression changes.
            Metadata and streaming settings are not changed by this request.

            The service rejects stale writes. This method does not refresh or retry a conflict;
            reconcile your definition with the latest checklist before trying again.
            Every successful call creates a commit, even if the definition is unchanged.

            Committing to main can update running streaming evaluations asynchronously. Returning
            confirms the commit, not adoption by every asset. No extra reload is issued by the SDK.
        """
        if not self._checks:
            raise ValueError("A checklist needs at least one check")
        entries = [scout_checks_api.UpdateChecklistEntryRequest(create_check=check) for check in self._checks]
        if checklist.commit_id is None:
            raise ValueError("Call checklist.refresh() to load its commit_id before committing")
        clients = checklist._clients
        response = clients.checklist.commit(
            clients.auth_header,
            checklist.rid,
            scout_checks_api.CommitChecklistRequest(
                checks=entries,
                checklist_variables=list(self._variables),
                commit_message=commit_message,
                latest_commit=checklist.commit_id,
            ),
        )
        return checklist._refresh_from_api(response)


def _variable_name(index: int) -> str:
    """Name a zero-based variable like Galaxy: a, ..., z, aa, ..., az, ba, ... ."""
    name = ""
    while index >= 0:
        index, remainder = divmod(index, 26)
        name = chr(ord("a") + remainder) + name
        index -= 1
    return name


def _source_variables(
    start_index: int,
    name: str,
    source: str,
    ranges: scout_compute_api.RangeSeries,
    inputs: Mapping[str, ChannelBinding],
    references: set[str],
) -> list[scout_checks_api.UnresolvedChecklistVariable]:
    """Give the code editor typed inputs and preserve source alongside its compiled graph."""
    variables = []
    context = {}
    for index, reference in enumerate(sorted(references)):
        binding = inputs[reference]
        variable_name = _variable_name(start_index + index)
        reference_node = scout_compute_api.Reference(name="raw")
        node = (
            scout_compute_api.ComputeNode(enum=scout_compute_api.EnumSeries(raw=reference_node))
            if binding.series_type == "categorical"
            else scout_compute_api.ComputeNode(numeric=scout_compute_api.NumericSeries(raw=reference_node))
        )
        variables.append(
            scout_checks_api.UnresolvedChecklistVariable(
                name=variable_name,
                display_name=f"{name}: {reference}",
                value=scout_checks_api.UnresolvedVariableLocator(
                    compute_node=scout_checks_api.UnresolvedComputeNodeWithContext(
                        context=scout_checks_api.UnresolvedVariables(variables={"raw": binding._to_conjure()}),
                        series_node=node,
                    )
                ),
            )
        )
        context[reference] = scout_checks_api.UnresolvedVariableLocator(checklist_variable=variable_name)
    variables.append(
        scout_checks_api.UnresolvedChecklistVariable(
            name=_variable_name(start_index + len(variables)),
            display_name=name,
            compute_expression=scout_checks_api.ComputeExpression(
                v1=scout_checks_api.ComputeExpressionV1(
                    python=scout_checks_api.ComputeExpressionV1Python(expression=source)
                )
            ),
            value=scout_checks_api.UnresolvedVariableLocator(
                compute_node=scout_checks_api.UnresolvedComputeNodeWithContext(
                    context=scout_checks_api.UnresolvedVariables(variables=context),
                    series_node=scout_compute_api.ComputeNode(ranges=ranges),
                )
            ),
        )
    )
    return variables
