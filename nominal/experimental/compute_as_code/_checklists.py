"""Author checklist failure conditions with compute expression objects."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Self, Sequence
from uuid import UUID

from conjure_python_client import ConjureDecoder
from nominal_api import scout_api, scout_checks_api, scout_compute_api

import nominal.compute as nc
from nominal.core import Checklist, NominalClient, Priority, User, Workspace
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core._utils.api_types import NominalProperties
from nominal.experimental.checklist_utils.checklist_utils import _create_checklist_with_content


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

    Note:
        The builder copies tags when adding a check. No channel lookup is performed locally;
        the service resolves the scope, channel, and tags when evaluating the checklist.
    """

    data_scope: str
    channel: str
    tags: Mapping[str, str] = field(default_factory=dict)

    def _to_conjure(self) -> scout_checks_api.UnresolvedVariableLocator:
        return scout_checks_api.UnresolvedVariableLocator(
            series=scout_api.ChannelLocator(
                data_source_ref=self.data_scope,
                channel=self.channel,
                tags=dict(self.tags),
            )
        )


class ChecklistBuilder:
    """Collect compute failure conditions to create or commit a published checklist.

    Construct directly with ``ChecklistBuilder()``. Adding checks is local and returns
    the same builder for chaining. Only `create` and `commit` make network requests. Expression graphs
    and channel bindings are captured when each check is added; Python source is not stored.

    Args:
        strict: Reject unused input bindings when True. Defaults to False so checks
            can share a binding map. Missing bindings always raise when adding a check.

    Example:
        ```python
        import nominal.compute as nc
        from nominal.core import Priority
        from nominal.experimental.compute_as_code import ChannelBinding, ChecklistBuilder

        temperature = nc.NumericSeries.Reference("temperature")
        checklist = (
            ChecklistBuilder()
            .add_check(
                "Sustained overheating",
                (temperature > 100).ranges(min_duration=nc.Duration.Seconds(2)),
                inputs={"temperature": ChannelBinding("telemetry", "engine.temperature")},
                priority=Priority.P1,
            )
            .create(client, "Engine acceptance")
        )
        review = checklist.execute(run).poll_for_completion()
        ```
    """

    def __init__(self, *, strict: bool = False) -> None:
        self._strict = strict
        self._checks: list[scout_checks_api.CreateCheckRequest] = []

    def add_check(
        self,
        name: str,
        failure_ranges: nc.RangeSeries,
        *,
        inputs: Mapping[str, ChannelBinding],
        priority: Priority,
        description: str = "",
        check_id: UUID | None = None,
    ) -> Self:
        """Add a check whose violations are the ranges produced by an expression.

        Args:
            name: Display name of the check.
            failure_ranges: Compute range expression describing violations, not passing intervals.
                Convert boolean expressions with ``.ranges()`` before passing them here.
            inputs: Map expression reference names to channels in the execution context.
                Unused bindings are omitted from the check, or rejected with ``strict=True``.
            priority: Severity of violations, from P0 (most severe) to P4.
            description: Description of the check.
            check_id: Persistent lineage UUID, distinct from a check version RID. Generate once
                and keep in source across updates. Optional for creation; required by ``commit``.

        Returns:
            This builder.

        Raises:
            TypeError: If ``failure_ranges`` is not a compute ``RangeSeries``.
            ValueError: If an expression reference has no input binding, or if an input
                is unused and the builder has ``strict=True``, or a check ID is duplicated.
                No check is added.

        Note:
            Alignment, fill, and missing-data behavior follow the service's checklist defaults.
            The SDK does not interpret absence of data as a passing check or validate references
            against live channels. Persistence and other range operations belong in the expression.
        """
        if not isinstance(failure_ranges, nc.RangeSeries):
            raise TypeError("failure_ranges must be a nominal.compute.RangeSeries; convert booleans with .ranges()")
        if check_id is not None and any(check.check_lineage_rid == str(check_id) for check in self._checks):
            raise ValueError(f"Duplicate check_id: {check_id}")
        wire = json.loads(failure_ranges.to_json())  # type: ignore[attr-defined]
        references = _expression_references(wire)
        ranges: scout_compute_api.RangeSeries = ConjureDecoder.do_decode(wire, scout_compute_api.RangeSeries)
        missing = references - inputs.keys()
        unused = inputs.keys() - references if self._strict else set()
        errors = []
        if missing:
            errors.append(f"missing input bindings: {', '.join(sorted(missing))}")
        if unused:
            errors.append(f"unused input bindings: {', '.join(sorted(unused))}")
        if errors:
            raise ValueError(f"Check {name!r}: {'; '.join(errors)}")
        variables = scout_checks_api.UnresolvedVariables(
            variables={reference: inputs[reference]._to_conjure() for reference in sorted(references)}
        )
        self._checks.append(
            scout_checks_api.CreateCheckRequest(
                title=name,
                description=description,
                priority=scout_api.Priority(priority.name),
                check_lineage_rid=None if check_id is None else str(check_id),
                condition=scout_checks_api.UnresolvedCheckCondition(
                    parameterized_num_ranges_v1=scout_checks_api.UnresolvedParameterizedNumRangesConditionV1(
                        ranges=ranges,
                        implementations=[variables],
                    )
                ),
            )
        )
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
        properties: NominalProperties | None = None,
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
            properties: Properties applied to the checklist.
            workspace: Workspace or workspace RID for creation. Defaults to the client's default workspace.

        Returns:
            The published checklist. Use its ``execute`` method to initiate a data review.

        Raises:
            ValueError: If no checks have been added.

        Note:
            This creates a new resource on every call; it does not update an existing checklist
            or consume the builder. The check list is retained for subsequent creations.
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
            labels=list(labels),
            properties=properties,
            is_published=True,
            workspace=workspace,
        )

    def commit(self, checklist: Checklist, *, commit_message: str) -> Checklist:
        """Replace a published checklist's definition with a new commit on main.

        Args:
            checklist: Loaded checklist whose ``commit_id`` is the expected current version.
            commit_message: Description of this revision.

        Returns:
            The same checklist, refreshed from the response with its new ``commit_id``.
            Save the previous ID before committing if you need to execute the earlier version.

        Raises:
            ValueError: If the builder is empty, any check lacks an explicit ``check_id``,
                or the checklist has no loaded commit ID.

        Note:
            This replaces all checks and checklist variables, including content authored in the UI.
            Omitted checks disappear from the new version; earlier versions remain available.
            Reusing a UUID preserves check lineage through renames and expression changes.
            Metadata and streaming settings are not changed by this request.

            The service rejects stale writes. This method does not refresh or retry a conflict;
            reconcile your definition with the latest checklist before trying again.
            Every successful call creates a commit, even if the definition is unchanged.

            Committing to main can update running streaming evaluations asynchronously. Returning
            confirms the commit, not adoption by every asset. No extra reload is issued by the SDK.
        """
        if not self._checks:
            raise ValueError("A checklist needs at least one check")
        if any(check.check_lineage_rid is None for check in self._checks):
            raise ValueError("Every check needs an explicit check_id when committing a checklist")
        if checklist.commit_id is None:
            raise ValueError("Call checklist.refresh() to load its commit_id before committing")
        clients = checklist._clients
        response = clients.checklist.commit(
            clients.auth_header,
            checklist.rid,
            scout_checks_api.CommitChecklistRequest(
                checks=[scout_checks_api.UpdateChecklistEntryRequest(create_check=check) for check in self._checks],
                checklist_variables=[],
                commit_message=commit_message,
                latest_commit=checklist.commit_id,
            ),
        )
        return checklist._refresh_from_api(response)
