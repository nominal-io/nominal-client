"""Author checklist failure conditions with compute expression objects."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Self, Sequence

from conjure_python_client import ConjureDecoder
from nominal_api import scout_api, scout_checks_api, scout_compute_api

import nominal.compute as nc
from nominal.core import Checklist, NominalClient, Priority, User, Workspace
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core._utils.api_types import NominalProperties
from nominal.experimental.checklist_utils.checklist_utils import _create_checklist_with_content


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
    """Collect compute failure conditions, then create a published checklist.

    Construct directly with ``ChecklistBuilder()``. Adding checks is local and returns
    the same builder for chaining. Only `create` makes network requests. Expression graphs
    and channel bindings are captured when each check is added; Python source is not stored.

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

    def __init__(self) -> None:
        self._checks: list[scout_checks_api.CreateChecklistEntryRequest] = []

    def add_check(
        self,
        name: str,
        failure_ranges: nc.RangeSeries,
        *,
        inputs: Mapping[str, ChannelBinding],
        priority: Priority,
        description: str = "",
    ) -> Self:
        """Add a check whose violations are the ranges produced by an expression.

        Args:
            name: Display name of the check.
            failure_ranges: Compute range expression describing violations, not passing intervals.
                Convert boolean expressions with ``.ranges()`` before passing them here.
            inputs: Map expression reference names to channels in the execution context.
            priority: Severity of violations, from P0 (most severe) to P4.
            description: Description of the check.

        Returns:
            This builder.

        Raises:
            TypeError: If ``failure_ranges`` is not a compute ``RangeSeries``.

        Note:
            Alignment, fill, and missing-data behavior follow the service's checklist defaults.
            The SDK does not interpret absence of data as a passing check or validate references
            against live channels. Persistence and other range operations belong in the expression.
        """
        return self.add_parameterized_check(
            name, failure_ranges, implementations=(inputs,), priority=priority, description=description
        )

    def add_parameterized_check(
        self,
        name: str,
        failure_ranges: nc.RangeSeries,
        *,
        implementations: Iterable[Mapping[str, ChannelBinding]],
        priority: Priority,
        description: str = "",
    ) -> Self:
        """Add one expression evaluated separately for each set of input bindings.

        Args:
            name: Display name of the check.
            failure_ranges: Range expression describing violations. Uses the same execution
                semantics as `add_check`.
            implementations: Nonempty iterable of reference-to-channel mappings. Each mapping
                is evaluated independently; violations are associated with its implementation
                index. Consumed and copied once when this method is called.
            priority: Severity of violations, from P0 (most severe) to P4.
            description: Description shared by the implementations.

        Returns:
            This builder.

        Raises:
            TypeError: If ``failure_ranges`` is not a compute ``RangeSeries``.
            ValueError: If no implementations are supplied.
        """
        if not isinstance(failure_ranges, nc.RangeSeries):
            raise TypeError("failure_ranges must be a nominal.compute.RangeSeries; convert booleans with .ranges()")
        ranges: scout_compute_api.RangeSeries = ConjureDecoder.do_decode(
            json.loads(failure_ranges.to_json()),  # type: ignore[attr-defined]
            scout_compute_api.RangeSeries,
        )
        variables = [
            scout_checks_api.UnresolvedVariables(
                variables={reference: binding._to_conjure() for reference, binding in inputs.items()}
            )
            for inputs in implementations
        ]
        if not variables:
            raise ValueError("A parameterized check needs at least one implementation")
        self._checks.append(
            scout_checks_api.CreateChecklistEntryRequest(
                create_check=scout_checks_api.CreateCheckRequest(
                    title=name,
                    description=description,
                    priority=scout_api.Priority(priority.name),
                    condition=scout_checks_api.UnresolvedCheckCondition(
                        parameterized_num_ranges_v1=scout_checks_api.UnresolvedParameterizedNumRangesConditionV1(
                            ranges=ranges,
                            implementations=variables,
                        )
                    ),
                )
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
            checks=list(self._checks),
            labels=list(labels),
            properties=properties,
            is_published=True,
            workspace=workspace,
        )
