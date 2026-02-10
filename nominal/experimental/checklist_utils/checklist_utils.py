from __future__ import annotations

from nominal_api import (
    scout_checks_api,
)

from nominal.core import NominalClient
from nominal.core._utils.api_tools import (
    rid_from_instance_or_string,
)
from nominal.core.checklist import Checklist
from nominal.core.client import WorkspaceSearchType
from nominal.core.workspace import Workspace
from nominal.experimental.id_utils.id_utils import UUID_PATTERN


def _create_checklist_with_content(
    client: NominalClient,
    title: str,
    *,
    commit_message: str | None = None,
    assignee_rid: str | None = None,
    description: str | None = None,
    checks: list[scout_checks_api.CreateChecklistEntryRequest] | None = None,
    properties: dict[str, str] | None = None,
    labels: list[str] | None = None,
    checklist_variables: list[scout_checks_api.UnresolvedChecklistVariable] | None = None,
    is_published: bool | None = False,
    workspace: Workspace | str | None = None,
) -> Checklist:
    request = scout_checks_api.CreateChecklistRequest(
        commit_message=commit_message or "",
        assignee_rid=rid_from_instance_or_string(assignee_rid) if assignee_rid else client.get_user().rid,
        title=title,
        description=description or "",
        checks=checks or [],
        properties=properties or {},
        labels=labels or [],
        checklist_variables=checklist_variables or [],
        is_published=is_published,
        workspace=client._workspace_rid_for_search(workspace or WorkspaceSearchType.ALL),
    )

    template = client._clients.checklist.create(client._clients.auth_header, request)
    return Checklist._from_conjure(client._clients, template)


def _to_unresolved_variable_locator(
    locator: scout_checks_api.VariableLocator,
) -> scout_checks_api.UnresolvedVariableLocator:
    """Transforms VariableLocator (in the Checklist Conjure object) to its
    required format in the request, preserving all underlying data.
    """
    if locator.checklist_variable is not None:
        return scout_checks_api.UnresolvedVariableLocator(checklist_variable=locator.checklist_variable)
    if locator.series is not None:
        return scout_checks_api.UnresolvedVariableLocator(series=locator.series)
    if locator.timestamp is not None:
        return scout_checks_api.UnresolvedVariableLocator(timestamp=locator.timestamp)
    if locator.compute_node is not None:
        context = locator.compute_node.context
        return scout_checks_api.UnresolvedVariableLocator(
            compute_node=scout_checks_api.UnresolvedComputeNodeWithContext(
                series_node=locator.compute_node.series_node,
                context=scout_checks_api.UnresolvedVariables(
                    variables={k: _to_unresolved_variable_locator(v) for k, v in context.variables.items()}
                ),
            )
        )
    raise ValueError("Unsupported VariableLocator variant")


def _to_unresolved_condition(
    condition: scout_checks_api.CheckCondition | None,
) -> scout_checks_api.UnresolvedCheckCondition | None:
    """Transforms CheckCondition (in the Checklist Conjure object) to its
    required format in the request, preserving all underlying data.
    """
    if condition is None:
        return None

    if condition.num_ranges_v2 is not None:
        resolved_v2 = condition.num_ranges_v2
        return scout_checks_api.UnresolvedCheckCondition(
            num_ranges_v2=scout_checks_api.UnresolvedNumRangesConditionV2(
                ranges=resolved_v2.ranges,
                function_spec=resolved_v2.function_spec,
                threshold=resolved_v2.threshold,
                operator=resolved_v2.operator,
                variables={k: _to_unresolved_variable_locator(v) for k, v in resolved_v2.variables.items()},
            )
        )

    if condition.num_ranges_v3 is not None:
        resolved_v3 = condition.num_ranges_v3
        return scout_checks_api.UnresolvedCheckCondition(
            num_ranges_v3=scout_checks_api.UnresolvedNumRangesConditionV3(
                ranges=resolved_v3.ranges,
                function_spec=resolved_v3.function_spec,
                threshold=resolved_v3.threshold,
                operator=resolved_v3.operator,
                variables={k: _to_unresolved_variable_locator(v) for k, v in resolved_v3.variables.items()},
            )
        )

    if condition.parameterized_num_ranges_v1 is not None:
        resolved_param = condition.parameterized_num_ranges_v1
        return scout_checks_api.UnresolvedCheckCondition(
            parameterized_num_ranges_v1=scout_checks_api.UnresolvedParameterizedNumRangesConditionV1(
                ranges=resolved_param.ranges,
                implementations=[
                    scout_checks_api.UnresolvedVariables(
                        variables={k: _to_unresolved_variable_locator(v) for k, v in impl.variables.items()}
                    )
                    for impl in resolved_param.implementations
                ],
            )
        )

    if condition.num_ranges is not None:
        raise NotImplementedError("NumRangesConditionV1 has no Unresolved equivalent")
    raise ValueError("Unsupported CheckCondition variant")


def _to_create_check_request(
    check: scout_checks_api.Check,
) -> scout_checks_api.CreateCheckRequest:
    """Transforms CheckRequest (in the Checklist Conjure object) to its
    required format in the request, preserving all underlying data.
    """
    # Extract UUID from check_lineage_rid if it's a full RID
    # The API expects a UUID, not a full RID string
    check_lineage_uuid = None
    if check.check_lineage_rid:
        if len(check.check_lineage_rid) == 36 and check.check_lineage_rid.count("-") == 4:
            check_lineage_uuid = check.check_lineage_rid
        else:
            match = UUID_PATTERN.search(check.check_lineage_rid)
            if match:
                check_lineage_uuid = match.group(1)

    return scout_checks_api.CreateCheckRequest(
        check_lineage_rid=check_lineage_uuid,
        title=check.title,
        description=check.description,
        auto_generated_title=check.auto_generated_title,
        auto_generated_description=check.auto_generated_description,
        priority=check.priority,
        generated_event_type=check.generated_event_type,
        generated_event_labels=check.generated_event_labels,
        condition=_to_unresolved_condition(check.condition),
    )


def _to_create_checklist_entries(
    entries: list[scout_checks_api.ChecklistEntry],
) -> list[scout_checks_api.CreateChecklistEntryRequest]:
    """Transforms ChecklistEntries (in the Checklist Conjure object) to its
    required format in the request, preserving all underlying data.
    """
    result: list[scout_checks_api.CreateChecklistEntryRequest] = []
    for entry in entries:
        if entry.check is None:
            raise ValueError("ChecklistEntry variant unsupported")
        result.append(scout_checks_api.CreateChecklistEntryRequest(create_check=_to_create_check_request(entry.check)))
    return result


def _to_unresolved_checklist_variables(
    variables: list[scout_checks_api.ChecklistVariable],
) -> list[scout_checks_api.UnresolvedChecklistVariable]:
    """Transforms ChecklistVariable (in the Checklist Conjure object) to its
    required format in the request, preserving all underlying data.
    """
    return [
        scout_checks_api.UnresolvedChecklistVariable(
            name=variable.name,
            display_name=variable.display_name,
            value=_to_unresolved_variable_locator(variable.value),
        )
        for variable in variables
    ]
