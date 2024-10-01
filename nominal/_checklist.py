from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Mapping, Sequence

import yaml
from pydantic import BaseModel, Field, PrivateAttr
from typing_extensions import Self

from nominal._api.combined import (
    api,
    scout_api,
    scout_checks_api,
    scout_compute_api,
    scout_compute_representation_api,
    scout_run_api,
)

if TYPE_CHECKING:
    # prevent circular imports
    from .core import NominalClient


# TODO(ritwikdixit): add support for more fields i.e. lineage
@dataclass(frozen=True)
class Check:
    rid: str
    name: str
    expression: str
    priority: Priority
    description: str
    _client: NominalClient = field(repr=False)


@dataclass(frozen=True)
class ChecklistVariable:
    name: str
    expression: str
    _client: NominalClient = field(repr=False)


class _CreateChecklistVariable(BaseModel):
    name: str
    expression: str


class _CreateCheck(BaseModel):
    name: str
    priority: Priority
    expression: str
    description: str | None


class ChecklistBuilder(BaseModel):
    name: str
    assignee_email: str
    checklist_variables: list[_CreateChecklistVariable] = Field(default_factory=list)
    checks: list[_CreateCheck] = Field(default_factory=list)
    default_ref_name: str | None = None
    commit_message: str | None = None
    description: str | None = None
    properties: Mapping[str, str] | None = None
    labels: Sequence[str] = Field(default_factory=list)
    _client: NominalClient = PrivateAttr()

    class Config:
        arbitrary_types_allowed = True

    def add_metadata(
        self,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        if properties is not None:
            self.properties = properties
        if labels is not None:
            self.labels = labels
        return self

    # TODO(alkasm): reorder args
    def add_check(self, name: str, expression: str, priority: Priority = 2, description: str | None = None) -> Self:
        self.checks.append(_CreateCheck(name=name, priority=priority, expression=expression, description=description))
        return self

    def add_checklist_variable(self, name: str, expression: str) -> Self:
        self.checklist_variables.append(_CreateChecklistVariable(name=name, expression=expression))
        return self

    def build_and_publish(self, commit_message: str | None = None) -> Checklist:
        conjure_checklist_variables = _batch_create_checklist_variable_to_conjure(
            self.checklist_variables,
            self._client._auth_header,
            self._client._compute_representation_client,
            self.default_ref_name,
        )

        conjure_checks = _batch_create_check_to_conjure(
            self.checks, self._client._auth_header, self._client._compute_representation_client, self.default_ref_name
        )

        request = scout_checks_api.CreateChecklistRequest(
            commit_message=commit_message if commit_message is not None else "",
            assignee_rid=self._client._get_user_rid_from_email(self.assignee_email),
            title=self.name,
            description=self.description if self.description is not None else "",
            # TODO(ritwikdixit): support functions
            functions={},
            properties={} if self.properties is None else dict(self.properties),
            labels=[] if self.labels is None else list(self.labels),
            checks=conjure_checks,
            checklist_variables=conjure_checklist_variables,
            # TODO(ritwikdixit): support checklist VCS
            is_published=True,
        )

        response = self._client._checklist_api_client.create(self._client._auth_header, request)
        return Checklist._from_conjure(self._client, response)


# TODO(ritwikdixit): add support for more Checklist metadata and versioning
@dataclass(frozen=True)
class Checklist:
    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    checklist_variables: Sequence[ChecklistVariable]
    checks: Sequence[Check]
    _client: NominalClient = field(repr=False)

    @classmethod
    def _from_conjure(cls, client: NominalClient, checklist: scout_checks_api.VersionedChecklist) -> Self:
        # TODO(ritwikdixit): support draft checklists with VCS
        if not checklist.metadata.is_published:
            raise ValueError("cannot get a checklist that has not been published")

        variable_name_to_graph_map = {
            variable_name: compute_graph
            for variable_name, compute_graph in (
                _conjure_checklist_variable_to_name_graph__pair(checklistVariable)
                for checklistVariable in checklist.checklist_variables
            )
        }
        check_rid_to_graph_and_def_map = {
            check_definition.rid: (check_definition, compute_graph)
            for check_definition, compute_graph in (
                _conjure_check_to_check_definition_graph_pair(check) for check in checklist.checks
            )
        }

        # # TODO(ritwikdixit): remove the need for these extraneous network requests
        variable_names_to_expressions = client._compute_representation_client.batch_compute_to_expression(
            client._auth_header, variable_name_to_graph_map
        )
        check_rids_to_expressions = client._compute_representation_client.batch_compute_to_expression(
            client._auth_header, {check_rid: graph for check_rid, (_, graph) in check_rid_to_graph_and_def_map.items()}
        )
        check_rids_to_definitions = {
            check_rid: check_def for check_rid, (check_def, _) in check_rid_to_graph_and_def_map.items()
        }

        return cls(
            rid=checklist.rid,
            name=checklist.metadata.title,
            description=checklist.metadata.description,
            properties=checklist.metadata.properties,
            labels=checklist.metadata.labels,
            checklist_variables=[
                ChecklistVariable(
                    name=checklist_variable_name,
                    expression=expression,
                    _client=client,
                )
                for checklist_variable_name, expression in variable_names_to_expressions.items()
            ],
            checks=[
                Check(
                    rid=check_rid,
                    name=check_definition.title,
                    description=check_definition.description,
                    expression=check_rids_to_expressions[check_rid],
                    _client=client,
                    priority=_conjure_priority_to_priority(check_definition.priority),
                )
                for check_rid, check_definition in check_rids_to_definitions.items()
            ],
            _client=client,
        )


Priority = Literal[0, 1, 2, 3, 4]
_priority_to_conjure_map: dict[Priority, scout_checks_api.Priority] = {
    0: scout_checks_api.Priority.P0,
    1: scout_checks_api.Priority.P1,
    2: scout_checks_api.Priority.P2,
    3: scout_checks_api.Priority.P3,
    4: scout_checks_api.Priority.P4,
}


def _priority_to_conjure_priority(priority: Priority) -> scout_checks_api.Priority:
    if priority in _priority_to_conjure_map:
        return _priority_to_conjure_map[priority]
    raise ValueError(f"unknown priority {priority}, expected one of {_priority_to_conjure_map.keys()}")


def _conjure_priority_to_priority(priority: scout_checks_api.Priority) -> Priority:
    inverted_map = {v: k for k, v in _priority_to_conjure_map.items()}
    if priority in inverted_map:
        return inverted_map[priority]
    raise ValueError(f"unknown priority '{priority}', expected one of {_priority_to_conjure_map.values()}")


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


def _create_checklist_builder_from_yaml(checklist_config_path: str, client: NominalClient) -> ChecklistBuilder:
    with open(checklist_config_path, "r") as file:
        checklist_dict = yaml.safe_load(file)
        data = checklist_dict["checklist"]

        checklist_builder = ChecklistBuilder(
            name=data["name"],
            assignee_email=data["assignee_email"],
            description=data.get("description"),
            default_ref_name=data.get("default_ref_name"),
        ).add_metadata(
            properties=data.get("properties"),
            labels=data.get("labels"),
        )
        checklist_builder._client = client

        if "variables" in data:
            for variable_dict in data["variables"]:
                variable_dict["expression"] = _remove_newlines(variable_dict["expression"])
                checklist_builder.add_checklist_variable(**variable_dict)

        if "checks" in data:
            for check_dict in data["checks"]:
                check_dict["expression"] = _remove_newlines(check_dict["expression"])
                checklist_builder.add_check(**check_dict)

        return checklist_builder


def _batch_get_compute_condition(
    expressions: list[str],
    auth_header: str,
    client: scout_compute_representation_api.ComputeRepresentationService,
    default_ref_name: str | None = None,
) -> dict[str, scout_checks_api.UnresolvedCheckCondition]:
    response_dict = client.batch_expression_to_compute(
        auth_header,
        scout_compute_representation_api.BatchExpressionToComputeRequest(
            expressions=expressions, default_ref_name=default_ref_name
        ),
    )

    def _get_compute_condition_for_compiled_node(
        compiledNode: scout_compute_representation_api.CompiledNode,
    ) -> scout_checks_api.UnresolvedCheckCondition:
        if compiledNode.node.type == "rangeSeries" and compiledNode.node.range_series is not None:
            return scout_checks_api.UnresolvedCheckCondition(
                num_ranges_v3=scout_checks_api.UnresolvedNumRangesConditionV3(
                    function_spec={},
                    operator=scout_compute_api.ThresholdOperator.GREATER_THAN,
                    threshold=0,
                    ranges=compiledNode.node.range_series,
                    variables={
                        key: _representation_variable_to_unresolved_variable_locator(value)
                        for key, value in compiledNode.context.variables.items()
                    },
                    function_variables={},
                )
            )
        else:
            raise ValueError("expression does not evaluate to a range_series")

    condition_dict = {}
    for expression, response in response_dict.items():
        if response.error is not None:
            raise ValueError(f"error translating expression to compute: {response.error}")
        elif response.success is not None:
            condition_dict[expression] = _get_compute_condition_for_compiled_node(response.success.node)
        else:
            raise ValueError("expression_to_compute response is not a success or error")

    return condition_dict


def _batch_create_check_to_conjure(
    create_checks: Sequence[_CreateCheck],
    auth_header: str,
    client: scout_compute_representation_api.ComputeRepresentationService,
    default_ref_name: str | None = None,
) -> list[scout_checks_api.CreateChecklistEntryRequest]:
    conditions_dict = _batch_get_compute_condition(
        [create_check.expression for create_check in create_checks], auth_header, client, default_ref_name
    )

    def _create_check_request(create_check: _CreateCheck) -> scout_checks_api.CreateChecklistEntryRequest:
        return scout_checks_api.CreateChecklistEntryRequest(
            scout_checks_api.CreateCheckRequest(
                title=create_check.name,
                description=create_check.description if create_check.description is not None else "",
                priority=_priority_to_conjure_priority(create_check.priority),
                condition=conditions_dict[create_check.expression],
            )
        )

    return [_create_check_request(create_check) for create_check in create_checks]


def _batch_create_checklist_variable_to_conjure(
    create_checklist_variables: Sequence[_CreateChecklistVariable],
    auth_header: str,
    client: scout_compute_representation_api.ComputeRepresentationService,
    default_ref_name: str | None = None,
) -> list[scout_checks_api.UnresolvedChecklistVariable]:
    response_dict = client.batch_expression_to_compute(
        auth_header,
        scout_compute_representation_api.BatchExpressionToComputeRequest(
            expressions=[
                create_checklist_variable.expression for create_checklist_variable in create_checklist_variables
            ],
            default_ref_name=default_ref_name,
        ),
    )

    def _create_unresolved_checklist_variable(
        compiledNode: scout_compute_representation_api.CompiledNode,
    ) -> scout_checks_api.UnresolvedChecklistVariable:
        return scout_checks_api.UnresolvedChecklistVariable(
            name=create_checklist_variable.name,
            value=scout_checks_api.UnresolvedVariableLocator(
                compute_node=scout_checks_api.UnresolvedComputeNodeWithContext(
                    series_node=_compiled_node_to_compute_node(compiledNode.node),
                    context=scout_checks_api.UnresolvedVariables(
                        sub_function_variables={},
                        variables={
                            key: _representation_variable_to_unresolved_variable_locator(value)
                            for key, value in compiledNode.context.variables.items()
                        },
                    ),
                )
            ),
        )

    unresolved_checklist_variables = []
    for create_checklist_variable in create_checklist_variables:
        response = response_dict[create_checklist_variable.expression]
        if response.error is not None:
            raise ValueError(f"error translating expression to compute: {response.error}")
        elif response.success is not None:
            unresolved_checklist_variables.append(_create_unresolved_checklist_variable(response.success.node))
        else:
            raise ValueError("expression_to_compute response is not a success or error")

    return unresolved_checklist_variables
