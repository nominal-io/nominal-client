from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Mapping, Sequence

from typing_extensions import Self

from .._api.combined import (
    api,
    scout_api,
    scout_checks_api,
    scout_compute_api,
    scout_compute_representation_api,
    scout_run_api,
)
from ._clientsbunch import ClientsBunch
from ._utils import HasRid


# TODO(ritwikdixit): add support for more fields i.e. lineage
@dataclass(frozen=True)
class Check(HasRid):
    rid: str
    name: str
    expression: str
    priority: Priority
    description: str
    _clients: ClientsBunch = field(repr=False)


@dataclass(frozen=True)
class ChecklistVariable:
    name: str
    expression: str


@dataclass(frozen=True)
class ChecklistBuilder:
    name: str
    assignee_rid: str
    description: str
    _default_ref_name: str | None
    # note that the ChecklistBuilder is immutable, but the lists/dicts it contains are mutable
    _variables: list[_CreateChecklistVariable]
    _checks: list[_CreateCheck]
    _properties: dict[str, str]
    _labels: list[str]
    _clients: ClientsBunch = field(repr=False)

    def add_properties(self, properties: Mapping[str, str]) -> Self:
        self._properties.update(properties)
        return self

    def add_labels(self, labels: Sequence[str]) -> Self:
        self._labels.extend(labels)
        return self

    def add_check(self, name: str, expression: str, priority: Priority = 2, description: str = "") -> Self:
        self._checks.append(_CreateCheck(name=name, expression=expression, priority=priority, description=description))
        return self

    def add_variable(self, name: str, expression: str) -> Self:
        self._variables.append(_CreateChecklistVariable(name=name, expression=expression))
        return self

    def publish(self, commit_message: str | None = None) -> Checklist:
        conjure_variables = _batch_create_variable_to_conjure(
            self._variables,
            self._clients.auth_header,
            self._clients.compute_representation,
            self._default_ref_name,
        )

        conjure_checks = _batch_create_check_to_conjure(
            self._checks,
            self._clients.auth_header,
            self._clients.compute_representation,
            self._default_ref_name,
        )

        request = scout_checks_api.CreateChecklistRequest(
            commit_message=commit_message if commit_message is not None else "",
            assignee_rid=self.assignee_rid,
            title=self.name,
            description=self.description,
            # TODO(ritwikdixit): support functions
            functions={},
            properties=self._properties,
            labels=self._labels,
            checks=conjure_checks,
            checklist_variables=conjure_variables,
            # TODO(ritwikdixit): support checklist VCS
            is_published=True,
        )

        response = self._clients.checklist.create(self._clients.auth_header, request)
        return Checklist._from_conjure(self._clients, response)


@dataclass(frozen=True)
class Checklist(HasRid):
    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    checklist_variables: Sequence[ChecklistVariable]
    checks: Sequence[Check]
    _clients: ClientsBunch = field(repr=False)

    @classmethod
    def _from_conjure(cls, clients: ClientsBunch, checklist: scout_checks_api.VersionedChecklist) -> Self:
        # TODO(ritwikdixit): support draft checklists with VCS
        if not checklist.metadata.is_published:
            raise ValueError("cannot get a checklist that has not been published")

        variable_name_to_graph_map = {
            variable_name: compute_graph
            for variable_name, compute_graph in (
                _conjure_checklist_variable_to_name_graph_pair(checklistVariable)
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
        variable_names_to_expressions = clients.compute_representation.batch_compute_to_expression(
            clients.auth_header, variable_name_to_graph_map
        )
        check_rids_to_expressions = clients.compute_representation.batch_compute_to_expression(
            clients.auth_header, {check_rid: graph for check_rid, (_, graph) in check_rid_to_graph_and_def_map.items()}
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
                )
                for checklist_variable_name, expression in variable_names_to_expressions.items()
            ],
            checks=[
                Check(
                    rid=check_rid,
                    name=check_definition.title,
                    description=check_definition.description,
                    expression=check_rids_to_expressions[check_rid],
                    _clients=clients,
                    priority=_conjure_priority_to_priority(check_definition.priority),
                )
                for check_rid, check_definition in check_rids_to_definitions.items()
            ],
            _clients=clients,
        )


Priority = Literal[0, 1, 2, 3, 4]


@dataclass(frozen=True)
class _CreateChecklistVariable:
    name: str
    expression: str


@dataclass(frozen=True)
class _CreateCheck:
    name: str
    expression: str
    priority: Priority
    description: str


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


def _conjure_checklist_variable_to_name_graph_pair(
    checklist_variable: scout_checks_api.ChecklistVariable,
) -> tuple[str, scout_compute_representation_api.CompiledNode]:
    if checklist_variable.value.compute_node is None:
        raise ValueError("checklist variable is not a compute node")
    preprocessed = {
        key: value.accept(visitor=_VariableLocatorVisitor())
        for key, value in checklist_variable.value.compute_node.context.variables.items()
    }

    compute_graph = scout_compute_representation_api.CompiledNode(
        node=checklist_variable.value.compute_node.series_node.accept(visitor=_ComputeNodeVisitor()),
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
        key: value.accept(visitor=_VariableLocatorVisitor())
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


def _remove_newlines(s: str) -> str:
    return s.replace("\n", "")


def _get_compute_condition_for_compiled_node(
    node: scout_compute_representation_api.CompiledNode,
) -> scout_checks_api.UnresolvedCheckCondition:
    if node.node.type == "rangeSeries" and node.node.range_series is not None:
        return scout_checks_api.UnresolvedCheckCondition(
            num_ranges_v3=scout_checks_api.UnresolvedNumRangesConditionV3(
                function_spec={},
                operator=scout_compute_api.ThresholdOperator.GREATER_THAN,
                threshold=0,
                ranges=node.node.range_series,
                variables={
                    key: value.accept(visitor=_VariableValueVisitor()) for key, value in node.context.variables.items()
                },
                function_variables={},
            )
        )
    raise ValueError(f"only range_series nodes are currently supported, got {node.node!r}")


def _batch_get_compute_condition(
    expressions: list[str],
    auth_header: str,
    client: scout_compute_representation_api.ComputeRepresentationService,
    default_ref_name: str | None = None,
) -> dict[str, scout_checks_api.UnresolvedCheckCondition]:
    responses = client.batch_expression_to_compute(
        auth_header,
        scout_compute_representation_api.BatchExpressionToComputeRequest(
            expressions=expressions, default_ref_name=default_ref_name
        ),
    )

    condition_dict = {}
    for expression, response in responses.items():
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
    conditions = _batch_get_compute_condition(
        [create_check.expression for create_check in create_checks], auth_header, client, default_ref_name
    )
    return [
        scout_checks_api.CreateChecklistEntryRequest(
            scout_checks_api.CreateCheckRequest(
                title=check.name,
                description=check.description,
                priority=_priority_to_conjure_priority(check.priority),
                condition=conditions[check.expression],
            )
        )
        for check in create_checks
    ]


def _create_unresolved_checklist_variable(
    variable: _CreateChecklistVariable,
    node: scout_compute_representation_api.CompiledNode,
) -> scout_checks_api.UnresolvedChecklistVariable:
    return scout_checks_api.UnresolvedChecklistVariable(
        name=variable.name,
        value=scout_checks_api.UnresolvedVariableLocator(
            compute_node=scout_checks_api.UnresolvedComputeNodeWithContext(
                series_node=node.node.accept(visitor=_NodeVisitor()),
                context=scout_checks_api.UnresolvedVariables(
                    sub_function_variables={},
                    variables={
                        key: value.accept(visitor=_VariableValueVisitor())
                        for key, value in node.context.variables.items()
                    },
                ),
            )
        ),
    )


def _batch_create_variable_to_conjure(
    variables: Sequence[_CreateChecklistVariable],
    auth_header: str,
    client: scout_compute_representation_api.ComputeRepresentationService,
    default_ref_name: str | None,
) -> list[scout_checks_api.UnresolvedChecklistVariable]:
    responses = client.batch_expression_to_compute(
        auth_header,
        scout_compute_representation_api.BatchExpressionToComputeRequest(
            expressions=[variable.expression for variable in variables],
            default_ref_name=default_ref_name,
        ),
    )

    unresolved_variables = []
    for variable in variables:
        response = responses[variable.expression]
        if response.error is not None:
            raise ValueError(f"error translating expression to compute: {response.error}")
        elif response.success is not None:
            unresolved_variables.append(_create_unresolved_checklist_variable(variable, response.success.node))
        else:
            raise ValueError("expression_to_compute response is not a success or error")

    return unresolved_variables


class _ComputeNodeVisitor(scout_compute_api.ComputeNodeVisitor):
    def _enum(self, enum: scout_compute_api.EnumSeriesNode) -> scout_compute_representation_api.Node:
        return scout_compute_representation_api.Node(enumerated_series=enum)

    def _numeric(self, numeric: scout_compute_api.NumericSeriesNode) -> scout_compute_representation_api.Node:
        return scout_compute_representation_api.Node(numeric_series=numeric)

    def _ranges(self, ranges: scout_compute_api.RangesNode) -> scout_compute_representation_api.Node:
        return scout_compute_representation_api.Node(range_series=ranges)

    def _raw(self, raw: scout_compute_api.RawUntypedSeriesNode) -> scout_compute_representation_api.Node:
        raise ValueError("Raw nodes are not yet supported by the client library")


class _NodeVisitor(scout_compute_representation_api.NodeVisitor):
    def _enumerated_series(self, enumerated_series: scout_compute_api.EnumSeriesNode) -> scout_compute_api.ComputeNode:
        return scout_compute_api.ComputeNode(enum=enumerated_series)

    def _numeric_series(self, numeric_series: scout_compute_api.NumericSeriesNode) -> scout_compute_api.ComputeNode:
        return scout_compute_api.ComputeNode(numeric=numeric_series)

    def _range_series(self, range_series: scout_compute_api.RangesNode) -> scout_compute_api.ComputeNode:
        return scout_compute_api.ComputeNode(ranges=range_series)


class _VariableValueVisitor(scout_compute_representation_api.ComputeRepresentationVariableValueVisitor):
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


class _VariableLocatorVisitor(scout_checks_api.VariableLocatorVisitor):
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
