from nominal.protos.compute.v1 import computation_spec_pb2, execution_inputs_pb2, execution_options_pb2, expressions_pb2
from nominal.protos.types.time import time_pb2


def series_summary_input(
    series: expressions_pb2.SeriesExpression,
    start: time_pb2.Timestamp,
    end: time_pb2.Timestamp,
    summarization: execution_options_pb2.SeriesSummarization | None,
) -> execution_inputs_pb2.ExecutionInput:
    return execution_inputs_pb2.ExecutionInput(
        context=expressions_pb2.ComputeContext(),
        start=start,
        end=end,
        computation=computation_spec_pb2.ComputationSpec(
            series_summary=computation_spec_pb2.SeriesSummarySpec(input=series, summarization=summarization)
        ),
    )
