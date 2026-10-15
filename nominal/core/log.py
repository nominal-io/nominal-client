from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Iterable, Mapping, Self

from google.protobuf import timestamp_pb2
from nominal_api import api, scout_compute_api

from nominal._utils import batched
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.protos.channel_writer.v1 import nominal_channel_writer_pb2 as writer_pb
from nominal.protos.channel_writer.v1 import nominal_channel_writer_pb2_grpc
from nominal.protos.direct_channel_writer_common.v2 import direct_channel_writer_common_pb2
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

_EMPTY_MAP: Mapping[str, str] = MappingProxyType({})


@dataclass(frozen=True)
class LogPoint:
    """LogPoint is a single, timestamped log entry.

    LogPoints are added to a Dataset using `Dataset.write_logs`.

    """

    timestamp: IntegralNanosecondsUTC
    message: str
    args: Mapping[str, str]

    @classmethod
    def create(
        cls, timestamp: str | datetime | IntegralNanosecondsUTC, message: str, args: Mapping[str, str] | None
    ) -> Self:
        return cls(
            timestamp=_SecondsNanos.from_flexible(timestamp).to_nanoseconds(),
            message=message,
            args=_EMPTY_MAP if args is None else MappingProxyType(args),
        )

    @classmethod
    def _from_compute_api(cls, point: scout_compute_api.LogValue, timestamp: api.Timestamp) -> Self:
        return cls(
            timestamp=_SecondsNanos.from_api(timestamp).to_nanoseconds(),
            message=point.message,
            args=point.args,
        )

    def _to_proto(self) -> writer_pb.LogPoint:
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromNanoseconds(self.timestamp)
        return writer_pb.LogPoint(
            timestamp=timestamp,
            value=direct_channel_writer_common_pb2.LogValue(
                message=self.message,
                args=dict(self.args),
            ),
        )


def _log_filter_operator(
    regex_match: str | None = None, insensitive_match: str | None = None
) -> scout_compute_api.LogFilterOperator:
    if regex_match and insensitive_match:
        raise ValueError("Only one of `regex_match` or `insensitive_match` may be provided")
    elif regex_match:
        return scout_compute_api.LogFilterOperator(
            regex_filter=scout_compute_api.LogRegexFilterOperator(regex=regex_match)
        )
    else:
        return scout_compute_api.LogFilterOperator(
            exact_match_case_insensitive_filter=scout_compute_api.LogExactMatchCaseInsensitiveFilter(
                token=insensitive_match or ""
            )
        )


def _write_logs(
    client: nominal_channel_writer_pb2_grpc.NominalChannelWriterServiceStub,
    data_source_rid: str,
    logs: Iterable[LogPoint],
    channel_name: str,
    batch_size: int,
) -> None:
    for batch in batched(logs, batch_size):
        request = writer_pb.WriteLogsRequest(
            data_source_rid=data_source_rid,
            logs=[log._to_proto() for log in batch],
            channel=channel_name,
        )
        with translate_grpc_errors():
            client.WriteLogs(request)
