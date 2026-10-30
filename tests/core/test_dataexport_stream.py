import gzip
from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core._utils.dataexport import stream_export
from nominal.exceptions import NominalPermissionDeniedError
from nominal.protos.dataexports.v1 import data_exports_pb2 as export_pb
from tests.conftest import _FakeRpcError


def test_stream_reads_incrementally_and_cancels_on_close():
    """Small reads consume only bounded chunks and closing releases the underlying RPC."""
    service = MagicMock()
    call = service.StreamExport.return_value
    call.__next__.side_effect = [
        export_pb.StreamExportResponse(data=b"a" * 10000),
        export_pb.StreamExportResponse(data=b"b"),
        StopIteration,
    ]
    with stream_export(service, export_pb.ExportDefinition()) as stream:
        assert stream.read(1) == b"a"
        assert call.__next__.call_count == 1
        assert stream.read() == b"a" * 9999 + b"b"
    call.cancel.assert_called_once()


def test_stream_handles_empty_chunks_and_gzip_boundaries():
    """Empty chunks do not signal EOF, and compressed bytes can span arbitrary RPC boundaries."""
    payload = gzip.compress(b"timestamp,value\n0,1\n")
    service = MagicMock()
    service.StreamExport.return_value.__next__.side_effect = [
        export_pb.StreamExportResponse(),
        *(export_pb.StreamExportResponse(data=bytes([byte])) for byte in payload),
        StopIteration,
    ]
    with stream_export(service, export_pb.ExportDefinition()) as stream:
        with gzip.GzipFile(fileobj=stream) as decoded:
            assert decoded.read() == b"timestamp,value\n0,1\n"


def test_stream_iteration_error_is_translated_and_closed():
    """Errors raised after the RPC starts are translated and closing cancels unfinished work."""
    service = MagicMock()
    call = service.StreamExport.return_value
    call.__next__.side_effect = _FakeRpcError(grpc.StatusCode.PERMISSION_DENIED)
    with pytest.raises(NominalPermissionDeniedError):
        with stream_export(service, export_pb.ExportDefinition()) as stream:
            stream.read()
    call.cancel.assert_called_once()
