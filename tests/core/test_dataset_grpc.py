from unittest.mock import MagicMock

from google.protobuf.timestamp_pb2 import Timestamp

from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.client import NominalClient, WorkspaceSearchType
from nominal.core.dataset import Dataset
from nominal.experimental.dataset_utils import create_dataset_with_uuid
from nominal.protos.datasets.v1 import dataset_files_pb2, datasets_pb2


def test_dataset_proto_presence_and_nanosecond_bounds():
    """Absent metadata stays absent while epoch and negative nanosecond bounds survive conversion."""
    raw = datasets_pb2.Dataset(rid="dataset", name="name")
    dataset = Dataset._from_proto(MagicMock(), raw)
    assert dataset.description is None
    assert dataset.bounds is None
    raw.description = ""
    raw.bounds.CopyFrom(dataset_files_pb2.DataBounds(start=Timestamp(seconds=-1, nanos=999999999), end=Timestamp()))
    dataset = Dataset._from_proto(MagicMock(), raw)
    assert dataset.description == ""
    assert dataset.bounds.start == -1
    assert dataset.bounds.end == 0


def test_dataset_update_mask_distinguishes_omitted_and_empty():
    """Only supplied metadata is replaced, and empty collections explicitly clear stored values."""
    clients = MagicMock()
    raw = datasets_pb2.Dataset(rid="dataset", name="before")
    clients.grpc_datasets.UpdateDataset.return_value = datasets_pb2.UpdateDatasetResponse(
        dataset=datasets_pb2.Dataset(rid="dataset", name="after")
    )
    dataset = Dataset._from_proto(clients, raw)
    assert dataset.update(labels=[], properties={}, description="") is dataset
    request = clients.grpc_datasets.UpdateDataset.call_args.args[0]
    assert set(request.update_mask.paths) == {"labels", "properties", "description"}
    assert request.dataset.description == ""
    assert request.dataset.HasField("description")
    assert dataset.name == "after"


def test_dataset_search_preserves_filters_and_page_order():
    """Search keeps explicit empty filters, nanosecond cutoffs, archive selection, and page ordering."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    clients.grpc_datasets.SearchDatasets.side_effect = [
        datasets_pb2.SearchDatasetsResponse(datasets=[datasets_pb2.Dataset(rid="first")], next_page_token="next"),
        datasets_pb2.SearchDatasetsResponse(datasets=[datasets_pb2.Dataset(rid="second")]),
    ]
    result = client.search_datasets(
        exact_match="",
        labels=["a", "b"],
        before=0,
        after=-1,
        archive_status=ArchiveStatusFilter.ANY,
        workspace=WorkspaceSearchType.ALL,
    )
    assert [dataset.rid for dataset in result] == ["first", "second"]
    requests = [call.args[0] for call in clients.grpc_datasets.SearchDatasets.call_args_list]
    assert [request.page_token for request in requests] == ["", "next"]
    assert all(
        request.sort.field == datasets_pb2.DATASET_SORT_FIELD_CREATED_AT and request.sort.descending
        for request in requests
    )
    clauses = getattr(requests[0].query, "and").queries
    assert [clause.WhichOneof("query") for clause in clauses] == [
        "or",
        "exact_name",
        "label",
        "label",
        "created_at_or_before",
        "created_at_or_after",
    ]
    assert [clause.is_archived for clause in getattr(clauses[0], "or").queries] == [True, False]
    assert clauses[1].exact_name == ""
    assert [clauses[2].label, clauses[3].label] == ["a", "b"]
    assert clauses[4].created_at_or_before.ToNanoseconds() == 0
    assert clauses[5].created_at_or_after.ToNanoseconds() == -1


def test_uuid_create_preserves_workspace_markings_and_optional_description():
    """Explicit identity and creation context reach gRPC without filling an omitted description."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "workspace"
    clients.grpc_datasets.CreateDataset.return_value = datasets_pb2.CreateDatasetResponse(
        dataset=datasets_pb2.Dataset(rid="created")
    )
    result = create_dataset_with_uuid(
        NominalClient(_clients=clients), "uuid", "name", labels=["label", "label"], markings=["marking", "marking"]
    )
    request = clients.grpc_datasets.CreateDataset.call_args.args[0]
    assert result.rid == "created"
    assert request.dataset_uuid == "uuid"
    assert request.workspace_rid == "workspace"
    assert request.marking_rids == ["marking"]
    assert request.dataset.labels == ["label"]
    assert not request.dataset.HasField("description")


def test_batch_get_deduplicates_and_splits_at_service_limit():
    """Repeated dataset scopes are fetched once, and large requests respect the service's batch limit."""
    clients = MagicMock()
    clients.grpc_datasets.BatchGetDatasets.side_effect = lambda request: datasets_pb2.BatchGetDatasetsResponse(
        datasets=[datasets_pb2.Dataset(rid=rid) for rid in request.dataset_rids]
    )
    rids = [str(index) for index in range(1001)]
    result = NominalClient(_clients=clients).get_datasets([*rids, "0"])
    assert [dataset.rid for dataset in result] == rids
    assert [len(call.args[0].dataset_rids) for call in clients.grpc_datasets.BatchGetDatasets.call_args_list] == [
        1000,
        1,
    ]


def test_empty_update_refreshes_without_invalid_empty_mask():
    """An update with no replacements still refreshes the same snapshot without sending an empty update mask."""
    clients = MagicMock()
    dataset = Dataset._from_proto(clients, datasets_pb2.Dataset(rid="dataset", name="before"))
    clients.grpc_datasets.BatchGetDatasets.return_value = datasets_pb2.BatchGetDatasetsResponse(
        datasets=[datasets_pb2.Dataset(rid="dataset", name="after")]
    )
    assert dataset.update() is dataset
    assert dataset.name == "after"
    clients.grpc_datasets.UpdateDataset.assert_not_called()


def test_update_collapses_duplicate_labels():
    """Repeated labels retain legacy set semantics instead of failing protobuf validation."""
    clients = MagicMock()
    dataset = Dataset._from_proto(clients, datasets_pb2.Dataset(rid="dataset"))
    clients.grpc_datasets.UpdateDataset.return_value = datasets_pb2.UpdateDatasetResponse(
        dataset=datasets_pb2.Dataset(rid="dataset")
    )
    dataset.update(labels=["a", "b", "a"])
    assert clients.grpc_datasets.UpdateDataset.call_args.args[0].dataset.labels == ["a", "b"]


def test_dataset_search_preserves_large_filter_conjunctions():
    """Large filter sets retain every condition while each protobuf conjunction stays within its limit."""
    clients = MagicMock()
    clients.grpc_datasets.SearchDatasets.return_value = datasets_pb2.SearchDatasetsResponse()
    client = NominalClient(_clients=clients)
    labels = [f"label-{index}" for index in range(205)]
    properties = {f"property-{index}": str(index) for index in range(105)}

    assert client.search_datasets(labels=labels, properties=properties, workspace=WorkspaceSearchType.ALL) == []

    def leaves(query):
        if query.WhichOneof("query") == "and":
            children = getattr(query, "and").queries
            assert 1 <= len(children) <= 100
            return [leaf for child in children for leaf in leaves(child)]
        return [query]

    clauses = leaves(clients.grpc_datasets.SearchDatasets.call_args.args[0].query)
    assert clauses[0].WhichOneof("query") == "is_archived"
    assert not clauses[0].is_archived
    assert [clause.label for clause in clauses[1:206]] == labels
    assert {clause.property.name: clause.property.value for clause in clauses[206:]} == properties
