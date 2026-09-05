from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from nominal.core.client import NominalClient
from nominal.core.datasource import DataSource


@pytest.mark.parametrize(
    ("method", "alias", "service", "rpc_name", "field"),
    [
        ("search_users", "exact_match", "authentication", "search_users_v2", "exact_match"),
        ("search_datasets", "exact_match", "catalog", "search_datasets", "exact_match"),
        ("search_assets", "exact_substring", "assets", "search_assets", "exact_substring"),
        ("search_runs", "exact_match", "run", "search_runs", "exact_match"),
        ("search_runs", "name_substring", "run", "search_runs", "exact_match"),
        ("search_workbook_templates", "exact_match", "template", "search_templates", "exact_match"),
    ],
)
@pytest.mark.parametrize("style", ["modern", "legacy", "both"])
def test_search_alias_sends_substring_to_backend(method, alias, service, rpc_name, field, style):
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "workspace"
    rpc = getattr(getattr(clients, service), rpc_name)
    rpc.return_value = SimpleNamespace(results=[], next_page_token=None)
    client = NominalClient(_clients=clients)
    arguments = {"substring_match": "needle"} if style == "modern" else {alias: "needle"}
    if style == "both":
        arguments = {"substring_match": "needle", alias: "ignored"}

    with pytest.warns(UserWarning, match=alias) if style != "modern" else nullcontext():
        assert getattr(client, method)(**arguments) == []

    rpc.assert_called_once()
    query = rpc.call_args.args[1].query
    assert [getattr(clause, field) for clause in query.and_ if getattr(clause, field) is not None] == ["needle"]


def test_legacy_positional_run_search_warns_and_forwards():
    client = NominalClient(_clients=MagicMock())
    with (
        patch.object(NominalClient, "_workspace_rid_for_search", return_value=None),
        patch.object(NominalClient, "_iter_search_runs", return_value=[]) as search,
        pytest.warns(UserWarning, match="name_substring"),
    ):
        client.search_runs(None, None, "needle")
    assert search.call_args.kwargs["substring_match"] == "needle"


@pytest.mark.parametrize("positional", [False, True])
def test_legacy_channel_search_warns_and_forwards(positional):
    clients = MagicMock()
    clients.datasource.search_channels.return_value.results = []
    clients.datasource.search_channels.return_value.next_page_token = None
    source = DataSource(rid="source", _clients=clients)
    with pytest.warns(UserWarning, match="exact_match"):
        result = source.search_channels(["needle"]) if positional else source.search_channels(exact_match=["needle"])
        assert list(result) == []
    assert clients.datasource.search_channels.call_args.args[1].exact_match == ["needle"]


@pytest.mark.parametrize("positional", [False, True])
def test_legacy_dataframe_search_warns_and_forwards(positional):
    pytest.importorskip("pandas")
    from nominal.thirdparty.pandas import datasource_to_dataframe

    source = MagicMock()
    source.search_channels.return_value = []
    with pytest.warns(UserWarning, match="channel_exact_match"):
        if positional:
            datasource_to_dataframe(source, ["needle"])
        else:
            datasource_to_dataframe(source, channel_exact_match=["needle"])
    source.search_channels.assert_called_once_with(substring_matches=["needle"], fuzzy_search_text="")
