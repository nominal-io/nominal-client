from unittest.mock import MagicMock, patch

import pytest

from nominal.core.asset import Asset
from nominal.core.channel import Channel, ChannelDataType
from nominal.core.client import NominalClient
from nominal.core.datasource import DataSource
from nominal.core.run import Run


@pytest.mark.parametrize(
    ("method", "alias", "iterator"),
    [
        ("search_users", "exact_match", "_iter_search_users"),
        ("search_datasets", "exact_match", "_iter_search_datasets"),
        ("search_assets", "exact_substring", "_iter_search_assets"),
        ("search_runs", "exact_match", "_iter_search_runs"),
        ("search_runs", "name_substring", "_iter_search_runs"),
        ("search_workbook_templates", "exact_match", "_iter_search_workbook_templates"),
    ],
)
def test_legacy_search_matches_modern_query(method, alias, iterator):
    client = NominalClient(_clients=MagicMock())
    with (
        patch.object(NominalClient, "_workspace_rid_for_search", return_value=None),
        patch.object(NominalClient, iterator, return_value=[]) as search,
    ):
        getattr(client, method)(substring_match="needle")
        expected = search.call_args
        with pytest.warns(UserWarning, match=alias):
            getattr(client, method)(**{alias: "needle"})
        assert search.call_args == expected
        with pytest.warns(UserWarning, match=alias):
            getattr(client, method)(substring_match="needle", **{alias: "ignored"})
        assert search.call_args == expected


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


@pytest.mark.parametrize("owner", [NominalClient, Asset, Run])
def test_legacy_workbook_search_warns_and_forwards(owner):
    with (
        patch(f"{owner.__module__}._search_workbooks", return_value=[]) as search,
        pytest.warns(UserWarning, match="exact_match"),
    ):
        assert owner.search_workbooks(MagicMock(), exact_match="needle") == []
    assert search.call_args.kwargs["substring_match"] == "needle"


def test_legacy_log_search_warns_and_forwards():
    channel = Channel("logs", "source", ChannelDataType.LOG, None, None, MagicMock())
    with (
        patch("nominal.core.channel._log_filter_operator", side_effect=RuntimeError("stop before RPC")) as operator,
        pytest.warns(UserWarning, match="insensitive_match"),
        pytest.raises(RuntimeError, match="stop before RPC"),
    ):
        list(channel.search_logs(insensitive_match="needle"))
    operator.assert_called_once_with(regex_match=None, substring_match="needle")


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
