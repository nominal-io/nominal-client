import importlib
import sys

import pytest

from nominal.core import BatchAddChannelsResult, CreateChannelRequest


def test_legacy_channel_imports_preserve_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Legacy channel imports warn and resolve to the canonical classes without restoring the mixin."""
    monkeypatch.delitem(sys.modules, "nominal.core.datasource", raising=False)
    with pytest.warns(DeprecationWarning, match=r"from nominal\.core instead\."):
        module = importlib.import_module("nominal.core.datasource")

    assert module.CreateChannelRequest is CreateChannelRequest
    assert module.BatchAddChannelsResult is BatchAddChannelsResult
    assert not hasattr(module, "DataSource")
