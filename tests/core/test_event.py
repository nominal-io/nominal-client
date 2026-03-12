from unittest.mock import MagicMock

import pytest

from nominal.core._event_types import EventType
from nominal.core.event import _create_event
from nominal.core.exceptions import NominalAPIError


def _make_nominal_api_error(*, status_code: int = 400, error_name: str = "", error_code: str = "") -> NominalAPIError:
    """Create a NominalAPIError with the given attributes."""
    message = f"{error_name}: {status_code} Client Error" if error_name else f"{status_code} Client Error"
    return NominalAPIError(message, status_code=status_code, error_name=error_name, error_code=error_code)


class TestCreateEventErrorHandling:
    def test_api_error_propagates_with_details(self):
        clients = MagicMock()
        clients.event.create_event.side_effect = _make_nominal_api_error(
            status_code=400,
            error_name="Scout:MissingAssetRid",
            error_code="INVALID_ARGUMENT",
        )

        with pytest.raises(NominalAPIError) as exc_info:
            _create_event(
                clients,
                name="test",
                type=EventType.INFO,
                start=0,
                duration=0,
                assets=None,
                description=None,
                properties=None,
                labels=None,
            )

        err = exc_info.value
        assert err.status_code == 400
        assert err.error_name == "Scout:MissingAssetRid"
        assert err.error_code == "INVALID_ARGUMENT"
        assert "Scout:MissingAssetRid" in str(err)

    def test_api_error_does_not_have_chained_traceback(self):
        clients = MagicMock()
        clients.event.create_event.side_effect = _make_nominal_api_error(
            status_code=400,
            error_name="Scout:MissingAssetRid",
            error_code="INVALID_ARGUMENT",
        )

        with pytest.raises(NominalAPIError) as exc_info:
            _create_event(
                clients,
                name="test",
                type=EventType.INFO,
                start=0,
                duration=0,
                assets=None,
                description=None,
                properties=None,
                labels=None,
            )

        assert exc_info.value.__cause__ is None
