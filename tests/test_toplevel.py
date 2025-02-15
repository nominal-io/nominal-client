from unittest import mock

import requests
from conjure_python_client import ConjureEncoder
from nominal_api.attachments_api import Attachment as _Attachment

import nominal as nm
from nominal._config import ProfileConfig


class MockGetAttachmentResponse(requests.Response):
    text = ConjureEncoder().encode(
        _Attachment(
            created_at="",
            created_by="",
            description="",
            file_type="",
            is_archived=False,
            labels=[],
            properties={},
            rid="",
            s3_path="",
            title="",
        )
    )

    def raise_for_status(self) -> None:
        pass


class MockGetAttachmentResponseWithProfile(requests.Response):
    text = ConjureEncoder().encode(
        _Attachment(
            created_at="",
            created_by="",
            description="",
            file_type="",
            is_archived=False,
            labels=[],
            properties={},
            rid="",
            s3_path="",
            title="",
        )
    )

    def raise_for_status(self) -> None:
        pass


@mock.patch("nominal._config.get_token", return_value="test-token")
@mock.patch("requests.Session.request", return_value=MockGetAttachmentResponse())
def test_default_connection(mock_get: mock.Mock, token: str) -> None:
    """Test setting the connection propagates through to requests.

    The mock.patch above patches all calls to the .request() method on any requests.Session object.
    The patch will return the Mock type, which has the minimal overrides needed so that Conjure can
    decode the response, so that the call succeeds.

    In addition, the patch above records all the interactions on the .request() method,
    so we can assert conditions about the call, which allows us to verify that the connection
    was set with the parameters specified.

    This is wrapped in a try/catch so that the default connection is restored after the test completes,
    regardless if the test is successful or not.
    """
    original_base_url = nm.nominal._global_base_url
    try:
        nm.set_base_url("test-url")
        _ = nm.get_attachment("")
        assert mock_get.call_count == 1
        assert len(mock_get.call_args_list) == 1
        call = mock_get.call_args_list[0]
        assert call.args[0] == "GET"
        assert call.args[1].startswith("test-url")
        assert call.kwargs["headers"]["Authorization"].endswith("test-token")
    finally:
        nm.nominal._global_base_url = original_base_url


@mock.patch(
    "nominal._config.get_profile", return_value=ProfileConfig(url="test-profile-url", token="test-profile-token")
)
@mock.patch("requests.Session.request", return_value=MockGetAttachmentResponseWithProfile())
def test_profile_connection(mock_get: mock.Mock, mock_profile: mock.Mock) -> None:
    """Test that using a profile correctly sets the connection parameters.

    Similar to test_default_connection, but verifies that when using a profile:
    1. The profile configuration is retrieved
    2. The URL is correctly constructed with https:// prefix
    3. The token from the profile is used in the Authorization header
    """
    _ = nm.get_attachment("", profile="test-profile")
    assert mock_get.call_count == 1
    assert len(mock_get.call_args_list) == 1
    call = mock_get.call_args_list[0]
    assert call.args[0] == "GET"
    assert call.args[1].startswith("https://test-profile-url")
    assert call.kwargs["headers"]["Authorization"].endswith("test-profile-token")
