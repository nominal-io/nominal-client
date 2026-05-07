from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from nominal_api import comments_api

from nominal.core.comment import Message
from nominal.core.run import Run

RUN_RID = "ri.scout.test.run.abc"


@pytest.fixture
def mock_clients():
    return MagicMock()


@pytest.fixture
def mock_run(mock_clients):
    return Run(
        rid=RUN_RID,
        name="Test Run",
        description="",
        properties={},
        labels=[],
        links=[],
        start=0,
        end=None,
        run_number=1,
        assets=[],
        created_at=0,
        _clients=mock_clients,
    )


def _stub_comment() -> comments_api.Comment:
    return comments_api.Comment(
        rid="ri.scout.test.comment.123",
        parent=comments_api.CommentParent(
            resource=comments_api.CommentParentResource(
                resource_type=comments_api.ResourceType.RUN,
                resource_rid=RUN_RID,
            )
        ),
        author_rid="ri.users.user.42",
        created_at="2026-05-07T12:00:00Z",
        content="hello",
        attachments=[],
        reactions=[],
    )


def test_add_message_sends_run_parented_request(mock_run, mock_clients):
    mock_clients.comments.create_comment.return_value = _stub_comment()

    mock_run.add_message("hello")

    mock_clients.comments.create_comment.assert_called_once()
    auth_header, request = mock_clients.comments.create_comment.call_args.args
    assert auth_header == mock_clients.auth_header
    assert isinstance(request, comments_api.CreateCommentRequest)
    assert request.content == "hello"
    assert request.attachments == []
    parent_resource = request.parent.resource
    assert parent_resource is not None
    assert parent_resource.resource_type == comments_api.ResourceType.RUN
    assert parent_resource.resource_rid == RUN_RID


def test_add_message_returns_message_dataclass(mock_run, mock_clients):
    mock_clients.comments.create_comment.return_value = _stub_comment()

    message = mock_run.add_message("hello")

    assert isinstance(message, Message)
    assert message.rid == "ri.scout.test.comment.123"
    assert message.author_rid == "ri.users.user.42"
    assert message.content == "hello"
    assert message.created_at == datetime(2026, 5, 7, 12, 0, 0, tzinfo=timezone.utc)
