from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from nominal_api import comments_api

from nominal.core.comment import Comment
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


def test_add_comment_sends_run_parented_request(mock_run, mock_clients):
    mock_clients.comments.create_comment.return_value = _stub_comment()

    mock_run.add_comment("hello")

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


def test_add_comment_returns_comment_dataclass(mock_run, mock_clients):
    mock_clients.comments.create_comment.return_value = _stub_comment()

    comment = mock_run.add_comment("hello")

    assert isinstance(comment, Comment)
    assert comment.rid == "ri.scout.test.comment.123"
    assert comment.author_rid == "ri.users.user.42"
    assert comment.content == "hello"
    # 2026-05-07T12:00:00Z in nanoseconds since epoch
    assert comment.created_at == 1_778_155_200_000_000_000
