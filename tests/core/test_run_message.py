from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.comment import Comment
from nominal.core.run import Run
from nominal.protos.comments.v1 import comments_pb2

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


def _stub_comment() -> comments_pb2.Comment:
    c = comments_pb2.Comment(
        rid="ri.scout.test.comment.123",
        author_rid="ri.users.user.42",
        content="hello",
    )
    c.created_at.FromNanoseconds(1_778_155_200_000_000_000)
    return c


def test_add_comment_sends_run_parented_request(mock_run, mock_clients):
    mock_clients.comments.CreateComment.return_value = comments_pb2.CreateCommentResponse(comment=_stub_comment())

    mock_run.add_comment("hello")

    mock_clients.comments.CreateComment.assert_called_once()
    (request,) = mock_clients.comments.CreateComment.call_args.args
    assert isinstance(request, comments_pb2.CreateCommentRequest)
    assert request.content == "hello"
    assert request.parent.resource.resource_type == comments_pb2.ResourceType.RUN
    assert request.parent.resource.resource_rid == RUN_RID


def test_add_comment_returns_comment_dataclass(mock_run, mock_clients):
    mock_clients.comments.CreateComment.return_value = comments_pb2.CreateCommentResponse(comment=_stub_comment())

    comment = mock_run.add_comment("hello")

    assert isinstance(comment, Comment)
    assert comment.rid == "ri.scout.test.comment.123"
    assert comment.author_rid == "ri.users.user.42"
    assert comment.content == "hello"
    # 2026-05-07T12:00:00Z in nanoseconds since epoch
    assert comment.created_at == 1_778_155_200_000_000_000
