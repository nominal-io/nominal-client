from __future__ import annotations

from nominal.core.comment import Comment
from nominal.protos.comments.v1 import comments_pb2


def test_comment_from_proto_maps_fields_and_timestamp() -> None:
    """_from_proto maps rid/author_rid/content and converts the proto Timestamp to integral nanos UTC."""
    proto = comments_pb2.Comment(rid="ri.comment.1", author_rid="ri.user.1", content="hello")
    proto.created_at.FromNanoseconds(1_700_000_000_000_000_000)

    comment = Comment._from_proto(proto)

    assert comment == Comment(
        rid="ri.comment.1",
        author_rid="ri.user.1",
        content="hello",
        created_at=1_700_000_000_000_000_000,
    )
