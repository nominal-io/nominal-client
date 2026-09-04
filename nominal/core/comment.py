from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import Self

from nominal.protos.comments.v1 import comments_pb2
from nominal.ts import IntegralNanosecondsUTC


@dataclass(frozen=True)
class Comment:
    """A comment posted to a resource's discussion."""

    rid: str
    author_rid: str
    content: str
    created_at: IntegralNanosecondsUTC

    @classmethod
    def _from_proto(cls, comment: comments_pb2.Comment) -> Self:
        return cls(
            rid=comment.rid,
            author_rid=comment.author_rid,
            content=comment.content,
            created_at=comment.created_at.ToNanoseconds(),
        )
