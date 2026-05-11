from __future__ import annotations

from dataclasses import dataclass

from nominal_api import comments_api
from typing_extensions import Self

from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True)
class Comment:
    """A comment posted to a resource's discussion."""

    rid: str
    author_rid: str
    content: str
    created_at: IntegralNanosecondsUTC

    @classmethod
    def _from_conjure(cls, api: comments_api.Comment) -> Self:
        return cls(
            rid=api.rid,
            author_rid=api.author_rid,
            content=api.content,
            created_at=_SecondsNanos.from_flexible(api.created_at).to_nanoseconds(),
        )
