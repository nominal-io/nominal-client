from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from nominal_api import comments_api
from typing_extensions import Self


@dataclass(frozen=True)
class Message:
    """A message posted to a resource's discussion."""

    rid: str
    author_rid: str
    content: str
    created_at: datetime

    @classmethod
    def _from_conjure(cls, api: comments_api.Comment) -> Self:
        return cls(
            rid=api.rid,
            author_rid=api.author_rid,
            content=api.content,
            created_at=_parse_iso8601(api.created_at),
        )


def _parse_iso8601(value: str) -> datetime:
    # conjure datetimes are ISO-8601; Python <3.11 doesn't accept the trailing 'Z'.
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
