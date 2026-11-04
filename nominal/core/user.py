from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from nominal.core import _display
from nominal.core._utils.api_tools import HasRid
from nominal.protos.authentication.users.v1 import users_pb2


@dataclass(frozen=True)
class User(HasRid):
    """A Nominal user's identity and contact details."""

    rid: str
    display_name: str
    email: str

    @_display.card
    def _repr_html_(self) -> str | None:
        return _display.render(
            kind="User",
            title=self.display_name,
            fallback=repr(self),
            fields=[("RID", _display.identifier(self.rid)), ("Email", _display.escape(self.email))],
        )

    @classmethod
    def _from_proto(cls, raw_user: users_pb2.User) -> Self:
        return cls(rid=raw_user.rid, display_name=raw_user.display_name, email=raw_user.email)
