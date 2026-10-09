from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import Self

from nominal.core._utils.api_tools import HasRid
from nominal.protos.authentication.users.v1 import users_pb2


@dataclass(frozen=True)
class User(HasRid):
    rid: str
    display_name: str
    email: str

    @classmethod
    def _from_proto(cls, raw_user: users_pb2.User) -> Self:
        return cls(rid=raw_user.rid, display_name=raw_user.display_name, email=raw_user.email)
