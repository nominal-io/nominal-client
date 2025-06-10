from __future__ import annotations

from dataclasses import dataclass

from nominal_api import authentication_api
from typing_extensions import Self

from nominal.core._utils import HasRid


@dataclass(frozen=True)
class User(HasRid):
    rid: str
    display_name: str
    email: str

    @classmethod
    def _from_conjure(cls, raw_user: authentication_api.UserV2) -> Self:
        return cls(rid=raw_user.rid, display_name=raw_user.display_name, email=raw_user.email)
