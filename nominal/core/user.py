from dataclasses import dataclass

from ._utils import HasRid


@dataclass(frozen=True)
class User(HasRid):
    rid: str
    display_name: str
    email: str
