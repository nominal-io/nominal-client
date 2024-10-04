from dataclasses import dataclass


@dataclass(frozen=True)
class User:
    rid: str
    display_name: str
    email: str
