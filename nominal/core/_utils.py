from typing import Protocol, runtime_checkable


@runtime_checkable
class HasRid(Protocol):
    rid: str


def rid_from_instance_or_string(value: HasRid | str) -> str:
    if isinstance(value, str):
        return value
    elif isinstance(value, HasRid):
        return value.rid
    raise TypeError("{value!r} is not a string nor an instance with a 'rid' attribute")
