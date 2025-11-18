from __future__ import annotations

import abc
import importlib.metadata
import logging
import platform
import sys
from typing import Any, Generic, Mapping, Protocol, Sequence, TypeAlias, TypedDict, TypeVar, runtime_checkable

from nominal_api import scout_compute_api, scout_run_api
from typing_extensions import NotRequired, Self

from nominal._utils.dataclass_tools import update_dataclass

logger = logging.getLogger(__name__)

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


@runtime_checkable
class HasRid(Protocol):
    rid: str


class RefreshableMixin(Generic[T], abc.ABC):
    _clients: Any
    __dataclass_fields__: dict[str, Any]

    @classmethod
    @abc.abstractmethod
    def _from_conjure(cls, clients: Any, _: T) -> Self: ...

    @abc.abstractmethod
    def _get_latest_api(self) -> T: ...

    def _refresh_from_api(self, api_obj: T) -> Self:
        updated_obj = type(self)._from_conjure(self._clients, api_obj)
        update_dataclass(self, updated_obj, fields=self.__dataclass_fields__)
        return self

    def refresh(self) -> Self:
        return self._refresh_from_api(self._get_latest_api())


def rid_from_instance_or_string(value: HasRid | str) -> str:
    if isinstance(value, str):
        return value
    elif isinstance(value, HasRid):
        return value.rid
    raise TypeError(f"{value!r} is not a string nor an instance with a 'rid' attribute")


def construct_user_agent_string() -> str:
    """Constructs a user-agent string with system & Python metadata.
    E.g.: nominal-python/1.0.0b0 (macOS-14.4-arm64-arm-64bit) cpython/3.12.4
    """
    try:
        v = importlib.metadata.version("nominal")
        p = platform.platform()
        impl = sys.implementation
        py = platform.python_version()
        return f"nominal-python/{v} ({p}) {impl.name}/{py}"
    except Exception as e:
        # I believe all of the above are cross-platform, but just in-case...
        logger.error("failed to construct user-agent string", exc_info=e)
        return "nominal-python/unknown"


Link: TypeAlias = tuple[str, str]


class LinkDict(TypedDict):
    url: str
    title: NotRequired[str]


def create_links(links: Sequence[str | Link | LinkDict]) -> list[scout_run_api.Link]:
    links_conjure = []
    for link in links:
        if isinstance(link, tuple):
            url, title = link
            links_conjure.append(scout_run_api.Link(url=url, title=title))
        elif isinstance(link, dict):
            links_conjure.append(scout_run_api.Link(url=link["url"], title=link.get("title")))
        else:
            links_conjure.append(scout_run_api.Link(url=link))
    return links_conjure


def create_api_tags(tags: Mapping[str, str] | None = None) -> dict[str, scout_compute_api.StringConstant]:
    if not tags:
        return {}

    return {key: scout_compute_api.StringConstant(literal=value) for key, value in tags.items()}
