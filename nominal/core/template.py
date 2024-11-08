from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from nominal._api.combined import (
    scout,
    scout_template_api
)
from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid


@dataclass(frozen=True)
class Template(HasRid):
    rid: str
    charts: any
    layout: any
    content: any
    _clients: _Clients = field(repr=False)

    class _Clients(HasAuthHeader, Protocol):
        @property
        def template(self) -> scout.TemplateService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, template: scout_template_api.Template) -> Template:
        return cls(
            rid=template.rid,
            charts=template.charts,
            layout=template.layout,
            content=template.content,
            _clients=clients,
        )
