from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Iterable, Mapping, Protocol, Sequence, cast

from nominal_api import (
    scout,
    scout_run_api,
)
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._conjure_utils import Link, create_links
from nominal.core._utils import HasRid, rid_from_instance_or_string, update_dataclass
from nominal.core.asset import Asset
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.data_scope_container import (
    ScopeType,
    ScopeTypeSpecifier,
    _DataScopeContainer,
)
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True)
class Run(HasRid, _DataScopeContainer):
    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC | None
    run_number: int
    assets: Sequence[str]

    _clients: _Clients = field(repr=False)

    class _Clients(
        Asset._Clients,
        _DataScopeContainer._Clients,
        HasScoutParams,
        Protocol,
    ):
        @property
        def run(self) -> scout.RunService: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Run in the Nominal app"""
        # TODO (drake): move logic into _from_conjure() factory function to accomodate different URL schemes
        return f"https://app.gov.nominal.io/runs/{self.run_number}"

    def update(
        self,
        *,
        name: str | None = None,
        start: datetime | IntegralNanosecondsUTC | None = None,
        end: datetime | IntegralNanosecondsUTC | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
        links: Sequence[str] | Sequence[Link] | None = None,
    ) -> Self:
        """Replace run metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Links can be URLs or tuples of (URL, name).

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in run.labels:
                new_labels.append(old_label)
            run = run.update(labels=new_labels)
        """
        request = scout_run_api.UpdateRunRequest(
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
            start_time=None if start is None else _SecondsNanos.from_flexible(start).to_scout_run_api(),
            end_time=None if end is None else _SecondsNanos.from_flexible(end).to_scout_run_api(),
            title=name,
            assets=[],
            links=None if links is None else create_links(links),
        )
        response = self._clients.run.update_run(self._clients.auth_header, request, self.rid)
        run = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, run, fields=self.__dataclass_fields__)
        return self

    def _add_data_scope(
        self,
        scope_name: str,
        scope: HasRid | str,
        scope_type: ScopeTypeSpecifier,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: timedelta | None = None,
    ) -> None:
        offset_duration = None
        if offset:
            seconds, nanos = divmod(offset.total_seconds(), 1)
            offset_duration = scout_run_api.Duration(nanos=int(nanos * 1e9), seconds=int(seconds))

        param_names = {"dataset": "dataset", "logset": "log_set", "connection": "connection", "video": "video"}
        datasource_args = {param_names[scope_type]: rid_from_instance_or_string(scope)}

        self._clients.run.add_data_sources_to_run(
            self._clients.auth_header,
            {
                scope_name: scout_run_api.CreateRunDataSource(
                    data_source=scout_run_api.DataSource(**datasource_args),
                    series_tags={**series_tags} if series_tags else {},
                    offset=offset_duration,
                )
            },
            self.rid,
        )

    def remove_data_scopes(
        self,
        *,
        names: Sequence[str] | None = None,
        scopes: Sequence[ScopeType | str] | None = None,
    ) -> None:
        ref_names = set(names or [])
        data_source_rids = set([rid_from_instance_or_string(ds) for ds in scopes] if scopes else [])

        conjure_run = self._clients.run.get_run(self._clients.auth_header, self.rid)

        data_sources_to_keep = {
            ref_name: scout_run_api.CreateRunDataSource(
                data_source=rds.data_source,
                series_tags=rds.series_tags,
                offset=rds.offset,
            )
            for ref_name, rds in conjure_run.data_sources.items()
            if ref_name not in ref_names
            and (rds.data_source.dataset or rds.data_source.connection or rds.data_source.video) not in data_source_rids
        }

        response = self._clients.run.update_run(
            self._clients.auth_header,
            scout_run_api.UpdateRunRequest(
                assets=[],
                data_sources=data_sources_to_keep,
            ),
            self.rid,
        )
        run = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, run, fields=self.__dataclass_fields__)

    def _rids_by_scope_name(self, stype: ScopeTypeSpecifier) -> Mapping[str, str]:
        enriched_run = self._clients.run.get_run(self._clients.auth_header, self.rid)
        rid_attrib = {"dataset": "dataset", "logset": "log_set", "connection": "connection", "video": "video"}
        return {
            ref_name: cast(str, getattr(source.data_source, rid_attrib[stype]))
            for ref_name, source in enriched_run.data_sources.items()
            if source.data_source.type.lower() == stype
        }

    def archive(self) -> None:
        self._clients.run.archive_run(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        self._clients.run.unarchive_run(self._clients.auth_header, self.rid)

    def add_attachment(self, attachment: Attachment | str) -> None:
        request = scout_run_api.UpdateAttachmentsRequest(
            attachments_to_add=[rid_from_instance_or_string(attachment)], attachments_to_remove=[]
        )
        self._clients.run.update_run_attachment(self._clients.auth_header, request, self.rid)

    def attachments(self) -> Iterable[Attachment]:
        run = self._clients.run.get_run(self._clients.auth_header, self.rid)
        for a in _iter_get_attachments(self._clients.auth_header, self._clients.attachment, run.attachments):
            yield Attachment._from_conjure(self._clients, a)

    def remove_attachments(self, attachments: Iterable[Attachment | str]) -> None:
        """Remove attachments from this run.
        Does not remove the attachments from Nominal.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_run_api.UpdateAttachmentsRequest(attachments_to_add=[], attachments_to_remove=rids)
        self._clients.run.update_run_attachment(self._clients.auth_header, request, self.rid)

    def _iter_list_assets(self) -> Iterable[Asset]:
        run = self._clients.run.get_run(self._clients.auth_header, self.rid)
        assets = self._clients.assets.get_assets(self._clients.auth_header, run.assets)
        for a in assets.values():
            yield Asset._from_conjure(self._clients, a)

    def list_assets(self) -> Sequence[Asset]:
        """List assets associated with this run."""
        return list(self._iter_list_assets())

    @classmethod
    def _from_conjure(cls, clients: _Clients, run: scout_run_api.Run) -> Self:
        return cls(
            rid=run.rid,
            name=run.title,
            description=run.description,
            properties=MappingProxyType(run.properties),
            labels=tuple(run.labels),
            start=_SecondsNanos.from_scout_run_api(run.start_time).to_nanoseconds(),
            end=(_SecondsNanos.from_scout_run_api(run.end_time).to_nanoseconds() if run.end_time else None),
            run_number=run.run_number,
            assets=tuple(run.assets),
            _clients=clients,
        )
