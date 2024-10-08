from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Iterable, Mapping, Sequence, cast

from typing_extensions import Self

from .._api.combined import scout_run_api
from ..ts import IntegralNanosecondsUTC, _SecondsNanos
from ._clientsbunch import ClientsBunch
from ._utils import HasRid, rid_from_instance_or_string, update_dataclass
from .attachment import Attachment, _iter_get_attachments
from .dataset import Dataset, _get_datasets
from .log import LogSet


@dataclass(frozen=True)
class Run(HasRid):
    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC | None
    _clients: ClientsBunch = field(repr=False)

    def add_dataset(self, ref_name: str, dataset: Dataset | str) -> None:
        """Add a dataset to this run.

        Datasets map "ref names" (their name within the run) to a Dataset (or dataset rid). The same type of datasets
        should use the same ref name across runs, since checklists and templates use ref names to reference datasets.
        """
        self.add_datasets({ref_name: dataset})

    def add_log_set(self, ref_name: str, log_set: LogSet | str) -> None:
        """Add a log set to this run.

        Log sets map "ref names" (their name within the run) to a Log set (or log set rid).
        """
        self.add_log_sets({ref_name: log_set})

    def add_log_sets(self, log_sets: Mapping[str, LogSet | str]) -> None:
        """Add multiple log sets to this run.

        Log sets map "ref names" (their name within the run) to a Log set (or log set rid).
        """
        data_sources = {
            ref_name: scout_run_api.CreateRunDataSource(
                data_source=scout_run_api.DataSource(log_set=rid_from_instance_or_string(log_set)),
                series_tags={},
                offset=None,
            )
            for ref_name, log_set in log_sets.items()
        }
        self._clients.run.add_data_sources_to_run(self._clients.auth_header, data_sources, self.rid)

    def add_datasets(self, datasets: Mapping[str, Dataset | str]) -> None:
        """Add multiple datasets to this run.

        Datasets map "ref names" (their name within the run) to a Dataset (or dataset rid). The same type of datasets
        should use the same ref name across runs, since checklists and templates use ref names to reference datasets.
        """
        # TODO(alkasm): support series tags & offset
        data_sources = {
            ref_name: scout_run_api.CreateRunDataSource(
                data_source=scout_run_api.DataSource(dataset=rid_from_instance_or_string(dataset)),
                series_tags={},
                offset=None,
            )
            for ref_name, dataset in datasets.items()
        }
        self._clients.run.add_data_sources_to_run(self._clients.auth_header, data_sources, self.rid)

    def _iter_list_datasets(self) -> Iterable[tuple[str, Dataset]]:
        run = self._clients.run.get_run(self._clients.auth_header, self.rid)
        dataset_rids_by_ref_name = {}
        for ref_name, source in run.data_sources.items():
            if source.data_source.type == "dataset":
                dataset_rid = cast(str, source.data_source.dataset)
                dataset_rids_by_ref_name[ref_name] = dataset_rid
        datasets_by_rids = {
            ds.rid: Dataset._from_conjure(self._clients, ds)
            for ds in _get_datasets(self._clients.auth_header, self._clients.catalog, dataset_rids_by_ref_name.values())
        }
        for ref_name, rid in dataset_rids_by_ref_name.items():
            dataset = datasets_by_rids[rid]
            yield (ref_name, dataset)

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List the datasets associated with this run.
        Returns (ref_name, dataset) pairs for each dataset.
        """
        return list(self._iter_list_datasets())

    def add_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Add attachments that have already been uploaded to this run.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_run_api.UpdateAttachmentsRequest(attachments_to_add=rids, attachments_to_remove=[])
        self._clients.run.update_run_attachment(self._clients.auth_header, request, self.rid)

    def remove_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Remove attachments from this run.
        Does not remove the attachments from Nominal.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_run_api.UpdateAttachmentsRequest(attachments_to_add=[], attachments_to_remove=rids)
        self._clients.run.update_run_attachment(self._clients.auth_header, request, self.rid)

    def _iter_list_attachments(self) -> Iterable[Attachment]:
        run = self._clients.run.get_run(self._clients.auth_header, self.rid)
        return _iter_get_attachments(self._clients, run.attachments)

    def list_attachments(self) -> Sequence[Attachment]:
        return list(self._iter_list_attachments())

    def archive(self) -> None:
        """Archive this run.
        Archived runs are not deleted, but are hidden from the UI.
        """
        self._clients.run.archive_run(self._clients.auth_header, self.rid)

    def update(
        self,
        *,
        name: str | None = None,
        start: datetime | IntegralNanosecondsUTC | None = None,
        end: datetime | IntegralNanosecondsUTC | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace run metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

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
        )
        response = self._clients.run.update_run(self._clients.auth_header, request, self.rid)
        run = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, run, fields=self.__dataclass_fields__)
        return self

    @classmethod
    def _from_conjure(cls, clients: ClientsBunch, run: scout_run_api.Run) -> Self:
        return cls(
            rid=run.rid,
            name=run.title,
            description=run.description,
            properties=MappingProxyType(run.properties),
            labels=tuple(run.labels),
            start=_SecondsNanos.from_scout_run_api(run.start_time).to_nanoseconds(),
            end=(_SecondsNanos.from_scout_run_api(run.end_time).to_nanoseconds() if run.end_time else None),
            _clients=clients,
        )
