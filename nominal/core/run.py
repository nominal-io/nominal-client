from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Mapping, Sequence, cast

from typing_extensions import Self

from .._api.combined import scout_run_api
from .._utils import (
    IntegralNanosecondsUTC,
    _conjure_time_to_integral_nanoseconds,
    update_dataclass,
)
from ._utils import rid_from_instance_or_string
from .attachment import Attachment
from .client import NominalClient
from .dataset import Dataset


@dataclass(frozen=True)
class Run:
    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC | None
    _client: NominalClient = field(repr=False)

    def add_dataset(self, ref_name: str, dataset: Dataset | str) -> None:
        """Add a dataset to this run.

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
        }
        self._client._run_client.add_data_sources_to_run(self._client._auth_header, data_sources, self.rid)

    def _iter_list_datasets(self) -> Iterable[tuple[str, Dataset]]:
        run = self._client._run_client.get_run(self._client._auth_header, self.rid)
        dataset_rids_by_ref_name = {}
        for ref_name, source in run.data_sources.items():
            if source.data_source.type == "dataset":
                dataset_rid = cast(str, source.data_source.dataset)
                dataset_rids_by_ref_name[ref_name] = dataset_rid
        datasets_by_rids = {ds.rid: ds for ds in self._client.get_datasets(dataset_rids_by_ref_name.values())}
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
        self._client._run_client.update_run_attachment(self._client._auth_header, request, self.rid)

    def remove_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Remove attachments from this run.
        Does not remove the attachments from Nominal.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_run_api.UpdateAttachmentsRequest(attachments_to_add=[], attachments_to_remove=rids)
        self._client._run_client.update_run_attachment(self._client._auth_header, request, self.rid)

    def _iter_list_attachments(self) -> Iterable[Attachment]:
        run = self._client._run_client.get_run(self._client._auth_header, self.rid)
        return self._client.get_attachments(run.attachments)

    def list_attachments(self) -> Sequence[Attachment]:
        return list(self._iter_list_attachments())

    def update(
        self,
        *,
        name: str | None = None,
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
            title=name,
        )
        response = self._client._run_client.update_run(self._client._auth_header, request, self.rid)
        run = self.__class__._from_conjure(self._client, response)
        update_dataclass(self, run, fields=self.__dataclass_fields__)
        return self

    @classmethod
    def _from_conjure(cls, nominal_client: NominalClient, run: scout_run_api.Run) -> Self:
        return cls(
            rid=run.rid,
            name=run.title,
            description=run.description,
            properties=MappingProxyType(run.properties),
            labels=tuple(run.labels),
            start=_conjure_time_to_integral_nanoseconds(run.start_time),
            end=(_conjure_time_to_integral_nanoseconds(run.end_time) if run.end_time else None),
            _client=nominal_client,
        )
