from __future__ import annotations

from typing import Iterable, Mapping, Sequence

from nominal_api import (
    api,
    authentication_api,
    event,
    ingest_api,
    scout_api,
    scout_asset_api,
    scout_catalog,
    scout_checks_api,
    scout_notebook_api,
    scout_rids_api,
    scout_run_api,
    scout_template_api,
    scout_video_api,
    secrets_api,
)

from nominal.ts import _InferrableTimestampType, _SecondsNanos


def _property_filters(properties: Mapping[str, str]) -> list[scout_rids_api.PropertiesFilter]:
    """One PropertiesFilter per key-value pair, each with a single-element values list."""
    return [scout_rids_api.PropertiesFilter(name=name, values=[value]) for name, value in properties.items()]


def _labels_filter(
    labels: Iterable[str],
    *,
    operator: api.SetOperator = api.SetOperator.AND,
) -> scout_rids_api.LabelsFilter:
    """Build a LabelsFilter. Defaults to AND (entity must have all labels); pass OR for any-of semantics."""
    return scout_rids_api.LabelsFilter(labels=list(labels), operator=operator)


def _to_api_ts(ts: _InferrableTimestampType | None) -> api.Timestamp | None:
    return None if ts is None else _SecondsNanos.from_flexible(ts).to_api()


def _to_run_ts(ts: _InferrableTimestampType | None) -> scout_run_api.UtcTimestamp | None:
    return None if ts is None else _SecondsNanos.from_flexible(ts).to_scout_run_api()


def _to_catalog_ts(ts: _InferrableTimestampType | None) -> scout_catalog.UtcTimestamp | None:
    return None if ts is None else _SecondsNanos.from_flexible(ts).to_scout_catalog()


def _to_iso8601_ts(ts: _InferrableTimestampType | None) -> str | None:
    return None if ts is None else _SecondsNanos.from_flexible(ts).to_iso8601()


def _run_timeframe_filter(
    start: _InferrableTimestampType | None,
    end: _InferrableTimestampType | None,
) -> scout_run_api.TimeframeFilter | None:
    """Build a run TimeframeFilter from flexible start/end bounds. Returns None when both are absent."""
    if start is None and end is None:
        return None
    return scout_run_api.TimeframeFilter(
        custom=scout_run_api.CustomTimeframeFilter(
            start_time=_to_run_ts(start),
            end_time=_to_run_ts(end),
        )
    )


def create_search_secrets_query(
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    workspace_rid: str | None = None,
) -> secrets_api.SearchSecretsQuery:
    queries = []
    if search_text is not None:
        queries.append(secrets_api.SearchSecretsQuery(search_text=search_text))
    if labels is not None:
        for label in labels:
            queries.append(secrets_api.SearchSecretsQuery(label=label))
    if properties is not None:
        for name, value in properties.items():
            queries.append(secrets_api.SearchSecretsQuery(property=api.Property(name=name, value=value)))
    if workspace_rid is not None:
        queries.append(secrets_api.SearchSecretsQuery(workspace=workspace_rid))
    return secrets_api.SearchSecretsQuery(and_=queries)


def create_search_videos_query(
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    workspace_rid: str | None = None,
) -> scout_video_api.SearchVideosQuery:
    queries = []
    if search_text is not None:
        queries.append(scout_video_api.SearchVideosQuery(search_text=search_text))
    if labels is not None:
        for label in labels:
            queries.append(scout_video_api.SearchVideosQuery(label=label))
    if properties is not None:
        for name, value in properties.items():
            queries.append(scout_video_api.SearchVideosQuery(property=api.Property(name=name, value=value)))
    if workspace_rid is not None:
        queries.append(scout_video_api.SearchVideosQuery(workspace=workspace_rid))
    return scout_video_api.SearchVideosQuery(and_=queries)


def create_search_users_query(
    exact_match: str | None = None,
    search_text: str | None = None,
) -> authentication_api.SearchUsersQuery:
    queries = []
    if exact_match is not None:
        queries.append(authentication_api.SearchUsersQuery(exact_match=exact_match))
    if search_text is not None:
        queries.append(authentication_api.SearchUsersQuery(search_text=search_text))

    return authentication_api.SearchUsersQuery(and_=queries)


def create_search_containerized_extractors_query(
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    workspace_rid: str | None = None,
) -> ingest_api.SearchContainerizedExtractorsQuery:
    queries = []
    if search_text is not None:
        queries.append(ingest_api.SearchContainerizedExtractorsQuery(search_text=search_text))

    if workspace_rid is not None:
        queries.append(ingest_api.SearchContainerizedExtractorsQuery(workspace=workspace_rid))

    if labels is not None:
        for label in labels:
            queries.append(ingest_api.SearchContainerizedExtractorsQuery(label=label))

    if properties is not None:
        for name, value in properties.items():
            queries.append(ingest_api.SearchContainerizedExtractorsQuery(property=api.Property(name=name, value=value)))

    return ingest_api.SearchContainerizedExtractorsQuery(and_=queries)


def create_search_assets_query(
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    exact_substring: str | None = None,
    workspace_rid: str | None = None,
    archived: bool | None = None,
) -> scout_asset_api.SearchAssetsQuery:
    # TODO (drake): add support for labels_any_of
    queries = []
    if search_text is not None:
        queries.append(scout_asset_api.SearchAssetsQuery(search_text=search_text))
    if exact_substring is not None:
        queries.append(scout_asset_api.SearchAssetsQuery(exact_substring=exact_substring))
    if labels is not None:
        queries.append(scout_asset_api.SearchAssetsQuery(labels=_labels_filter(labels)))
    if properties:
        for f in _property_filters(properties):
            queries.append(scout_asset_api.SearchAssetsQuery(properties=f))
    if workspace_rid is not None:
        queries.append(scout_asset_api.SearchAssetsQuery(workspace=workspace_rid))
    if archived is not None:
        queries.append(scout_asset_api.SearchAssetsQuery(archived=archived))

    return scout_asset_api.SearchAssetsQuery(and_=queries)


def create_search_checklists_query(
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    author: str | None = None,
    assignee: str | None = None,
    workspace_rid: str | None = None,
    archived: bool | None = None,
    author_rid_any_of: Sequence[str] | None = None,
) -> scout_checks_api.ChecklistSearchQuery:
    # TODO(drake): add support for labels_any_of
    queries = [scout_checks_api.ChecklistSearchQuery(is_published=True)]
    if search_text is not None:
        queries.append(scout_checks_api.ChecklistSearchQuery(search_text=search_text))
    if labels is not None:
        queries.append(scout_checks_api.ChecklistSearchQuery(labels=_labels_filter(labels)))
    if properties:
        for f in _property_filters(properties):
            queries.append(scout_checks_api.ChecklistSearchQuery(properties=f))
    if author is not None:
        queries.append(scout_checks_api.ChecklistSearchQuery(author_rid=author))
    if assignee is not None:
        queries.append(scout_checks_api.ChecklistSearchQuery(assignee_rid=assignee))
    if workspace_rid is not None:
        queries.append(scout_checks_api.ChecklistSearchQuery(workspace=workspace_rid))
    if archived is not None:
        queries.append(scout_checks_api.ChecklistSearchQuery(is_archived=archived))
    if author_rid_any_of is not None:
        queries.append(scout_checks_api.ChecklistSearchQuery(author_rids=list(author_rid_any_of)))
    return scout_checks_api.ChecklistSearchQuery(and_=queries)


def create_search_dataset_files_query(
    start: _InferrableTimestampType | None = None,
    end: _InferrableTimestampType | None = None,
    file_tags: Mapping[str, str] | None = None,
) -> scout_catalog.SearchDatasetFilesQuery:
    queries = []
    if start is not None or end is not None:
        queries.append(
            scout_catalog.SearchDatasetFilesQuery(
                time_range=scout_catalog.TimeRangeFilter(start=_to_catalog_ts(start), end=_to_catalog_ts(end))
            )
        )
    if file_tags is not None:
        queries.append(scout_catalog.SearchDatasetFilesQuery(file_tags=dict(file_tags)))
    return scout_catalog.SearchDatasetFilesQuery(and_=queries)


def create_search_datasets_query(
    exact_match: str | None = None,
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    ingested_before_inclusive: _InferrableTimestampType | None = None,
    ingested_after_inclusive: _InferrableTimestampType | None = None,
    workspace_rid: str | None = None,
    archived: bool | None = None,
) -> scout_catalog.SearchDatasetsQuery:
    queries = []
    if search_text is not None:
        queries.append(scout_catalog.SearchDatasetsQuery(search_text=search_text))

    if exact_match is not None:
        queries.append(scout_catalog.SearchDatasetsQuery(exact_match=exact_match))

    if labels is not None:
        for label in labels:
            queries.append(scout_catalog.SearchDatasetsQuery(label=label))

    if properties is not None:
        for prop_key, prop_value in properties.items():
            queries.append(scout_catalog.SearchDatasetsQuery(properties=api.Property(prop_key, prop_value)))

    if (ts := _to_iso8601_ts(ingested_before_inclusive)) is not None:
        queries.append(scout_catalog.SearchDatasetsQuery(ingested_before_inclusive=ts))

    if (ts := _to_iso8601_ts(ingested_after_inclusive)) is not None:
        queries.append(scout_catalog.SearchDatasetsQuery(ingested_after_inclusive=ts))

    if workspace_rid is not None:
        queries.append(scout_catalog.SearchDatasetsQuery(workspace=workspace_rid))

    if archived is not None:
        queries.append(scout_catalog.SearchDatasetsQuery(archive_status=archived))

    return scout_catalog.SearchDatasetsQuery(and_=queries)


def create_search_runs_query(  # noqa: PLR0912
    start: _InferrableTimestampType | None = None,
    end: _InferrableTimestampType | None = None,
    name_substring: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    exact_match: str | None = None,
    search_text: str | None = None,
    created_after: _InferrableTimestampType | None = None,
    created_before: _InferrableTimestampType | None = None,
    workspace_rid: str | None = None,
    asset_rids: Sequence[str] | None = None,
    has_single_asset: bool | None = None,
    run_number: int | None = None,
    archived: bool | None = None,
) -> scout_run_api.SearchQuery:
    # TODO(drake): allow searching by datasets, check alert status, and datasources by tags
    queries = []
    if (tf := _run_timeframe_filter(start, end)) is not None:
        queries.append(scout_run_api.SearchQuery(end_time=tf))
    if (tf := _run_timeframe_filter(created_after, created_before)) is not None:
        queries.append(scout_run_api.SearchQuery(created_at=tf))
    if name_substring is not None:
        queries.append(scout_run_api.SearchQuery(exact_match=name_substring))
    if labels:
        queries.append(scout_run_api.SearchQuery(labels=_labels_filter(labels)))
    if properties:
        for f in _property_filters(properties):
            queries.append(scout_run_api.SearchQuery(properties=f))
    if exact_match is not None:
        queries.append(scout_run_api.SearchQuery(exact_match=exact_match))
    if search_text is not None:
        queries.append(scout_run_api.SearchQuery(search_text=search_text))
    if workspace_rid is not None:
        queries.append(scout_run_api.SearchQuery(workspace=workspace_rid))
    if asset_rids:
        queries.append(scout_run_api.SearchQuery(assets=scout_run_api.AssetsFilter(assets=list(asset_rids))))
    if has_single_asset is not None:
        queries.append(scout_run_api.SearchQuery(is_single_asset=has_single_asset))
    if run_number is not None:
        queries.append(scout_run_api.SearchQuery(run_number=run_number))
    if archived is not None:
        queries.append(scout_run_api.SearchQuery(archived=archived))
    return scout_run_api.SearchQuery(and_=queries)


def create_search_workbooks_query(  # noqa: PLR0912
    exact_match: str | None = None,
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    asset_rid: str | None = None,
    exact_asset_rids: Sequence[str] | None = None,
    created_by_rid: str | None = None,
    run_rid: str | None = None,
    workspace_rid: str | None = None,
    archived: bool | None = None,
    created_by_rid_any_of: Sequence[str] | None = None,
    run_rid_any_of: Sequence[str] | None = None,
    workbook_types: Sequence[scout_notebook_api.NotebookType] | None = None,
) -> scout_notebook_api.SearchNotebooksQuery:
    # TODO(drake): support asset_any_of, consolidate naming of `created_by` vs. `author`, multiple authors
    queries = []

    if exact_match is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(exact_match=exact_match))

    if search_text is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(search_text=search_text))

    if asset_rid is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(asset_rid=asset_rid))

    if exact_asset_rids is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(exact_asset_rids=[*exact_asset_rids]))

    if created_by_rid is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(author_rid=created_by_rid))

    if run_rid is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(run_rid=run_rid))

    if labels:
        queries.append(scout_notebook_api.SearchNotebooksQuery(labels=_labels_filter(labels)))

    if properties:
        for f in _property_filters(properties):
            queries.append(scout_notebook_api.SearchNotebooksQuery(properties=f))

    if workspace_rid is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(workspace=workspace_rid))

    if archived is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(archived=archived))

    if created_by_rid_any_of is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(author_rids=list(created_by_rid_any_of)))

    if run_rid_any_of is not None:
        run_filter = scout_notebook_api.RunsFilter(operator=api.SetOperator.OR, runs=list(run_rid_any_of))
        queries.append(scout_notebook_api.SearchNotebooksQuery(run_rids=run_filter))

    if workbook_types is not None:
        notebook_type_filter = scout_notebook_api.NotebookTypesFilter(types=list(workbook_types))
        queries.append(scout_notebook_api.SearchNotebooksQuery(notebook_types=notebook_type_filter))

    return scout_notebook_api.SearchNotebooksQuery(and_=queries)


def create_search_workbook_templates_query(
    exact_match: str | None = None,
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    created_by: str | None = None,
    archived: bool | None = None,
    published: bool | None = None,
    workspace_rid: str | None = None,
    created_by_rid_any_of: Sequence[str] | None = None,
) -> scout_template_api.SearchTemplatesQuery:
    # TODO(drake): add support for label_any_of
    queries = []

    if exact_match is not None:
        queries.append(scout_template_api.SearchTemplatesQuery(exact_match=exact_match))

    if search_text is not None:
        queries.append(scout_template_api.SearchTemplatesQuery(search_text=search_text))

    if created_by is not None:
        queries.append(scout_template_api.SearchTemplatesQuery(created_by=created_by))

    if labels:
        queries.append(scout_template_api.SearchTemplatesQuery(labels=_labels_filter(labels)))

    if properties:
        for f in _property_filters(properties):
            queries.append(scout_template_api.SearchTemplatesQuery(properties=f))

    if archived is not None:
        queries.append(scout_template_api.SearchTemplatesQuery(is_archived=archived))

    if published is not None:
        queries.append(scout_template_api.SearchTemplatesQuery(is_published=published))

    if workspace_rid is not None:
        queries.append(scout_template_api.SearchTemplatesQuery(workspace=workspace_rid))

    if created_by_rid_any_of is not None:
        queries.append(scout_template_api.SearchTemplatesQuery(author_rids=list(created_by_rid_any_of)))

    return scout_template_api.SearchTemplatesQuery(and_=queries)


def _create_search_events_query(  # noqa: PLR0912, PLR0915
    search_text: str | None = None,
    after: _InferrableTimestampType | None = None,
    before: _InferrableTimestampType | None = None,
    asset_rids: Iterable[str] | None = None,
    labels: Iterable[str] | None = None,
    properties: Mapping[str, str] | None = None,
    created_by_rid: str | None = None,
    workbook_rid: str | None = None,
    data_review_rid: str | None = None,
    assignee_rid: str | None = None,
    event_type: event.EventType | None = None,
    origin_types: Iterable[event.SearchEventOriginType] | None = None,
    workspace_rid: str | None = None,
    archived: bool | None = None,
    priorities: Iterable[scout_api.Priority] | None = None,
    assignee_rid_any_of: Iterable[str] | None = None,
    event_type_any_of: Iterable[event.EventType] | None = None,
    created_by_rid_any_of: Iterable[str] | None = None,
) -> event.SearchQuery:
    queries = []
    if search_text is not None:
        queries.append(event.SearchQuery(search_text=search_text))
    if (ts := _to_api_ts(after)) is not None:
        queries.append(event.SearchQuery(after=ts))
    if (ts := _to_api_ts(before)) is not None:
        queries.append(event.SearchQuery(before=ts))
    if asset_rids:
        queries.append(event.SearchQuery(assets=event.AssetsFilter([*asset_rids], api.SetOperator.AND)))
    if labels:
        queries.append(event.SearchQuery(labels=_labels_filter(labels)))
    if properties:
        for f in _property_filters(properties):
            queries.append(event.SearchQuery(properties=f))
    if created_by_rid:
        queries.append(event.SearchQuery(created_by=created_by_rid))
    if workbook_rid is not None:
        queries.append(event.SearchQuery(workbook=workbook_rid))
    if data_review_rid is not None:
        queries.append(event.SearchQuery(data_review=data_review_rid))
    if assignee_rid is not None:
        queries.append(event.SearchQuery(assignee=assignee_rid))
    if event_type is not None:
        queries.append(event.SearchQuery(event_type=event_type))
    if origin_types is not None:
        origin_type_filter = event.OriginTypesFilter(api.SetOperator.OR, list(origin_types))
        queries.append(event.SearchQuery(origin_types=origin_type_filter))
    if workspace_rid is not None:
        queries.append(event.SearchQuery(workspace=workspace_rid))
    if archived is not None:
        queries.append(event.SearchQuery(archived=archived))
    if priorities is not None:
        queries.append(event.SearchQuery(priorities=list(priorities)))
    if assignee_rid_any_of is not None:
        assignees_filter = event.AssigneesFilter(assignees=list(assignee_rid_any_of), operator=api.SetOperator.OR)
        queries.append(event.SearchQuery(assignees=assignees_filter))
    if event_type_any_of is not None:
        queries.append(event.SearchQuery(event_types=list(event_type_any_of)))
    if created_by_rid_any_of is not None:
        queries.append(event.SearchQuery(created_by_any_of=list(created_by_rid_any_of)))

    return event.SearchQuery(and_=queries)
