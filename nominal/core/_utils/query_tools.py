from __future__ import annotations

from datetime import datetime
from typing import Iterable, Mapping, Sequence

from nominal_api import (
    api,
    authentication_api,
    event,
    scout_asset_api,
    scout_catalog,
    scout_checks_api,
    scout_notebook_api,
    scout_run_api,
    scout_template_api,
    secrets_api,
)

from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


def create_search_secrets_query(
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
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

    return secrets_api.SearchSecretsQuery(and_=queries)


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


def create_search_assets_query(
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> scout_asset_api.SearchAssetsQuery:
    queries = []
    if search_text is not None:
        queries.append(scout_asset_api.SearchAssetsQuery(search_text=search_text))
    if labels is not None:
        for label in labels:
            queries.append(scout_asset_api.SearchAssetsQuery(label=label))
    if properties:
        for name, value in properties.items():
            queries.append(scout_asset_api.SearchAssetsQuery(property=api.Property(name=name, value=value)))

    return scout_asset_api.SearchAssetsQuery(and_=queries)


def create_search_checklists_query(
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> scout_checks_api.ChecklistSearchQuery:
    queries = []
    if search_text is not None:
        queries.append(scout_checks_api.ChecklistSearchQuery(search_text=search_text))
    if labels is not None:
        for label in labels:
            queries.append(scout_checks_api.ChecklistSearchQuery(label=label))
    if properties is not None:
        for prop_key, prop_value in properties.items():
            queries.append(scout_checks_api.ChecklistSearchQuery(property=api.Property(prop_key, prop_value)))

    return scout_checks_api.ChecklistSearchQuery(and_=queries)


def create_search_datasets_query(
    exact_match: str | None = None,
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    ingested_before_inclusive: str | datetime | IntegralNanosecondsUTC | None = None,
    ingested_after_inclusive: str | datetime | IntegralNanosecondsUTC | None = None,
    workspace_id: str | None = None,
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

    if ingested_before_inclusive is not None:
        queries.append(
            scout_catalog.SearchDatasetsQuery(
                ingested_before_inclusive=_SecondsNanos.from_flexible(ingested_before_inclusive).to_iso8601()
            )
        )

    if ingested_after_inclusive is not None:
        queries.append(
            scout_catalog.SearchDatasetsQuery(
                ingested_after_inclusive=_SecondsNanos.from_flexible(ingested_after_inclusive).to_iso8601()
            )
        )

    if workspace_id is not None:
        queries.append(scout_catalog.SearchDatasetsQuery(workspace=workspace_id))

    return scout_catalog.SearchDatasetsQuery(and_=queries)


def create_search_events_query(
    search_text: str | None = None,
    after: str | datetime | IntegralNanosecondsUTC | None = None,
    before: str | datetime | IntegralNanosecondsUTC | None = None,
    assets: Iterable[str] | None = None,
    labels: Iterable[str] | None = None,
    properties: Mapping[str, str] | None = None,
    created_by: str | None = None,
) -> event.SearchQuery:
    queries = []
    if search_text is not None:
        queries.append(event.SearchQuery(search_text=search_text))
    if after is not None:
        queries.append(event.SearchQuery(after=_SecondsNanos.from_flexible(after).to_api()))
    if before is not None:
        queries.append(event.SearchQuery(before=_SecondsNanos.from_flexible(before).to_api()))
    if assets:
        for asset in assets:
            queries.append(event.SearchQuery(asset=asset))
    if labels:
        for label in labels:
            queries.append(event.SearchQuery(label=label))
    if properties:
        for name, value in properties.items():
            queries.append(event.SearchQuery(property=api.Property(name=name, value=value)))
    if created_by:
        queries.append(event.SearchQuery(created_by=created_by))

    return event.SearchQuery(and_=queries)


def create_search_runs_query(
    start: str | datetime | IntegralNanosecondsUTC | None = None,
    end: str | datetime | IntegralNanosecondsUTC | None = None,
    name_substring: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> scout_run_api.SearchQuery:
    queries = []
    if start is not None:
        start_time = _SecondsNanos.from_flexible(start).to_scout_run_api()
        queries.append(scout_run_api.SearchQuery(start_time_inclusive=start_time))
    if end is not None:
        end_time = _SecondsNanos.from_flexible(end).to_scout_run_api()
        queries.append(scout_run_api.SearchQuery(end_time_inclusive=end_time))
    if name_substring is not None:
        queries.append(scout_run_api.SearchQuery(exact_match=name_substring))
    if labels:
        for label in labels:
            queries.append(scout_run_api.SearchQuery(label=label))
    if properties:
        for name, value in properties.items():
            queries.append(scout_run_api.SearchQuery(property=api.Property(name=name, value=value)))

    return scout_run_api.SearchQuery(and_=queries)


def create_search_workbooks_query(
    exact_match: str | None = None,
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    asset_rid: str | None = None,
    exact_asset_rids: Sequence[str] | None = None,
    author_rid: str | None = None,
    run_rid: str | None = None,
) -> scout_notebook_api.SearchNotebooksQuery:
    queries = []

    if exact_match is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(exact_match=exact_match))

    if search_text is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(search_text=search_text))

    if asset_rid is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(asset_rid=asset_rid))

    if exact_asset_rids is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(exact_asset_rids=[*exact_asset_rids]))

    if author_rid is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(author_rid=author_rid))

    if run_rid is not None:
        queries.append(scout_notebook_api.SearchNotebooksQuery(run_rid=run_rid))

    if labels:
        for label in labels:
            queries.append(scout_notebook_api.SearchNotebooksQuery(label=label))

    if properties:
        for key, value in properties.items():
            queries.append(scout_notebook_api.SearchNotebooksQuery(property=api.Property(key, value)))

    return scout_notebook_api.SearchNotebooksQuery(and_=queries)


def create_search_workbook_templates_query(
    exact_match: str | None = None,
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    created_by: str | None = None,
) -> scout_template_api.SearchTemplatesQuery:
    queries = []

    if exact_match is not None:
        queries.append(scout_template_api.SearchTemplatesQuery(exact_match=exact_match))

    if search_text is not None:
        queries.append(scout_template_api.SearchTemplatesQuery(search_text=search_text))

    if created_by is not None:
        queries.append(scout_template_api.SearchTemplatesQuery(created_by=created_by))

    if labels:
        for label in labels:
            queries.append(scout_template_api.SearchTemplatesQuery(label=label))

    if properties:
        for key, value in properties.items():
            queries.append(scout_template_api.SearchTemplatesQuery(property=api.Property(key, value)))

    return scout_template_api.SearchTemplatesQuery(and_=queries)
