from __future__ import annotations

from typing import Any, Iterable, Protocol, Sequence, TypeVar, overload

from nominal_api import (
    api,
    authentication_api,
    event,
    scout,
    scout_asset_api,
    scout_assets,
    scout_catalog,
    scout_checklistexecution_api,
    scout_checks_api,
    scout_datareview_api,
    scout_notebook_api,
    scout_run_api,
    scout_template_api,
    secrets_api,
)

DEFAULT_PAGE_SIZE = 100

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


def search_events_paginated(
    client: event.EventService,
    auth_header: str,
    query: event.SearchQuery,
) -> Iterable[event.Event]:
    def factory(page_token: str | None) -> event.SearchEventsRequest:
        return event.SearchEventsRequest(
            page_size=DEFAULT_PAGE_SIZE,
            query=query,
            sort=event.SortOptions(
                field=event.SortField.START_TIME,
                is_descending=True,
            ),
            next_page_token=page_token,
        )

    for response in paginate_rpc(client.search_events, auth_header, request_factory=factory):
        yield from response.results


def search_datasets_paginated(
    client: scout_catalog.CatalogService, auth_header: str, query: scout_catalog.SearchDatasetsQuery
) -> Iterable[scout_catalog.EnrichedDataset]:
    def factory(page_token: str | None) -> scout_catalog.SearchDatasetsRequest:
        return scout_catalog.SearchDatasetsRequest(
            page_size=DEFAULT_PAGE_SIZE,
            query=query,
            sort_options=scout_catalog.SortOptions(
                field=scout_catalog.SortField.INGEST_DATE,
                is_descending=True,
            ),
            token=page_token,
        )

    for response in paginate_rpc(client.search_datasets, auth_header, request_factory=factory):
        yield from response.results


def search_assets_paginated(
    client: scout_assets.AssetService,
    auth_header: str,
    query: scout_asset_api.SearchAssetsQuery,
) -> Iterable[scout_asset_api.Asset]:
    def factory(page_token: str | None) -> scout_asset_api.SearchAssetsRequest:
        return scout_asset_api.SearchAssetsRequest(
            page_size=DEFAULT_PAGE_SIZE,
            query=query,
            sort=scout_asset_api.AssetSortOptions(
                field=scout_asset_api.SortField.CREATED_AT,
                is_descending=True,
            ),
            next_page_token=page_token,
        )

    for response in paginate_rpc(client.search_assets, auth_header, request_factory=factory):
        yield from response.results


def search_data_reviews_paginated(
    datareview: scout_datareview_api.DataReviewService,
    auth_header: str,
    assets: Sequence[str] | None = None,
    runs: Sequence[str] | None = None,
) -> Iterable[scout_datareview_api.DataReview]:
    """Search for any data reviews present within a collection of runs and assets."""

    def factory(page_token: str | None) -> scout_datareview_api.FindDataReviewsRequest:
        return scout_datareview_api.FindDataReviewsRequest(
            asset_rids=[] if assets is None else list(assets),
            checklist_refs=[],
            run_rids=[] if runs is None else list(runs),
            archived_statuses=[api.ArchivedStatus.NOT_ARCHIVED],
            page_size=DEFAULT_PAGE_SIZE,
            next_page_token=page_token,
        )

    for response in paginate_rpc(datareview.find_data_reviews, auth_header, request_factory=factory):
        yield from response.data_reviews


def list_streaming_checklists_paginated(
    checklist_execution: scout_checklistexecution_api.ChecklistExecutionService, auth_header: str
) -> Iterable[str]:
    def factory(page_token: str | None) -> scout_checklistexecution_api.ListStreamingChecklistRequest:
        return scout_checklistexecution_api.ListStreamingChecklistRequest(
            workspaces=[],
            page_size=DEFAULT_PAGE_SIZE,
            page_token=page_token,
        )

    for response in paginate_rpc(checklist_execution.list_streaming_checklist, auth_header, request_factory=factory):
        yield from response.checklists


def list_streaming_checklists_for_asset_paginated(
    checklist_execution: scout_checklistexecution_api.ChecklistExecutionService, auth_header: str, asset: str
) -> Iterable[str]:
    def factory(page_token: str | None) -> scout_checklistexecution_api.ListStreamingChecklistForAssetRequest:
        return scout_checklistexecution_api.ListStreamingChecklistForAssetRequest(
            asset_rid=asset,
            page_size=DEFAULT_PAGE_SIZE,
            page_token=page_token,
        )

    for response in paginate_rpc(
        checklist_execution.list_streaming_checklist_for_asset, auth_header, request_factory=factory
    ):
        yield from response.checklists


def search_checklists_paginated(
    checklist: scout_checks_api.ChecklistService, auth_header: str, query: scout_checks_api.ChecklistSearchQuery
) -> Iterable[scout_checks_api.VersionedChecklist]:
    archived_statuses = [api.ArchivedStatus.NOT_ARCHIVED]

    def factory(page_token: str | None) -> scout_checks_api.SearchChecklistsRequest:
        return scout_checks_api.SearchChecklistsRequest(
            query=query,
            archived_statuses=archived_statuses,
            page_size=DEFAULT_PAGE_SIZE,
            next_page_token=page_token,
        )

    for response in paginate_rpc(checklist.search, auth_header, request_factory=factory):
        yield from response.values


def search_runs_paginated(
    run: scout.RunService, auth_header: str, query: scout_run_api.SearchQuery
) -> Iterable[scout_run_api.Run]:
    def factory(page_token: str | None) -> scout_run_api.SearchRunsRequest:
        return scout_run_api.SearchRunsRequest(
            page_size=DEFAULT_PAGE_SIZE,
            query=query,
            sort=scout_run_api.SortOptions(
                field=scout_run_api.SortField.START_TIME,
                is_descending=True,
            ),
            next_page_token=page_token,
        )

    for response in paginate_rpc(run.search_runs, auth_header, request_factory=factory):
        yield from response.results


def search_secrets_paginated(
    secrets: secrets_api.SecretService, auth_header: str, query: secrets_api.SearchSecretsQuery
) -> Iterable[secrets_api.Secret]:
    def factory(page_token: str | None) -> secrets_api.SearchSecretsRequest:
        return secrets_api.SearchSecretsRequest(
            page_size=DEFAULT_PAGE_SIZE,
            query=query,
            sort=secrets_api.SortOptions(field=secrets_api.SortField.CREATED_AT, is_descending=True),
            archived_statuses=[api.ArchivedStatus.NOT_ARCHIVED],
            token=page_token,
        )

    for response in paginate_rpc(secrets.search, auth_header, request_factory=factory):
        yield from response.results


def search_users_paginated(
    authentication: authentication_api.AuthenticationServiceV2,
    auth_header: str,
    query: authentication_api.SearchUsersQuery,
) -> Iterable[authentication_api.UserV2]:
    def factory(page_token: str | None) -> authentication_api.SearchUsersRequest:
        return authentication_api.SearchUsersRequest(
            page_size=DEFAULT_PAGE_SIZE,
            next_page_token=page_token,
            query=query,
            sort_by=authentication_api.SortBy(field=authentication_api.SortByField.EMAIL, is_descending=False),
        )

    for response in paginate_rpc(authentication.search_users_v2, auth_header, request_factory=factory):
        yield from response.results


def search_workbooks_paginated(
    workbook: scout.NotebookService,
    auth_header: str,
    query: scout_notebook_api.SearchNotebooksQuery,
    include_archived: bool,
) -> Iterable[scout_notebook_api.NotebookMetadataWithRid]:
    def factory(page_token: str | None) -> scout_notebook_api.SearchNotebooksRequest:
        return scout_notebook_api.SearchNotebooksRequest(
            query=query,
            show_drafts=False,
            show_archived=include_archived,
            next_page_token=page_token,
        )

    for response in paginate_rpc(workbook.search, auth_header, request_factory=factory):
        yield from response.results


def search_workbook_templates_paginated(
    template: scout.TemplateService,
    auth_header: str,
    query: scout_template_api.SearchTemplatesQuery,
) -> Iterable[scout_template_api.TemplateSummary]:
    def factory(page_token: str | None) -> scout_template_api.SearchTemplatesRequest:
        return scout_template_api.SearchTemplatesRequest(query=query, next_page_token=page_token)

    for response in paginate_rpc(template.search_templates, auth_header, request_factory=factory):
        yield from response.results


_TokenT = TypeVar("_TokenT")
_TokenT_co = TypeVar("_TokenT_co", covariant=True)
_TokenT_contra = TypeVar("_TokenT_contra", contravariant=True)

_RequestT = TypeVar("_RequestT")
_RequestT_co = TypeVar("_RequestT_co", covariant=True)
_RequestT_contra = TypeVar("_RequestT_contra", contravariant=True)

_ResponseT = TypeVar("_ResponseT")
_ResponseT_co = TypeVar("_ResponseT_co", covariant=True)
_ResponseT_contra = TypeVar("_ResponseT_contra", contravariant=True)


class _HasNextPageToken(Protocol):
    @property
    def next_page_token(self) -> str | None: ...


_DefaultResponseT = TypeVar("_DefaultResponseT", bound=_HasNextPageToken)


class _PaginatedRpc(Protocol[_RequestT_contra, _ResponseT_co]):
    def __call__(self, auth_header: str, _: _RequestT_contra, /) -> _ResponseT_co: ...


class _RequestFactory(Protocol[_TokenT_contra, _RequestT_co]):
    def __call__(self, page_token: _TokenT_contra | None, /) -> _RequestT_co: ...


class _TokenFactory(Protocol[_ResponseT_contra, _TokenT_co]):
    def __call__(self, response: _ResponseT_contra, /) -> _TokenT_co | None: ...


def _default_token_factory(response: _HasNextPageToken) -> str | None:
    return response.next_page_token


@overload
def paginate_rpc(
    rpc: _PaginatedRpc[_RequestT, _DefaultResponseT],
    auth_header: str,
    *,
    request_factory: _RequestFactory[str, _RequestT],
) -> Iterable[_DefaultResponseT]: ...


@overload
def paginate_rpc(
    rpc: _PaginatedRpc[_RequestT, _ResponseT],
    auth_header: str,
    *,
    request_factory: _RequestFactory[_TokenT, _RequestT],
    token_factory: _TokenFactory[_ResponseT, _TokenT],
) -> Iterable[_ResponseT]: ...


# Using Any because overloads provide strong type safety
def paginate_rpc(
    rpc: _PaginatedRpc[Any, Any],
    auth_header: str,
    *,
    request_factory: _RequestFactory[Any, Any],
    token_factory: _TokenFactory[Any, Any] = _default_token_factory,
) -> Iterable[Any]:
    next_page_token = None
    while True:
        request = request_factory(next_page_token)
        response = rpc(auth_header, request)
        yield response
        next_page_token = token_factory(response)
        if next_page_token is None:
            break
