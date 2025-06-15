from __future__ import annotations

from datetime import datetime
from typing import Iterable, Mapping, Protocol, Sequence, TypeVar

from nominal_api import (
    api,
    authentication_api,
    event,
    scout,
    scout_asset_api,
    scout_assets,
    scout_checklistexecution_api,
    scout_checks_api,
    scout_datareview_api,
    scout_run_api,
    secrets_api,
)
from typing_extensions import TypeAlias

from nominal.core._utils import T_co, T_contra
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

DEFAULT_PAGE_SIZE = 100


Link: TypeAlias = tuple[str, str]


def create_links(links: Sequence[str] | Sequence[Link]) -> list[scout_run_api.Link]:
    links_conjure = []
    for link in links:
        if isinstance(link, tuple):
            url, title = link
            links_conjure.append(scout_run_api.Link(url=url, title=title))
        else:
            links_conjure.append(scout_run_api.Link(url=link))
    return links_conjure


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

    for response in _paginate(factory, client.search_events, auth_header):
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

    for response in _paginate(factory, client.search_assets, auth_header):
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

    for response in _paginate(factory, datareview.find_data_reviews, auth_header):
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

    for response in _paginate(factory, checklist_execution.list_streaming_checklist, auth_header):
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

    for response in _paginate(factory, checklist_execution.list_streaming_checklist_for_asset, auth_header):
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

    for response in _paginate(factory, checklist.search, auth_header):
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

    for response in _paginate(factory, run.search_runs, auth_header):
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

    for response in _paginate(factory, secrets.search, auth_header):
        yield from response.results


def search_users_paginated(
    authentication: authentication_api.AuthenticationServiceV2,
    auth_header: str,
    query: authentication_api.SearchUsersQuery,
) -> Iterable[authentication_api.UserV2]:
    def factory(page_token: str | None) -> authentication_api.SearchUsersRequest:
        return authentication_api.SearchUsersRequest(
            page_size=DEFAULT_PAGE_SIZE,
            query=query,
            sort_by=authentication_api.SortBy(field=authentication_api.SortByField.EMAIL, is_descending=False),
        )

    for response in _paginate(factory, authentication.search_users_v2, auth_header):
        yield from response.results


class _HasNextPageToken(Protocol):
    @property
    def next_page_token(self) -> str | None: ...


class _RequestFactory(Protocol[T_co]):
    """Creates a request by passing a positional page token.

    Note that we use a positional explicitly because the argument is inconsistently named
    `token`, `page_token`, or `next_page_token` in the API.
    """

    def __call__(self, _: str | None, /) -> T_co: ...


class _RPC(Protocol[T_contra, T_co]):
    """Invokes an RPC by passing a positional request."""

    def __call__(self, auth_header: str, _: T_contra, /) -> T_co: ...


_RequestT = TypeVar("_RequestT")
_ResponseT = TypeVar("_ResponseT", bound=_HasNextPageToken)


def _paginate(
    factory: _RequestFactory[_RequestT], rpc: _RPC[_RequestT, _ResponseT], auth_header: str
) -> Iterable[_ResponseT]:
    """Paginate through results of an RPC call that returns a next page token.

    Requests inconsistently use `next_page_token`, `page_token`, or `token`, so this function
    takes a factory function which, when called with a token, returns a request object.

    This function expects that the initial request uses `None` as the token to start pagination.

    All responses have a `next_page_token`, expected to be `None` when there are no more pages.
    """
    next_page_token = None
    while True:
        request = factory(next_page_token)
        response = rpc(auth_header, request)
        yield response
        if response.next_page_token is None:
            break
        next_page_token = response.next_page_token
