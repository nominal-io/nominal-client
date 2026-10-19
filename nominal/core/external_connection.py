"""Typed configuration for dataset-backed external connections."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar, Literal, Mapping, Sequence, TypeAlias

from nominal_api import api as common
from nominal_api import scout_catalog
from nominal_api import scout_datasource_connection_api as api

from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core.secret import Secret


@dataclass(frozen=True)
class SecretHeader:
    """A header whose value is resolved from a stored secret."""

    secret: Secret | str


def _headers_to_conjure(headers: Mapping[str, str | SecretHeader]) -> dict[str, api.HeaderValue]:
    return {
        name: api.HeaderValue(plain=value)
        if isinstance(value, str)
        else api.HeaderValue(secret_rid=rid_from_instance_or_string(value.secret))
        for name, value in headers.items()
    }


def _headers_from_conjure(headers: Mapping[str, api.HeaderValue]) -> dict[str, str | SecretHeader]:
    result: dict[str, str | SecretHeader] = {}
    for name, value in headers.items():
        if value.plain is not None:
            result[name] = value.plain
        elif value.secret_rid is not None:
            result[name] = SecretHeader(value.secret_rid)
        # A variant unknown to this SDK is omitted from the read snapshot.
    return result


@dataclass(frozen=True)
class PasswordCredentials:
    """Username and stored password for an InfluxDB 1 connection."""

    username: str
    """Username used to authenticate to the external database."""
    password_secret: Secret | str
    """Password secret object or RID in the dataset workspace."""

    def _to_conjure(self) -> api.PasswordCredentials:
        return api.PasswordCredentials(
            username=self.username, password_secret_rid=rid_from_instance_or_string(self.password_secret)
        )

    @classmethod
    def _from_conjure(cls, raw: api.PasswordCredentials) -> PasswordCredentials:
        return cls(raw.username, raw.password_secret_rid)


@dataclass(frozen=True)
class TimescaleConnectionDetails:
    """Connection parameters for Timescale. Secrets are SDK secrets or RIDs."""

    provider_type: ClassVar[Literal["timescale"]] = "timescale"

    database: str
    """External database name."""
    host: str
    """External database hostname."""
    port: int
    """External database port."""
    username: str
    """Username used to authenticate to the external database."""
    password_secret: Secret | str
    """Password secret object or RID in the dataset workspace."""

    def _to_conjure(self) -> api.ConnectionDetails:
        return api.ConnectionDetails(
            timescale=api.TimescaleConnectionDetails(
                database=self.database,
                host=self.host,
                port=self.port,
                username=self.username,
                password_secret_rid=rid_from_instance_or_string(self.password_secret),
            )
        )

    @classmethod
    def _from_conjure(cls, raw: api.TimescaleConnectionDetails) -> TimescaleConnectionDetails:
        return cls(
            database=raw.database,
            host=raw.host,
            port=raw.port,
            username=raw.username,
            password_secret=raw.password_secret_rid,
        )


@dataclass(frozen=True)
class Influx2ConnectionDetails:
    """Connection parameters for Influx2. Secrets are SDK secrets or RIDs."""

    provider_type: ClassVar[Literal["influx"]] = "influx"

    host: str
    """External database hostname."""
    port: int
    """External database port."""
    org: str
    """InfluxDB organization identifier."""
    token_secret: Secret | str
    """API token secret object or RID in the dataset workspace."""
    headers: Mapping[str, str | SecretHeader] = field(default_factory=dict, kw_only=True)
    """Additional HTTP headers; strings are literal values and SecretHeader values resolve stored secrets."""

    def _to_conjure(self) -> api.ConnectionDetails:
        return api.ConnectionDetails(
            influx=api.Influx2ConnectionDetails(
                host=self.host,
                port=self.port,
                org=self.org,
                token_secret_rid=rid_from_instance_or_string(self.token_secret),
                headers=_headers_to_conjure(self.headers),
            )
        )

    @classmethod
    def _from_conjure(cls, raw: api.Influx2ConnectionDetails) -> Influx2ConnectionDetails:
        return cls(
            host=raw.host,
            port=raw.port,
            org=raw.org,
            token_secret=raw.token_secret_rid,
            headers=_headers_from_conjure(raw.headers),
        )


@dataclass(frozen=True)
class Influx1ConnectionDetails:
    """Connection parameters for Influx1. Secrets are SDK secrets or RIDs."""

    provider_type: ClassVar[Literal["influx1"]] = "influx1"

    host: str
    """External database hostname."""
    port: int
    """External database port."""
    headers: Mapping[str, str | SecretHeader] = field(default_factory=dict, kw_only=True)
    """Additional HTTP headers; strings are literal values and SecretHeader values resolve stored secrets."""
    credentials: PasswordCredentials | None = field(default=None, kw_only=True)
    """Optional username and password secret; None omits credentials."""

    def _to_conjure(self) -> api.ConnectionDetails:
        return api.ConnectionDetails(
            influx1=api.Influx1ConnectionDetails(
                host=self.host,
                port=self.port,
                headers=_headers_to_conjure(self.headers),
                credentials=self.credentials._to_conjure() if self.credentials is not None else None,
            )
        )

    @classmethod
    def _from_conjure(cls, raw: api.Influx1ConnectionDetails) -> Influx1ConnectionDetails:
        return cls(
            host=raw.host,
            port=raw.port,
            headers=_headers_from_conjure(raw.headers),
            credentials=PasswordCredentials._from_conjure(raw.credentials) if raw.credentials is not None else None,
        )


@dataclass(frozen=True)
class TimestreamConnectionDetails:
    """Connection parameters for Timestream. Secrets are SDK secrets or RIDs."""

    provider_type: ClassVar[Literal["timestream"]] = "timestream"

    nominal_role_arn: str
    """Nominal-side AWS role ARN used for access."""
    nominal_role_region: str
    """AWS region for the Nominal-side role."""
    external_region: str
    """AWS region of the external Timestream database."""
    external_role_arn: str
    """External AWS role ARN assumed to access Timestream."""
    database: str
    """External database name."""

    def _to_conjure(self) -> api.ConnectionDetails:
        return api.ConnectionDetails(
            timestream=api.TimestreamConnectionDetails(
                nominal_role_arn=self.nominal_role_arn,
                nominal_role_region=self.nominal_role_region,
                external_region=self.external_region,
                external_role_arn=self.external_role_arn,
                database=self.database,
            )
        )

    @classmethod
    def _from_conjure(cls, raw: api.TimestreamConnectionDetails) -> TimestreamConnectionDetails:
        return cls(
            nominal_role_arn=raw.nominal_role_arn,
            nominal_role_region=raw.nominal_role_region,
            external_region=raw.external_region,
            external_role_arn=raw.external_role_arn,
            database=raw.database,
        )


@dataclass(frozen=True)
class VisualCrossingConnectionDetails:
    """Connection parameters for VisualCrossing. Secrets are SDK secrets or RIDs."""

    provider_type: ClassVar[Literal["visualCrossing"]] = "visualCrossing"

    api_key_secret: Secret | str
    """API key secret object or RID in the dataset workspace."""

    def _to_conjure(self) -> api.ConnectionDetails:
        return api.ConnectionDetails(
            visual_crossing=api.VisualCrossingConnectionDetails(
                api_key_secret_rid=rid_from_instance_or_string(self.api_key_secret),
            )
        )

    @classmethod
    def _from_conjure(cls, raw: api.VisualCrossingConnectionDetails) -> VisualCrossingConnectionDetails:
        return cls(api_key_secret=raw.api_key_secret_rid)


@dataclass(frozen=True)
class BigQueryConnectionDetails:
    """Connection parameters for BigQuery. Secrets are SDK secrets or RIDs."""

    provider_type: ClassVar[Literal["bigQuery"]] = "bigQuery"

    region: str
    """BigQuery region."""
    project: str
    """BigQuery project identifier."""
    dataset: str
    """BigQuery dataset name."""
    table: str
    """BigQuery table name."""
    service_account_key_secret: Secret | str
    """Service account key secret object or RID in the dataset workspace."""

    def _to_conjure(self) -> api.ConnectionDetails:
        return api.ConnectionDetails(
            big_query=api.BigQueryConnectionDetails(
                region=self.region,
                project=self.project,
                dataset=self.dataset,
                table=self.table,
                service_account_key_secret_rid=rid_from_instance_or_string(self.service_account_key_secret),
            )
        )

    @classmethod
    def _from_conjure(cls, raw: api.BigQueryConnectionDetails) -> BigQueryConnectionDetails:
        return cls(
            region=raw.region,
            project=raw.project,
            dataset=raw.dataset,
            table=raw.table,
            service_account_key_secret=raw.service_account_key_secret_rid,
        )


@dataclass(frozen=True)
class ApiConnectionDetails:
    """Connection parameters for Api. Secrets are SDK secrets or RIDs."""

    provider_type: ClassVar[Literal["api"]] = "api"

    api_uri: str
    """Base URI of the external connection API service."""
    token_secret: Secret | str
    """API token secret object or RID in the dataset workspace."""

    def _to_conjure(self) -> api.ConnectionDetails:
        return api.ConnectionDetails(
            api=api.ApiConnectionDetails(
                api_uri=self.api_uri,
                token_secret_rid=rid_from_instance_or_string(self.token_secret),
            )
        )

    @classmethod
    def _from_conjure(cls, raw: api.ApiConnectionDetails) -> ApiConnectionDetails:
        return cls(api_uri=raw.api_uri, token_secret=raw.token_secret_rid)


ExternalConnectionDetails: TypeAlias = (
    TimescaleConnectionDetails
    | Influx2ConnectionDetails
    | Influx1ConnectionDetails
    | TimestreamConnectionDetails
    | VisualCrossingConnectionDetails
    | BigQueryConnectionDetails
    | ApiConnectionDetails
)


@dataclass(frozen=True)
class UnknownExternalConnectionDetails:
    """Read-only description of a provider unsupported by this SDK version."""

    provider_type: str


def _details_from_conjure(  # noqa: PLR0911 - explicit typed dispatch for each provider
    raw: api.ConnectionDetails,
) -> ExternalConnectionDetails | UnknownExternalConnectionDetails:
    if raw.timescale is not None:
        return TimescaleConnectionDetails._from_conjure(raw.timescale)
    if raw.influx is not None:
        return Influx2ConnectionDetails._from_conjure(raw.influx)
    if raw.influx1 is not None:
        return Influx1ConnectionDetails._from_conjure(raw.influx1)
    if raw.timestream is not None:
        return TimestreamConnectionDetails._from_conjure(raw.timestream)
    if raw.visual_crossing is not None:
        return VisualCrossingConnectionDetails._from_conjure(raw.visual_crossing)
    if raw.big_query is not None:
        return BigQueryConnectionDetails._from_conjure(raw.big_query)
    if raw.api is not None:
        return ApiConnectionDetails._from_conjure(raw.api)
    return UnknownExternalConnectionDetails(raw.type)


@dataclass(frozen=True)
class InfluxScrapingFilter:
    """Match buckets or measurements, optionally negating the match."""

    kind: Literal["bucket", "measurement"]
    values: Sequence[str]
    """Names matched by this filter."""
    negate: bool = field(default=False, kw_only=True)
    """Whether to exclude matching names instead of including them."""

    def _to_conjure(self) -> api.InfluxScrapingFilter:
        match = (
            api.InfluxScrapingFilter(match_bucket=list(self.values))
            if self.kind == "bucket"
            else api.InfluxScrapingFilter(match_measurement=list(self.values))
        )
        return api.InfluxScrapingFilter(not_=match) if self.negate else match


@dataclass(frozen=True)
class TimescaleScrapingFilter:
    """Match tables or schemas, optionally negating the match."""

    kind: Literal["table", "schema"]
    values: Sequence[str]
    """Names matched by this filter."""
    negate: bool = field(default=False, kw_only=True)
    """Whether to exclude matching names instead of including them."""

    def _to_conjure(self) -> api.TimescaleScrapingFilter:
        match = (
            api.TimescaleScrapingFilter(match_table=list(self.values))
            if self.kind == "table"
            else api.TimescaleScrapingFilter(match_schema=list(self.values))
        )
        return api.TimescaleScrapingFilter(not_=match) if self.negate else match


@dataclass(frozen=True)
class InfluxScrapingConfig:
    """Discover Influx series using ordered channel components and ANDed filters."""

    channel_name_components: Sequence[Literal["bucket", "measurement", "field"]]
    """Ordered components used to construct channel names."""
    separator: str
    """Separator between channel name components."""
    filters: Sequence[InfluxScrapingFilter] = field(default=(), kw_only=True)
    """Filters combined with AND; an empty sequence applies no filters."""

    def _to_conjure(self) -> api.ScrapingConfig:
        components = []
        for component in self.channel_name_components:
            if component == "bucket":
                components.append(api.InfluxChannelNameComponent(bucket=common.Empty()))
            elif component == "measurement":
                components.append(api.InfluxChannelNameComponent(measurement=common.Empty()))
            else:
                components.append(api.InfluxChannelNameComponent(field=common.Empty()))
        return api.ScrapingConfig(
            influx=api.InfluxScrapingConfig(
                filter=[value._to_conjure() for value in self.filters],
                series_archetype_construction=api.InfluxSeriesArchetypeConstructionConfig(
                    channel_name_components=components, separator=self.separator
                ),
            )
        )


@dataclass(frozen=True)
class TagValueComponent:
    """Use a tag's value in a Timestream channel name; omit it when absent."""

    tag_name: str


@dataclass(frozen=True)
class TimestreamScrapingConfig:
    """Discover Timestream measures in explicitly named tables."""

    channel_name_components: Sequence[Literal["table", "measure", "attribute"] | TagValueComponent]
    """Ordered components used to construct channel names."""
    separator: str
    """Separator between channel name components."""
    tables: Sequence[str] | None = field(default=None, kw_only=True)
    """None omits the table filter; an empty sequence sends an empty match.

    The backend requires at least one named table when scraping, so supply a nonempty sequence.
    """
    max_look_back_period_hours: int | None = field(default=None, kw_only=True)
    """Maximum discovery lookback in hours; None uses the backend default."""

    def _to_conjure(self) -> api.ScrapingConfig:
        components = []
        for component in self.channel_name_components:
            if isinstance(component, TagValueComponent):
                components.append(api.TimestreamChannelNameComponent(value_of_tag_with_name=component.tag_name))
            elif component == "table":
                components.append(api.TimestreamChannelNameComponent(table=common.Empty()))
            elif component == "measure":
                components.append(api.TimestreamChannelNameComponent(measure=common.Empty()))
            else:
                components.append(api.TimestreamChannelNameComponent(attribute=common.Empty()))
        return api.ScrapingConfig(
            timestream=api.TimestreamScrapingConfig(
                channel_name_components=components,
                separator=self.separator,
                filter=[] if self.tables is None else [api.TimestreamScrapingFilter(match_table=list(self.tables))],
                max_look_back_period_hours=self.max_look_back_period_hours,
            )
        )


@dataclass(frozen=True)
class TimescaleScrapingConfig:
    """Discover series in a pivoted Timescale table with time, name, and value columns."""

    time_column: str
    """Column containing timestamps."""
    name_column: str
    """Column containing the series name."""
    value_column: str
    """Column containing the series value."""
    dimension_columns: Sequence[str]
    """Columns interpreted as indexed dimensions."""
    channel_name_components: Sequence[Literal["table", "name"]]
    """Ordered components used to construct channel names."""
    separator: str
    """Separator between channel name components."""
    filters: Sequence[TimescaleScrapingFilter] = field(default=(), kw_only=True)
    """Filters combined with AND; an empty sequence applies no filters."""

    def _to_conjure(self) -> api.ScrapingConfig:
        components = [
            api.PivotedTimescaleChannelNameComponent(table=common.Empty())
            if value == "table"
            else api.PivotedTimescaleChannelNameComponent(name=common.Empty())
            for value in self.channel_name_components
        ]
        return api.ScrapingConfig(
            timescale=api.PivotedTimescaleScrapingConfig(
                time_column=self.time_column,
                name_column=self.name_column,
                value_column=self.value_column,
                dimension_columns=list(self.dimension_columns),
                channel_name_components=components,
                separator=self.separator,
                filter=[value._to_conjure() for value in self.filters],
            )
        )


@dataclass(frozen=True)
class BigQueryChannelNameComponents:
    """Select the identifiers prepended to each BigQuery column name."""

    include_project: bool
    include_dataset: bool
    include_table: bool


@dataclass(frozen=True)
class BigQueryScrapingConfig:
    """Identify timestamps, tags, and optional channel prefixes in BigQuery."""

    time_column: str
    """Column containing timestamps."""
    tag_columns: Sequence[str]
    """Columns interpreted as series tags."""
    channel_name_components: BigQueryChannelNameComponents | None = field(default=None, kw_only=True)
    """Optional prefix identifiers; None adds no project, dataset, or table prefix."""
    separator: str | None = field(default=None, kw_only=True)
    """Channel prefix separator; None uses the backend default, a period."""

    def _to_conjure(self) -> api.ScrapingConfig:
        components = self.channel_name_components
        return api.ScrapingConfig(
            big_query=api.BigQueryScrapingConfig(
                time_column=self.time_column,
                tag_columns=list(self.tag_columns),
                separator=self.separator,
                channel_name_components=None
                if components is None
                else api.BigQueryChannelNameComponent(
                    include_project=components.include_project,
                    include_dataset=components.include_dataset,
                    include_table=components.include_table,
                ),
            )
        )


@dataclass(frozen=True)
class VisualCrossingScrapingConfig:
    """Enable Visual Crossing discovery; its legacy series list is no longer used."""

    def _to_conjure(self) -> api.ScrapingConfig:
        return api.ScrapingConfig(visual_crossing=api.VisualCrossingScrapingConfig(available_series=[]))


@dataclass(frozen=True)
class ApiScrapingConfig:
    """Scrape all channels, or only allow/block the supplied channel names."""

    mode: Literal["all", "allow", "block"] = "all"
    """Discover all channels, only the allow list, or all except the block list."""
    channels: Sequence[str] = ()
    """Channel names for allow/block mode; these lists must be nonempty. Ignored in all mode."""

    def _to_conjure(self) -> api.ScrapingConfig:
        if self.mode == "all":
            if self.channels:
                raise ValueError("All-channel scraping does not accept a channel list")
            config = api.ApiScrapingConfig(all_channels=api.AllChannelsConnectionsScrapingConfig())
        elif self.mode == "allow":
            config = api.ApiScrapingConfig(
                channel_allow_list=api.ChannelAllowListConnectionsScrapingConfig(channel_allow_list=list(self.channels))
            )
        elif self.mode == "block":
            config = api.ApiScrapingConfig(
                channel_block_list=api.ChannelBlockListConnectionsScrapingConfig(channel_block_list=list(self.channels))
            )
        else:
            raise ValueError(f"Unsupported API scraping mode: {self.mode}")
        return api.ScrapingConfig(api=config)


ExternalScrapingConfig: TypeAlias = (
    InfluxScrapingConfig
    | TimestreamScrapingConfig
    | TimescaleScrapingConfig
    | BigQueryScrapingConfig
    | VisualCrossingScrapingConfig
    | ApiScrapingConfig
)


@dataclass(frozen=True)
class ExternalQueryRange:
    """Maximum query duration, retaining the backend's integral count and unit."""

    count: int
    """Integral number of duration units."""
    unit: str
    """HOURS, DAYS, or WEEKS for writes; reads preserve other decoded unit names."""

    def _to_conjure(self) -> api.Duration:
        if self.unit not in {"HOURS", "DAYS", "WEEKS"}:
            raise ValueError(f"Unsupported external query duration unit: {self.unit}")
        return api.Duration(count=self.count, unit=api.TemporalUnit(self.unit))


@dataclass(frozen=True)
class ExternalConnectionLimits:
    """Optional maximum range for an external query."""

    max_query_range: ExternalQueryRange | None = None
    """Maximum range of one query; None omits the limit."""

    def _to_conjure(self) -> api.LimitsConfig:
        return api.LimitsConfig(
            max_query_range=None if self.max_query_range is None else self.max_query_range._to_conjure()
        )

    @classmethod
    def _from_conjure(cls, raw: api.LimitsConfig) -> ExternalConnectionLimits:
        duration = raw.max_query_range
        if duration is None:
            return cls()
        return cls(ExternalQueryRange(duration.count, duration.unit.value))


@dataclass(frozen=True)
class ExternalConnectionConfig:
    """Configuration to register an external connection on an existing dataset.

    Both omitted and empty ``available_tag_values`` clear existing tag values
    when replacing configuration; subsequent scraping may discover values. Scraping runs only when both
    ``scraping`` and ``should_scrape=True`` are supplied.
    """

    details: ExternalConnectionDetails
    """Provider-specific connection parameters."""
    metadata: Mapping[str, str] = field(default_factory=dict, kw_only=True)
    """External configuration metadata; an empty mapping clears previous metadata."""
    required_tag_names: Sequence[str] = field(default=(), kw_only=True)
    """Additional tags required to fully qualify a series; empty clears previous requirements."""
    available_tag_values: Mapping[str, Sequence[str]] | None = field(default=None, kw_only=True)
    """Externally supplied tag values; both None and an empty mapping clear previous values."""
    scraping: ExternalScrapingConfig | None = field(default=None, kw_only=True)
    """Provider-specific discovery configuration; None clears previous scraping settings."""
    should_scrape: bool = field(default=False, kw_only=True)
    """Enable discovery only when scraping configuration is also supplied; defaults to False."""
    limits: ExternalConnectionLimits | None = field(default=None, kw_only=True)
    """External query limits; None means no limits in this snapshot or registration."""

    def _to_conjure(self, dataset_rid: str) -> scout_catalog.RegisterExternalConnectionConfigRequest:
        return scout_catalog.RegisterExternalConnectionConfigRequest(
            dataset_rid=dataset_rid,
            connection_details=self.details._to_conjure(),
            metadata=dict(self.metadata),
            required_tag_names=list(self.required_tag_names),
            should_scrape=self.should_scrape,
            available_tag_values=None
            if self.available_tag_values is None
            else {name: list(values) for name, values in self.available_tag_values.items()},
            scraping=None if self.scraping is None else self.scraping._to_conjure(),
            limits=None if self.limits is None else self.limits._to_conjure(),
        )


@dataclass(frozen=True)
class ExternalConnection:
    """External connection metadata returned with a dataset, without remote lifecycle methods."""

    details: ExternalConnectionDetails | UnknownExternalConnectionDetails
    """Provider-specific connection parameters."""
    is_connected: bool
    """Last reported status; initial True after registration is not a connectivity check."""
    limits: ExternalConnectionLimits | None
    """External query limits; None means no limits in this snapshot or registration."""

    @property
    def provider_type(self) -> str:
        """Backend provider identifier, such as ``influx`` or ``bigQuery``."""
        return self.details.provider_type

    @classmethod
    def _from_conjure(cls, raw: scout_catalog.ExternalConnectionMetadata) -> ExternalConnection:
        return cls(
            details=_details_from_conjure(raw.connection_details),
            is_connected=raw.connection_status.is_connected,
            limits=None if raw.limits is None else ExternalConnectionLimits._from_conjure(raw.limits),
        )
