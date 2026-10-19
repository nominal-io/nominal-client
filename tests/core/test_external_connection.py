import pytest
from nominal_api import scout_catalog
from nominal_api import scout_datasource_connection_api as api

from nominal.core.external_connection import (
    ApiConnectionDetails,
    ApiScrapingConfig,
    BigQueryConnectionDetails,
    ExternalConnection,
    ExternalConnectionConfig,
    ExternalConnectionLimits,
    ExternalQueryRange,
    Influx1ConnectionDetails,
    Influx2ConnectionDetails,
    InfluxScrapingFilter,
    PasswordCredentials,
    SecretHeader,
    TagValueComponent,
    TimescaleConnectionDetails,
    TimestreamConnectionDetails,
    TimestreamScrapingConfig,
    UnknownExternalConnectionDetails,
    VisualCrossingConnectionDetails,
)


def test_headers_distinguish_plain_values_from_secret_references():
    """A literal header resembling a RID remains literal while secret headers round-trip."""
    details = Influx1ConnectionDetails(
        "localhost", 8086, headers={"plain": "ri.secret", "secret": SecretHeader("ri.secret")}
    )
    raw = details._to_conjure()
    assert raw.influx1 is not None
    assert raw.influx1.headers["plain"].plain == "ri.secret"
    assert raw.influx1.headers["secret"].secret_rid == "ri.secret"
    snapshot = ExternalConnection._from_conjure(
        scout_catalog.ExternalConnectionMetadata(
            connection_details=raw, connection_status=api.ConnectionStatus(is_connected=False)
        )
    )
    assert snapshot.details == details
    assert not snapshot.is_connected


def test_registration_preserves_omitted_and_empty_available_tags():
    """Omitted and explicitly empty tags retain their distinct transport presence."""
    details = ApiConnectionDetails("https://example.com", "ri.secret")
    assert ExternalConnectionConfig(details)._to_conjure("ri.dataset").available_tag_values is None
    request = ExternalConnectionConfig(details, available_tag_values={}, scraping=ApiScrapingConfig())._to_conjure(
        "ri.dataset"
    )
    assert request.available_tag_values == {}
    assert request.scraping is not None and request.scraping.api is not None
    assert request.scraping.api.all_channels is not None


def test_external_limits_preserve_explicit_empty_and_integral_units():
    """Empty limits and integral week durations round-trip without coercion."""
    empty = ExternalConnectionLimits()
    assert ExternalConnectionLimits._from_conjure(empty._to_conjure()) == empty
    weekly = ExternalConnectionLimits(ExternalQueryRange(3, "WEEKS"))
    assert ExternalConnectionLimits._from_conjure(weekly._to_conjure()) == weekly


def test_negated_filters_and_tag_components_preserve_union_cases():
    """Filter negation and named tag components select the correct provider union cases."""
    raw = InfluxScrapingFilter("measurement", ["temperature"], negate=True)._to_conjure()
    assert raw.not_ is not None and raw.not_.match_measurement == ["temperature"]
    scraping = TimestreamScrapingConfig(
        ["table", TagValueComponent("vehicle"), "measure"], ".", tables=[]
    )._to_conjure()
    assert scraping.timestream is not None
    assert scraping.timestream.filter[0].match_table == []
    assert [component.type for component in scraping.timestream.channel_name_components] == [
        "table",
        "valueOfTagWithName",
        "measure",
    ]
    assert scraping.timestream.channel_name_components[1].value_of_tag_with_name == "vehicle"


def test_all_existing_provider_details_roundtrip_without_native_connection():
    """Each supported external provider round-trips its credentials and endpoint fields."""
    providers = [
        ApiConnectionDetails("https://example.com", "ri.secret"),
        BigQueryConnectionDetails("us-east1", "project", "dataset", "table", "ri.secret"),
        Influx1ConnectionDetails("localhost", 8086, credentials=PasswordCredentials("user", "ri.secret")),
        Influx2ConnectionDetails("localhost", 8086, "org", "ri.secret"),
        TimescaleConnectionDetails("database", "localhost", 5432, "user", "ri.secret"),
        TimestreamConnectionDetails("nominal-role", "us-east-1", "us-east-2", "external-role", "database"),
        VisualCrossingConnectionDetails("ri.secret"),
    ]
    for details in providers:
        snapshot = ExternalConnection._from_conjure(
            scout_catalog.ExternalConnectionMetadata(
                connection_details=details._to_conjure(), connection_status=api.ConnectionStatus(is_connected=True)
            )
        )
        assert snapshot.details == details
        assert snapshot.is_connected
        assert snapshot.provider_type == details.provider_type


def test_native_provider_metadata_is_readable_but_has_no_external_write_conversion():
    """Legacy native metadata is inspectable without enabling native registration."""
    snapshot = ExternalConnection._from_conjure(
        scout_catalog.ExternalConnectionMetadata(
            connection_details=api.ConnectionDetails(
                nominal=api.NominalConnectionDetails(nominal_data_source_rid="ri.dataset")
            ),
            connection_status=api.ConnectionStatus(is_connected=False),
        )
    )
    assert snapshot.details == UnknownExternalConnectionDetails("nominal")


def test_unknown_query_unit_cannot_be_written_back():
    """Read-only unknown duration units cannot accidentally change registration semantics."""
    with pytest.raises(ValueError, match="Unsupported external query duration unit"):
        ExternalQueryRange(2, "UNKNOWN")._to_conjure()


def test_unrecognized_header_variant_is_omitted_from_read_snapshot():
    """A header with neither supported payload does not prevent reading other provider details."""
    raw = api.Influx1ConnectionDetails(
        host="localhost",
        port=8086,
        headers={"future": api.HeaderValue(type_of_union="future"), "known": api.HeaderValue(plain="value")},
    )
    details = Influx1ConnectionDetails._from_conjure(raw)
    assert details.headers == {"known": "value"}
