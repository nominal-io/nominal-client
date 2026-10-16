# External connections

An external connection belongs to a dataset. Nominal discovers channels and queries the external
provider using that dataset's configuration. For data streamed directly into Nominal, create an
ordinary dataset and use its write stream instead; no external configuration is needed.

Create an empty dataset, then attach a complete external configuration. Store credentials in a
Nominal secret in the same workspace as the dataset. Configuration accepts secret objects or RIDs,
not credential values.

```python
from nominal.core import NominalClient
from nominal.core.external_connection import (
    ApiConnectionDetails,
    ApiScrapingConfig,
    ExternalConnectionConfig,
)

client = NominalClient.from_profile("default")
dataset = client.create_dataset("External telemetry")
dataset.register_external_connection(
    ExternalConnectionConfig(
        details=ApiConnectionDetails(
            api_uri="https://telemetry.example.com",
            token_secret="ri.secrets.main.secret.example",
        ),
        scraping=ApiScrapingConfig(mode="all"),
        should_scrape=True,
    )
)

# The dataset can be used in the same places as other datasets.
run = client.get_run("ri.scout.main.run.example")
run.add_dataset("telemetry", dataset)
```

The example enables background channel discovery. Registration returns before
channel discovery or a query completes. Initial connected status is not proof that credentials or
network access work. Verify discovery and a query before relying on the integration.

## Supported providers

| Provider | Connection details | Scraping configuration |
| --- | --- | --- |
| External API service | `ApiConnectionDetails` | `ApiScrapingConfig` |
| BigQuery | `BigQueryConnectionDetails` | `BigQueryScrapingConfig` |
| InfluxDB 2 | `Influx2ConnectionDetails` | `InfluxScrapingConfig` |
| InfluxDB 1 | `Influx1ConnectionDetails` | No discovery implementation in the current backend |
| Timescale | `TimescaleConnectionDetails` | `TimescaleScrapingConfig` |
| Timestream | `TimestreamConnectionDetails` | `TimestreamScrapingConfig` |
| Visual Crossing | `VisualCrossingConnectionDetails` | `VisualCrossingScrapingConfig` |

See the {doc}`external connection reference <../../sdk/external-connections>` for provider fields,
filters, naming components, and query limits.

## Replacing configuration

`Dataset.register_external_connection` replaces the complete external configuration. It does not
patch individual fields: empty collections clear metadata and tag configuration, and omitted
scraping and query limits clear their previous values. Supply the complete intended configuration
on every call. Existing connection status and archive state are preserved by the backend.

The method refreshes the same `Dataset` instance from the response. `dataset.external_connection`
contains provider details, the last reported connection status, and query limits. It is `None` for
ordinary datasets. This snapshot does not include all registration fields, so it cannot be used to
reconstruct the full previous configuration for a read-modify-write update.

Creating a dataset and registering configuration are two operations. If registration fails, the
dataset remains available; correct the configuration and retry registration on that dataset.
