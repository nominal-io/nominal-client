from nominal.core._clientsbunch import api_base_url_to_app_base_url


def test_api_app_url_conversion():
    c = api_base_url_to_app_base_url
    assert c("https://api.gov.nominal.io/api") == "https://app.gov.nominal.io"
    assert c("https://api-staging.gov.nominal.io/api") == "https://app-staging.gov.nominal.io"
    assert c("https://api.nominal.test") == "https://app.nominal.test"
    assert c("https://api-customer.eu.nominal.io/api") == "https://app-customer.eu.nominal.io"
    assert c("https://api-customer.gov.nominal.io/api") == "https://app-customer.gov.nominal.io"
    assert c("https://api.nominal.gov.deployment.customer.com/api") == "https://app.nominal.gov.deployment.customer.com"
    assert c("https://api.nominal.customer.internal/api") == "https://app.nominal.customer.internal"
    assert c("https://unknown") == ""
