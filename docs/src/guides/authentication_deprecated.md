---
myst:
  html_meta:
    description: "How to send secure requests to Nominal's APIs and SDKs"
---

# Authenticate to Nominal

{.lead}
How to send secure requests to Nominal's APIs and SDKs

This page covers how to secure and authorize requests to Nominal's APIs and SDKs. If your scripts use the legacy API, using `import nominal as nm`, see [Migration to the Profile API](/guides/profile-migration.md).

## Generating an API key
Your API key secures and authorizes all project requests to Nominal. API keys are specific to a single organization.

To get an API key:

1. Go to your profile's **API keys** page in **Settings**:

    ```{button-link} https://app.gov.nominal.io/settings/org/api-keys
    :color: primary

    Go to API keys page
    ```
1. Click **Create key** and complete the following:

    * Add your API key name.
    * Set an expiration for your API key.
    * Click **Generate key** and copy the key value. You won't be able to see it again. Store your API key securely to protect your Nominal data.

## Using the API key

```{include} /guides/_snippets/python/auth.md
```
