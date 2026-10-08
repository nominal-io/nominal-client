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
```{include} /guides/_snippets/api-key.md
```

## Using the API key

```{include} /guides/_snippets/python/auth.md
```
