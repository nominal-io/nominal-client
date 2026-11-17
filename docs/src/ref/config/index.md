# Config and profiles

A profile is a named Nominal API key and URL in the config file, `~/.config/nominal/config.yml`.
Create one with `nom config profile add`, or from Python:

```python
import nominal.config

nominal.config.make_profile("staging", token="nominal_api_key_...", base_url="https://api-staging.gov.nominal.io/api")
nominal.config.list_profiles()            # {"default": "https://api.gov.nominal.io/api", "staging": ...}
nominal.config.use_profile("staging")     # for the rest of this Python session
client = nominal.config.get_client()      # a NominalClient on the active profile
```

## Which profile is active

`get_client()` and `df.nom.to_nominal()` use the first of these that is set:

1. `nominal.config.use_profile(name)`, in this Python session
2. the `NOMINAL_PROFILE` environment variable (`PANOMINAL_PROFILE` is still honored)
3. `"default"`

`NOMINAL_WORKSPACE_RID`, if set, picks the workspace instead of the profile's own.

`NominalClient.from_profile(name)` is unchanged: it always uses exactly the profile you give it.

## API Reference

```{eval-rst}
.. include:: ../_summaries/nominal.config.rst
```
