# `nom` CLI

The `nom` command is installed with the Python SDK:

```shell
pip install nominal
```

Add a connection profile using your API token:

```shell
nom config profile add default -t "<API_TOKEN>"
```

This stores the profile in `~/.config/nominal/config.yml` and validates the connection.
Use `-w "<WORKSPACE_RID>"` to select a workspace, and `-u "<API_BASE_URL>"`
to connect to a different deployment. See [Networking & TLS](../networking-tls.md)
for corporate certificate and local-deployment configuration.

If `nom` is unavailable on your shell's path, use the same interpreter that has the SDK installed:

```shell
python -m nominal.cli config profile add default -t "<API_TOKEN>"
```

Use the stored profile from Python:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
```

The commands and options below are generated from the CLI.

```{eval-rst}
.. click:: nominal.cli:nom
   :prog: nom
   :nested: full
```
