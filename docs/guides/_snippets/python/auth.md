The Nominal Client authenticates two ways: with a profile stored on disk, or with a token passed directly.

::::{dropdown} Storing credentials to disk

Run the following in a terminal and follow on-screen prompts to set up a connection profile:

```shell
nom config profile add default -t <api-token>
```

Alternatively, if `nom` is missing from the path:

```shell
python -m nominal.cli config profile add default
```

Here, "default" can be any name chosen to represent this profile (reminder: a profile represents a base URL, API key, and workspace).

If you have more than one workspace, you must specify the workspace rid as follows:

```shell
nom config profile add default -t <api-token> -w <workspace-rid>
```

You can get the workspace rid by selecting the home button, and clicking on workspace settings. From there, you can hover over the workspace you wish to use and click on the three dots icon, after which you can copy the workspace rid to your clipboard.

If you are connecting to a self-hosted Nominal stack, you must also specify the API URL with `-u`:

```shell
nom config profile add default -t <api-token> -u <https://api.nominal.companyname.com/api>
```

The profile will be stored in `~/.config/nominal/config.yml`, and can then be used to create a client:

```{literalinclude} /guides/_snippets/code/getting_started/auth_1.py
:language: python
```

:::{warning}

If you previously used `nom` to store credentials before profiles existed, migrate your old configuration file (`~/.nominal.yml`) to the new format (`~/.config/nominal/config.yml`).

You can do this with the following command:

```shell
nom config migrate
```

Or, if `nom` is missing from your path:

```shell
python -m nominal.cli config migrate
```
:::
::::

::::{dropdown} Directly using credentials in your scripts

```{literalinclude} /guides/_snippets/code/getting_started/auth_2.py
:language: python
```

:::{warning}

**NOTE**: you should never share your Nominal API key with anyone.
Nominal recommends that you **not** save it in your code and/or scripts.

* If you trust the computer you are on, use `nom` to store the credential to disk.
* Otherwise, use a password manager such as [1password](https://1password.com/) or [bitwarden](https://bitwarden.com/) to keep your token safe.
:::
::::
