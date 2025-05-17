from __future__ import annotations

import click
from conjure_python_client import ConjureHTTPError

from nominal.core.client import NominalClient


def validate_token_url(token: str, base_url: str, workspace_rid: str | None) -> None:
    """Ensure the user sets a valid configuration before letting them import the client."""
    docs_link = "https://docs.nominal.io/python/api-tokens"
    status_code = 200
    err_msg = ""
    client = NominalClient.create(base_url, token)
    try:
        client.get_user()
    except ConjureHTTPError as err:
        status_code = err.response.status_code
        if status_code == 401:
            err_msg = f"The authorization token may be invalid. Read the docs on how to get a new token: {docs_link}"
        elif status_code == 404:
            err_msg = "The base_url may be incorrect. Ensure the url subdomain begins with 'api' (not 'app')."
        elif status_code != 200:
            err_msg = (
                f"There is likely a misconfiguration between the base_url and token. "
                f"Ensure the url subdomain begins with 'api' (not 'app'), "
                f"and create a new token: {docs_link} ({status_code})"
            )
    try:
        client.get_workspace(workspace_rid)
    except RuntimeError:
        err_msg = "Workspace not provided, but there is no default workspace for the user."
    except ConjureHTTPError as err:
        status_code = err.response.status_code
        if status_code == 404:
            err_msg = "The base_url may be incorrect. Ensure the url subdomain begins with 'api' (not 'app')."
        elif status_code != 200:
            err_msg = f"There is likely a misconfiguration; received {status_code=}. Contact support for help."
    if err_msg:
        click.secho(err_msg, err=True, fg="red")
        raise click.ClickException("Failed to authenticate. See above for details")
