"""
Utils related to token and endpoint management

Example:

import nominal as nm
nm.cloud.set_token(...)
nm.cloud.set_base_url('STAGING')
"""

import os

import keyring as kr
from rich import print

ENDPOINTS = dict(
    file_upload="{}/upload/v1/upload-file?fileName={}",
    dataset_upload="{}/ingest/v1/trigger-ingest-v2",
    run_upload="{}/ingest/v1/ingest-run",
    run_retrieve="{}/scout/v1/run/{}",  # GET
    run_update="{}/scout/v1/run/{}",  # PUT
)

BASE_URLS = dict(
    STAGING="https://api-staging.gov.nominal.io/api",
    PROD="https://api.gov.nominal.io/api",
)


def _auth_help_blurb():
    print("\nUnauthorized - you likely need to set or update your API token")
    print("Get your API access token from [link]{0}/sandbox[/link]".format(get_app_base_url()))
    print("Then set your access token with:")
    print("[code]import nominal as nm; nm.cloud.set_token('YOUR TOKEN HERE')[/code]\n")


def set_token(token):
    if token is None:
        print("Retrieve your access token from [link]{0}/sandbox[/link]".format(get_base_url()))
    kr.set_password("Nominal API", "python-client", token)


def set_base_url(base_url: str = "STAGING"):
    """
    Usage:
    import nominal as nm
    nm.cloud.set_base_url('PROD')
    """
    if base_url in BASE_URLS:
        os.environ["NOMINAL_BASE_URL"] = BASE_URLS[base_url]
    else:
        os.environ["NOMINAL_BASE_URL"] = base_url


def get_base_url():
    if "NOMINAL_BASE_URL" not in os.environ:
        set_base_url()  # set to default
    return os.environ["NOMINAL_BASE_URL"]


def get_app_base_url():
    """
    eg, https://app-staging.gov.nominal.io

    TODO
    ----
    This won't work for custom domains
    """
    return get_base_url().rstrip("/api").replace("api", "app")
