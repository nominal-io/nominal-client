'''
Utils related to user identity and API auth

Example:

import nominal as nm
nm.auth.set_token(...)
'''

import keyring as kr
from ..nominal import get_base_url

def set_token(token):
    if token is None:
        print("Retrieve your access token from [link]{0}/sandbox[/link]".format(get_base_url()))
    kr.set_password("Nominal API", "python-client", token)
