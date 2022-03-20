"""Google Credentials

Lightweight helper functions for easily going through all steps of the OAuth flow process
for accessing Google APIs. All functions return authenticated and authorized
credentials.

References
----------
route1.io blog post on creating OAuth 2.0 credentials:
    https://www.route1.io/configuring-oauth-2-0-access-to-the-google-apis/
Google's example on credential access:
    https://developers.google.com/docs/api/quickstart/python
To create credentials:
    https://console.cloud.google.com/apis/credentials
To enable APIs:
    https://console.cloud.google.com/apis/library
Available OAuth 2.0 scope URL endpoints:
    https://developers.google.com/identity/protocols/oauth2/scopes

Examples
--------
Generally the use case is for automating tasks that require accessing Google APIs
such as Sheets or Drive. Developers should first create an app on Google Cloud Platform,
enable relevant APIs, create OAuth 2.0 credentials, and then download those credentials
to their local machine.

Sample code for getting credentials for Google Drive API:

SCOPES = ["https://www.googleapis.com/auth/drive"]
CLIENT_SECRETS_FPATH = "/path/to/client/secrets.json"
TOKEN_FPATH = "/path/to/save/token.json"
creds = get_token_from_full_auth_flow(TOKEN_FPATH, CLIENT_SECRETS_FPATH, SCOPES)
"""

from typing import List
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from ..utils import endpoints

def get_token_from_full_auth_flow(authorized_user_file: str,
                                  client_secrets_file: str, scopes: List[str],
                                  port: int = 0) -> "google.oauth2.credentials.Credentials":
    """Return authorized and authenticated credentials for accessing Google APIs.
    If refresh token hasn't yet been generated or is invalid, this function will
    open a user consent screen and then save the credentials that are returned.

    Reference code sample: https://developers.google.com/docs/api/quickstart/python

    Parameters
    ----------
    authorized_user_file : str
        Filepath to token JSON file. If file does not exist then this becomes
        the filepath the token will be dumped to for future use after going
        through the user consent screen.
    client_secrets_file : str
        Filepath to client secrets file downloaded from GCP after creating credentials
    scopes : List[str]
        Enabled APIs that we want our app to have access to
    port : int
        Port to open user consent screen on

    Returns
    -------
    creds : google.oauth2.credentials.Credentials
        Authenticated and authorized credentials for accessing Google API
    """
    creds = None
    if os.path.exists(authorized_user_file):
        creds = refresh_token_from_authorized_user_file(authorized_user_file=authorized_user_file)

    if creds is None or not creds.valid:
        expired_but_has_refresh_token = (creds is not None) and (creds.expired) and (creds.refresh_token)
        if expired_but_has_refresh_token:
            _refresh_credentials(creds=creds)
        else:
            creds = get_token_from_user_consent_screen(
                client_secrets_file=client_secrets_file,
                scopes=scopes,
                port=port,
                fpath=authorized_user_file
            )
    return creds

def get_token_from_user_consent_screen(client_secrets_file: str, scopes: List[str],
                                       port: int = 0, fpath: str = None) -> "google.oauth2.credentials.Credentials":
    """Return valid credentials after opening user consent screen authorizing
    the app to access scopes enabled for the app outlined in client secrets file.

    Parameters
    ----------
    client_secrets_file : str
        Filepath to client secrets file downloaded from OAuth 2.0 Client IDs
        on Google Cloud Platform after generating OAuth credentials
    scopes : List[str]
        Scopes of APIs that have been enabled on Google Cloud Platform
    port : int = 0
        Port to open user consent screen on
    fpath : str = None
        If specified, dumps the token to this filepath as a JSON

    Returns
    -------
    creds : google.oauth2.credentials.Credentials
        Authenticated and authorized credentials for accessing Google API
    """
    flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file=client_secrets_file, scopes=scopes)
    creds = flow.run_local_server(port=port)
    if fpath is not None:
        _save_credentials(creds=creds, fpath=fpath)
    return creds

def refresh_token_from_authorized_user_file(authorized_user_file: str):
    """Return valid credentials after refreshing from previously saved credentials.

    authorized_user_file : str
        Filepath to a file containing refresh token and various credentials
        acquired after user consented to scopes.

    Returns
    -------
    creds : google.oauth2.credentials.Credentials
        Authenticated and authorized credentials for accessing Google API
    """
    creds = Credentials.from_authorized_user_file(filename=authorized_user_file)
    if creds.expired:
        _refresh_credentials(creds=creds)
    return creds

def refresh_token_from_credentials(refresh_token: str,
                                   client_id: str, client_secret: str,
                                   scopes: List[str] = None) -> "google.oauth2.credentials.Credentials":
    """Return valid credentials refreshed from explicitly passed refresh token,
    client ID, and client secret

    Parameters
    ----------
    refresh_token : str
        Valid refresh token
    client_id : str
        Client ID acquired from creating credentials in APIs & Services on GCP
    client_secret : str
        Client secret acquired from creating credentials in APIs & Services on GCP
    scopes : List[str] = None
        Optional scopes to pass. This has no bearing on the token refresh but it's
        a good idea to explicitly set what scopes we have access to to keep track
        of permissions.

    Returns
    -------
    creds : google.oauth2.credentials.Credentials
        Authenticated and authorized credentials for accessing Google API
    """
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        token_uri=endpoints.GOOGLE_TOKEN_ENDPOINT,
        scopes=scopes
    )
    _refresh_credentials(creds=creds)
    return creds

def _save_credentials(creds: "google.oauth2.credentials.Credentials", fpath: str) -> None:
    """Save credentials to JSON file at fpath"""
    with open(fpath, "w") as outjson:
            outjson.write(creds.to_json())

def _refresh_credentials(creds: "google.oauth2.credentials.Credentials") -> None:
    """Return credentials object in place"""
    creds.refresh(Request())