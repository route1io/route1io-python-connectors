"""Google Credentials

Helper functions for easily going through all steps of the OAuth flow process
for accessing Google APIs.
"""

import json
from typing import List

import requests

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from ..utils import endpoints

def get_token_from_client_secrets_file(client_secrets_file: str, scopes: List[str],
                                       port: int = 0) -> "google.oath2.credentials.Credentials":
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

    Return
    ------
    creds : "google.oath2.credentials.Credentials"
    """
    flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file=client_secrets_file, scopes=scopes)
    creds = flow.run_local_server(port=port)
    return creds

def refresh_token_from_authorized_user_file(authorized_user_file: str):
    """Return valid credentials after refreshing from previously saved credentials.

    authorized_user_file : str
        Filepath to a file containing refresh token and various credentials
        acquired after user consented to scopes.
    """
    creds = Credentials.from_authorized_user_file('token.json')
    if creds.expired:
        creds.refresh(Request())
    return creds

def get_google_credentials(refresh_token: str, cid: str, csc: str) -> "google.oath2.credentials.Credentials":
    """Return a Credentials object containing refreshed access token

    Parameters
    ----------
    refresh_token : str
        Valid refresh token for getting a new access token
    cid : str
        Client ID from GCP console
    csc : str
        Client secret from GCP console

    Returns
    -------
    creds : google.oath2.credentials.Credentials
        Valid access credentials for accessing Google API
    """
    data = {
        "refresh_token": refresh_token,
        "client_id": cid,
        "client_secret": csc,
        "grant_type": "refresh_token"
    }
    resp = requests.post(endpoints.GOOGLE_TOKEN_ENDPOINT, data=data)
    access_token_data = json.loads(resp.text)
    access_token = access_token_data["access_token"]
    creds = Credentials(token=access_token)
    return creds

def get_refresh_token(credentials_fpath: str, endpoints: List[str], port: int = 8080) -> str:
    """Opens Google auth screen and returns a refresh token"""
    flow = InstalledAppFlow.from_client_secrets_file(credentials_fpath, endpoints)
    creds = flow.run_local_server(port=port)
    return creds.refresh_token