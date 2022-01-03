import json
from typing import List

import requests

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from ..utils import endpoints

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