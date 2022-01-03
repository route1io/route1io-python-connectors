import requests

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

def get_google_credentials(refresh_token: str, cid: str, csc: str) -> "google.oath2.credentials.Credentials":
    """Return a Credentials object containing the necessary credentials for
    connecting to DCM/Campaign Manager 360

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
    gsheets_credentials : google.oath2.credentials.Credentials
        Valid access credentials for accessing Google Sheets API
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
    dcm_credentials = Credentials(token=access_token)
    return dcm_credentials

def get_refresh_token(credentials_fpath: str) -> str:
    """Opens Google auth screen and returns a refresh token"""
    flow = InstalledAppFlow.from_client_secrets_file(credentials_fpath, endpoints.DCM_REPORTING_AUTHENTICATION_ENDPOINT)
    creds = flow.run_local_server(port=8080)
    return creds.refresh_token