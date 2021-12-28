"""OneDrive

The purpose of this module is for uploading/downloading files to/from OneDrive and SharePoint
via the Microsoft Graph API
"""

from os import access
import webbrowser
from typing import List, Dict
import json

import requests 
import jwt
from requests.api import request

def permissions_prompt(tenant_id: str, client_id: str, scope: List[str]) -> None:
    """Convenience function for opening web browser to permissions prompt"""
    # NOTE: Microsoft's masl Python package seems to have some functionality
    # for interactively opening browser and then returnign access token
    # after going through the flow
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize?client_id={client_id}&response_type=code&scope={'%20'.join(scope)}&prompt=consent"
    webbrowser.open(url)

def request_access_token(code: str, tenant_id: str, client_id: str, scope: List[str],
        redirect_uri: str, client_secret: str) -> Dict[str, str]:
    """Return a dict of authentication information"""
    return _request_token_endpoint(
        data=_encode_payload(
            client_id=client_id,
            scope=_encode_scope(scope),
            code=code,
            redirect_uri=redirect_uri,
            grant_type="authorization_code",
            client_secret=client_secret
        ),        
        url=_get_token_url(tenant_id=tenant_id)
    )

def refresh_access_token(client_id: str, client_secret: str, refresh_token: str,
        scope: List[str], tenant_id: str) -> Dict[str, str]:
    """Return dictionary of refreshed access token information"""
    return _request_token_endpoint(
        data=_encode_payload(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            grant_type="refresh_token",
            scope=_encode_scope(scope)
        ),
        url=_get_token_url(tenant_id=tenant_id)
    )

def search_sharepoint_site(access_token: str, search: str) -> Dict[str, str]:
    """Return results of find SharePoint site(s) by keyword search"""
    resp = requests.get(
        headers={"Authorization": f"Bearer {access_token}"},
        url=f"https://graph.microsoft.com/v1.0/sites?search={search}"
    )
    return json.loads(resp.text)

def create_file_in_folder(access_token: str, drive_id: str, folder_id: str, filename: str) -> Dict[str, str]:
    """Create file in folder"""
    resp = requests.post(
        json={
            "name": filename,
            "file": {}
        },
        headers={"Authorization": f"Bearer {access_token}"},
        url=f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children"
    )
    return json.loads(resp.text)

def upload_file(access_token: str, url: str, local_fpath: str) -> Dict[str, str]:
    with open(local_fpath, 'rb') as f:
        data = f.read()
    resp = requests.put(
        data=data,
        headers={"Authorization": f"Bearer {access_token}"},
        url=url
    )
    return json.loads(resp.text)

def _request_token_endpoint(data: str, url: str) -> Dict[str, str]:
    """Return JSON response as dictionary after POST requesting token endpoint"""
    resp = requests.post(data=data, url=url)
    return json.loads(resp.text)

def _get_token_url(tenant_id: str) -> str:
    """Return token URL built form the given tenant ID"""
    return f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

def _encode_payload(**kwargs) -> str:
    """Return dictionary payload as a string formatted for POST request"""
    return "&".join(
        f"{key}={val}" for key, val in kwargs.items()
    )

def _encode_scope(scope: List[str]) -> str:
    """Return scope encoded as a string for a URL query param"""
    return '%20'.join(scope)

if __name__ == "__main__":
    import dotenv
    env = dotenv.dotenv_values()
    