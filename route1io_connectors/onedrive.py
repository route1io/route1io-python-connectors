"""OneDrive

The purpose of this module is for uploading/downloading files to/from OneDrive and SharePoint
"""

import webbrowser
from typing import List, Dict
import json

import requests 
import jwt

def permissions_prompt(tenant_id: str, client_id: str, scope: List[str]) -> None:
    """Convenience function for opening web browser to permissions prompt"""
    # NOTE: Microsoft's masl Python package seems to have some functionality
    # for interactively opening browser and then returnign access token
    # after going through the flow
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize?client_id={client_id}&response_type=code&scope={'%20'.join(scope)}&prompt=consent"
    webbrowser.open(url)

def request_access_token(
        code: str, 
        tenant_id: str, 
        client_id: str, 
        scope: List[str], 
        redirect_uri: str, 
        client_secret: str
    ) -> dict:
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

def refresh_access_token(
        client_id: str, 
        client_secret: str, 
        refresh_token: str, 
        scope: List[str], 
        tenant_id: str
    ):
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
    data = refresh_access_token(
    tenant_id=env['tenant_id'],
    client_id=env['client_id'],
    refresh_token=env['refresh_token'],
    scope=["user.read", "Sites.Read.All", "offline_access"],
    client_secret=env['client_secret'])