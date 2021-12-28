"""OneDrive

The purpose of this module is for uploading/downloading files to/from OneDrive and SharePoint
via the Microsoft Graph API
"""

import webbrowser
from typing import List, Dict
import json

import requests 

def upload_file(access_token: str, url: str, fpath: str) -> Dict[str, str]:
    """Upload file locally to OneDrive at specified URL. Note: URL must be 
    suffixed with /content to work
    
    Parameters
    ----------
    access_token : str
        Valid access token
    url : str
        Valid Microsoft Graph API URL of the file we are going to update 
        and/or create
    fpath : str
        Local fpath of the file we will upload to OneDrive location specified
        at url

    Returns
    -------
    resp : Dict[str, str]
        Dictionary of information pertaining to recently uploaded file
    """
    with open(fpath, 'rb') as f:
        data = f.read()
    resp = requests.put(
        data=data,
        headers={"Authorization": f"Bearer {access_token}"},
        url=url
    )
    return json.loads(resp.text)

def permissions_prompt(tenant_id: str, client_id: str, scope: List[str]) -> None:
    """Convenience function for opening web browser to permissions prompt"""
    # NOTE: Microsoft's masl Python package seems to have some functionality
    # for interactively opening browser and then returning access token
    # after going through the flow
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize?client_id={client_id}&response_type=code&scope={'%20'.join(scope)}&prompt=consent"
    webbrowser.open(url)

def request_access_token(code: str, tenant_id: str, client_id: str, scope: List[str],
                         redirect_uri: str, client_secret: str) -> Dict[str, str]:
    """Return a dict of authentication information using the code provided from
    redirect URI
    
    Parameters
    ----------
    code : str
        Code acquired after accepting app permissions request and redirecting
        to redirect URI
    tenant_id : str
        User tenant ID
    client_id : str
        Client ID acquired after registering an app with Azure Active Directory
    scope : str
        List of scopes to acquire access token for
    redirect_uri : str
        Redirect URI
    client_secret : str
        Client secret for signing request
        
    Returns
    -------
    Dict[str, str]
        Dictionary containing access credentials that were just requested
    """
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