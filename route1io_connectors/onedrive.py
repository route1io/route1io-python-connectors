"""OneDrive

The purpose of this module is for uploading/downloading files to/from OneDrive and SharePoint
via the Microsoft Graph API
"""

import os
import webbrowser
from typing import List, Dict
import json
import tempfile
from pathlib import Path

import requests 

from . import aws

DEFAULT_UPLOAD_CHUNK_SIZE = 3_276_800

def get_file(access_token: str, url: str) -> str:
    """Get content from file on OneDrive specified at URL

    Parameters
    ----------
    access_token : str
        Valid access token
    url : str
        Valid Microsoft Graph API URL of the file we are going to download

    Returns
    -------
    content : str
        Content of the downloaded file
    """
    resp = _get_request_url(access_token=access_token, url=url)
    return resp.content

def download_file(access_token: str, drive_id: str, remote_fpath: str, local_fpath: str) -> str:
    """Download file locally from OneDrive from the specified URL

    Parameters
    ----------
    access_token : str
        Valid access token
    drive_id : str 
        ID of the drive to upload file to 
    remote_fpath : str 
        Filepath in Drive to upload the file to
    local_fpath : str 
        Local filepath to upload to OneDrive

    Returns
    -------
    content : str
        Content of the downloaded file
    """
    download_url = _construct_download_url(drive_id=drive_id, remote_fpath=remote_fpath)
    resp = _get_request_url(access_token=access_token, url=download_url)
    with open(local_fpath, 'wb') as outfile:
        outfile.write(resp.content)
    return resp.content

def upload_file(access_token: str, drive_id: str, remote_fpath: str, 
                local_fpath: str, chunk_size: int = DEFAULT_UPLOAD_CHUNK_SIZE) -> Dict[str, str]:
    """Upload file locally to OneDrive at specified URL. Algorithm uses chunking 
    to allow for arbitrarily large files to be uploaded. See official Microsoft 
    Graph API docs for details on uploading large files via chunking and upload
    sessions: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession?view=odsp-graph-online
    
    Parameters
    ----------
    access_token : str
        Valid access token
    drive_id : str 
        ID of the drive to upload file to 
    remote_fpath : str 
        Filepath in Drive to upload the file to
    local_fpath : str 
        Local filepath to upload to OneDrive
    fpath : str
        Local fpath of the file we will upload to OneDrive location specified
        at url
    chunk_size : int
        Size of chunks to upload to OneDrive

    Returns
    -------
    resp : Dict[str, str]
        Dictionary of information pertaining to recently uploaded file
    """
    upload_url = _construct_upload_url(drive_id=drive_id, remote_fpath=remote_fpath)
    metadata = _create_upload_session(access_token=access_token, url=upload_url)
    upload_session_url = _get_upload_session_url(metadata=metadata)
    file_size = os.path.getsize(local_fpath)
    with open(local_fpath, 'rb') as infile:
        for chunk in _read_in_chunks(infile, chunk_size=chunk_size):
            metadata = _upload_chunk(
                access_token=access_token, 
                chunk=chunk,
                upload_session_url=upload_session_url,
                metadata=metadata,
                chunk_size=chunk_size,
                file_size=file_size
            ) 
    return metadata

def copy_file_to_aws_s3(access_token: str, drive_id: str, remote_fpath: str, s3, 
                        bucket: str, key: str = None) -> None:
    """Copy file at given URL to S3 bucket

    Parameters
    ----------
    access_token : str
        Valid access token for accessing OneDrive
    url : str
        URL of the file on OneDrive or SharePoint
    s3
        Valid S3 connection created using aws.connect_to_s3
    bucket : str
        Existing bucket on AWS
    key : str = None
        (Optional) Key name of the file as it will appear in S3. If left blank
        it will default to the same name that's in OneDrive
    """
    if key is None:
        key = Path(remote_fpath).name
    with tempfile.NamedTemporaryFile("wb+") as outfile:
        download_file(
            access_token=access_token,
            drive_id=drive_id, 
            remote_fpath=remote_fpath, 
            local_fpath=outfile.name
        )
        outfile.seek(0)
        aws.upload_to_s3(s3=s3, bucket=bucket, filename=outfile.name, key=key)

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
    """Return results of find SharePoint site(s) by keyword search

    Parameters
    ----------
    access_token : str
        Valid access token for accessing SharePoint
    search : str
        Search term for finding site

    Return
    ------
    Dict[str, str]
        Returns response JSON
    """
    resp = requests.get(
        headers={"Authorization": f"Bearer {access_token}"},
        url=f"https://graph.microsoft.com/v1.0/sites?search={search}"
    )
    return json.loads(resp.text)

def _construct_download_url(drive_id: str, remote_fpath: str):
    """Return properly formatted download URL"""
    return f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:{remote_fpath}:/content"

def _construct_upload_url(drive_id: str, remote_fpath: str) -> str:
    """Return properly formatted upload URL"""
    return f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:{remote_fpath}:/createUploadSession"

def _upload_chunk(access_token, chunk, upload_session_url, metadata, chunk_size, file_size) -> Dict[str, str]:
    """PUT request a chunk to the upload URL and return response metadata"""
    next_expected_start_byte = _get_next_expected_start_byte(metadata)
    content_range = _create_content_range_value(
        start_byte=next_expected_start_byte, 
        chunk_size=chunk_size, 
        file_size=file_size
    )
    metadata = requests.put(
        data=chunk,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Length": str(chunk_size),
            "Content-Range": content_range
        },
        url=upload_session_url
    )
    return json.loads(metadata.text)

def _create_content_range_value(start_byte: int, chunk_size: int, file_size: int) -> str:
    """Return Content-Range value at current chunk upload iteration"""
    end_byte = start_byte + (chunk_size - 1) 
    if end_byte >= file_size:
        end_byte = file_size - 1
    return f"bytes {start_byte}-{end_byte}/{file_size}"

def _get_upload_session_url(metadata: Dict[str, str]) -> str:
    """Return upload URL for PUT requesting file chunks into"""
    return metadata["uploadUrl"]

def _get_next_expected_start_byte(metadata: Dict[str, str]) -> int:
    """Return next expected start byte of file upload"""
    print(metadata)
    return int(metadata["nextExpectedRanges"][0].split("-")[0])

def _create_upload_session(access_token: str, url: str) -> Dict[str, str]:
    """Return dictionary of JSON response after creating upload session"""
    resp = requests.post(
        headers={"Authorization": f"Bearer {access_token}"},
        url=url
    ) 
    return json.loads(resp.text)

def _read_in_chunks(file_obj, chunk_size: int):
    """Return lazy-loaded chunk from file object"""
    while True:
        data = file_obj.read(chunk_size)
        if not data:
            break
        yield data

def _parse_filename_from_response_headers(headers) -> "str":
    """Return filename from GET request response header"""
    content_disposition = headers["Content-Disposition"]
    split_content = content_disposition.split(";")
    filename_field = split_content[-1]
    parsed_filename = filename_field.replace("filename=", "").replace('"', "")
    return parsed_filename

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

def _get_request_url(access_token: str, url: str) -> str:
    """Return response at URL"""
    resp = requests.get(
        headers={"Authorization": f"Bearer {access_token}"},
        url=url
    )
    return resp