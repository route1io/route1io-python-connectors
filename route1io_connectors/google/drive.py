"""Google Drive connectors

This module contains functions for interacting with Google Drive

References
----------
Upload file data
    https://developers.google.com/drive/api/v3/manage-uploads
Create and populate folders
    https://developers.google.com/drive/api/v3/folder
Files: create
    https://developers.google.com/drive/api/v3/reference/files/create
"""
from typing import Dict
from pathlib import Path

from googleapiclient.http import MediaFileUpload
from googleapiclient.discovery import build

def upload_file(drive: "googleapiclient.discovery.Resource", fpath: str, name: str = None, folder_id: str = None) -> Dict[str, str]:
    """POST request a file to a Google Drive folder and return the response

    Parameters
    ----------
    drive : "googleapiclient.discovery.Resource"
        Authenticated connection to Google Drive API
    fpath : str
        Filepath of the file we want to upload to Drive
    name : str
        Name of the file as it will appear in Drive
    folder_id : str
        ID of the folder to upload to

    Returns
    -------
    response : Dict[str, str]
        Response from Google after POST requesting file
    """
    file_metadata = {
        "name": name if name is not None else Path(fpath).name,
    }
    if folder_id is not None:
        file_metadata["parents"] = [folder_id]
    media = MediaFileUpload(fpath)
    request = drive.files().create(body=file_metadata, media_body=media, supportsAllDrives=True)
    response = request.execute()
    return response

def connect_to_google_drive(credentials: "google.oauth2.credentials.Credentials"
                       ) -> "googleapiclient.discovery.Resource":
    """Return a connection to Google Drive

    Parameters
    ----------
    credentials : google.oath2.credentials.Credentials
        Valid Credentials object with necessary authentication

    Returns
    -------
    google_drive_conn : googleapiclient.discovery.Resource
        Connection to Google Drive API
    """
    google_drive_conn = build('drive', 'v3', credentials=credentials)
    return google_drive_conn