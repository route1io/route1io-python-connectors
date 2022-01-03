import time
from typing import Dict

import requests
import pandas as pd
from googleapiclient.discovery import build

from . import credentials

# DCM API resource version
DCM_API_RESOURCE_VERSION = "v3.4"

def get_dcm_data(dcm: "googleapiclient.discovery.Resource", refresh_token: str, cid: str, csc: str, profile_id: str, report_id: str, fpath: str) -> "pd.DataFrame":
    """Return filepath to downloaded DCM fpath

    Parameters
    ----------
    dcm : googleapiclient.discovery.Resource
        Authenticated instance of DCM connection
    profile_id : str
        Profile ID on DCM of the account that has access to the report
    report_id : str
        Existing report on DCM that we want to create
    fpath : str
        Local filepath to download report to

    Returns
    -------
    fpath : str
        Absolute filepath to the downloaded DCM report
    """
    request_response = _request_report_run(
        dcm=dcm,
        profile_id=profile_id,
        report_id=report_id
    )
    ready_response = _wait_for_report(
        dcm=dcm,
        report_id=report_id,
        file_id=request_response['id']
    )
    report_url = _report_url_from_response(resp=ready_response)
    fpath = _download_report(url=report_url, credentials=credentials, fpath=fpath)
    return fpath

def connect_to_dcm(credentials: "google.oauth2.credentials.Credentials") -> "googleapiclient.discovery.Resource":
    """Return a connection to Campaign Manager 360

    Parameters
    ----------
    credentials : google.oath2.credentials.Credentials
        Valid Credentials object with necessary authentication

    Returns
    -------
    dcm : googleapiclient.discovery.Resource
        Connection to Google Sheets API
    """
    dcm = build('dfareporting', DCM_API_RESOURCE_VERSION, credentials=credentials)
    return dcm

def _download_report(url: str, credentials: "google.oath2.credentials.Credentials", fpath: str) -> str:
    """Return filepath of downloaded report from API via GET request"""
    headers = {
        "Authorization": f"Bearer {credentials.token}"
    }
    resp = requests.get(url=url, headers=headers)
    with open(fpath, "wb") as outfile:
        outfile.write(resp.content)
    return fpath

def _report_url_from_response(resp: Dict[str, str]) -> str:
    """Return URL from report response"""
    return resp["urls"]["apiUrl"]

def _request_report_run(dcm: "googleapiclient.discovery.Resource",
                       profile_id: str, report_id: str) -> Dict[str, str]:
    """Return response after requesting a report begins processing a new file

    Parameters
    ----------
    dcm : googleapiclient.discovery.Resource
        Authenticated connection to DCM
    profile_id : str
        Profile ID on DCM of the account that has access to the report
    report_id : str
        Existing report on DCM that we want to create

    Returns
    -------
    response : Dict[str, str]
        JSON response of file processing request
    """
    request = dcm.reports().run(profileId=profile_id, reportId=report_id)
    response = request.execute()
    return response

def _wait_for_report(dcm: "googleapiclient.discovery.Resource",
                    report_id: str, file_id: str) -> None:
    """Block with exponential backoff until report status is completed

    Parameters
    ----------
    dcm : googleapiclient.discovery.Resource
        Authenticated connection to DCM
    report_id : str
        Report ID of existing report on DCM that contains the file being processed
    file_id : str
        File ID of file being processed
    """
    wait = 2
    request = dcm.files().get(reportId=report_id, fileId=file_id)
    response = request.execute()
    while response['status'] != 'REPORT_AVAILABLE':
        time.sleep(wait)
        request = dcm.files().get(reportId=report_id, fileId=file_id)
        response = request.execute()
        wait *= 2
    return response
