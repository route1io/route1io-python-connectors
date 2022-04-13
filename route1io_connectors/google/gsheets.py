"""Google Sheets connectors

This module contains functions for interacting with Google Sheets
"""
import tempfile

import pandas as pd
from googleapiclient.discovery import build

from .. import aws

def upload_gsheets_spreadsheet(gsheets_conn: "googleapiclient.discovery.Resource",
                               filename: str, spreadsheet_id: str,
                               spreadsheet_name: str
                               ) -> None:
    """Clears a Google Sheet and uploads from file to sheet

    Parameters
    ----------
    gsheets_conn : googleapiclient.discovery.Resource
        Connection to Google Sheets API
    filename : str
        Name of the file to be uploaded
    spreadsheet_id : str
        ID of the Google Sheet to upload to
    spreadsheet_name : str
        Name of the specific Sheet to write to
    """
    df = pd.read_csv(filename)
    df = df.fillna("")
    clear_google_sheet(
        gsheets_conn=gsheets_conn,
        spreadsheet_id=spreadsheet_id,
        spreadsheet_name=spreadsheet_name
    )
    gsheets_conn.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        valueInputOption='USER_ENTERED',
        range=f"{spreadsheet_name}!A1",
        body={
            "majorDimension": "ROWS",
            "values": df.T.reset_index().T.values.tolist()
        }
    ).execute()

def connect_to_gsheets(credentials: "google.oauth2.credentials.Credentials"
                       ) -> "googleapiclient.discovery.Resource":
    """Return a connection to Google Sheets

    Parameters
    ----------
    credentials : google.oath2.credentials.Credentials
        Valid Credentials object with necessary authentication

    Returns
    -------
    gsheets : googleapiclient.discovery.Resource
        Connection to Google Sheets API
    """
    gsheets_conn = build('sheets', 'v4', credentials=credentials)
    return gsheets_conn

def clear_google_sheet(gsheets_conn: "googleapiclient.discovery.Resource",
                       spreadsheet_id: str, spreadsheet_name: str) -> None:
    """Clear specified Google Sheet

    gsheets_conn : googleapiclient.discovery.Resource
        Connection to Google Sheets API
    spreadsheet_id : str
        ID of the Google Sheets spreadsheet
    spreadsheet_name : str
        Specific name of Google Sheet to be cleared
    """
    range_all = f'{spreadsheet_name}!A1:Z'
    gsheets_conn.spreadsheets().values().clear(spreadsheetId=spreadsheet_id,
                                               range=range_all, body={}).execute()

def download_gsheets_spreadsheet(gsheets_conn: "googleapiclient.discovery.Resource",
                                 filename: str, spreadsheet_id: str,
                                 spreadsheet_name: str) -> None:
    """Download a file from a specified Google Sheet

    Parameters
    ----------
    gsheets_conn : googleapiclient.discovery.Resource
        Connection to Google Sheets API
    filename : str
        Name of the local filename to download to
    spreadsheet_id : str
        ID of the Google Sheet to download from
    spreadsheet_name : str
        Name of the specific Sheet to write to
    """
    range_all = f'{spreadsheet_name}!A1:Z'
    result = gsheets_conn.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_all).execute()
    values = result.get('values', [])
    df = pd.DataFrame(values[1:], columns=values[0])
    df.to_csv(filename, index=False)

def copy_sheet_to_aws_s3(gsheets_conn: "googleapiclient.discovery.Resource",
    spreadsheet_id: str, spreadsheet_name: str, s3, bucket: str,
    key: str = None) -> None:
    """Copy file at given Google Sheet to S3 bucket

    Parameters
    ----------
    gsheets_conn : googleapiclient.discovery.Resource
        Connection to Google Sheets API
    spreadsheet_id : str
        ID of the Google Sheet to download from
    spreadsheet_name : str
        Name of the specific Sheet to write to
    s3
        Valid S3 connection created using aws.connect_to_s3
    bucket : str
        Existing bucket on AWS
    key : str = None
        (Optional) Key name of the file as it will appear in S3. If left blank
        it will default to the same name that's in OneDrive
    """
    if key is None:
        key = f"{spreadsheet_name}.csv"
    with tempfile.NamedTemporaryFile("wb+") as outfile:
        download_gsheets_spreadsheet(
            gsheets_conn=gsheets_conn,
            spreadsheet_id=spreadsheet_id,
            spreadsheet_name=spreadsheet_name,
            filename=outfile.name
        )
        outfile.seek(0)
        aws.upload_to_s3(s3=s3, bucket=bucket, filename=outfile.name, key=key)