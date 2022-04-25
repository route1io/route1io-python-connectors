"""SA360 connectors

This module contains code for accessing data from Search Ads 360.
"""
# pylint: disable=too-many-arguments,too-many-locals,too-few-public-methods,line-too-long
from abc import ABC, abstractmethod
import datetime
import json
from threading import local
import time
import os
from typing import Dict, List, Union

import requests
import pandas as pd
import pysftp

from ..utils import endpoints

class SearchAds360:
    """High level Python connection to Search Ads 360"""
    def __init__(self, access_token: str) -> None:
        """Python API for getting data from Search Ads 360"""
        self.access_token = access_token

    def post_request_report(
            self,
            agency_id: str,
            report_type: str,
            advertiser_id: str = None,
            columns: list = None,
            start_date: str = None,
            end_date: str = None,
            verify_single_timezone: bool = None,
            statistics_currency: str = "agency",
            include_removed_entities: bool = None,
            max_rows_per_file: int = 1_000_000,
            download_format: str = "csv"
        ) -> dict:
        """Return response from a POST requested report"""
        # pylint: disable=unused-variable
        json_payload = create_report_payload(
            agency_id,
            report_type,
            advertiser_id,
            columns,
            start_date,
            end_date,
            verify_single_timezone,
            statistics_currency,
            include_removed_entities,
            max_rows_per_file,
            download_format
        )
        resp = requests.post(
            endpoints.GOOGLE_DOUBLE_CLICK_SEARCH_REPORT_ENDPOINT,
            headers={"Authorization": f"Bearer {self.access_token}"},
            data=json_payload
        )
        json_data = json.loads(resp.text)
        try:
            test = json_data['id']
        except KeyError:
            raise ValueError(f"SA360 returned an error:\n{resp.text}")

        return json_data

    def get_request_report(self, report_id: str) -> dict:
        """Return response from a given URL via GET request"""
        report_url = f"{endpoints.GOOGLE_DOUBLE_CLICK_SEARCH_REPORT_ENDPOINT}/{report_id}"
        return requests.get(report_url, headers={"Authorization": f"Bearer {self.access_token}"})

    def download_report(self, fpath: str, file_urls: list) -> None:
        """Download report to a given local fpath given a list of file_urls"""
        first_file = True
        for report_file in file_urls:
            resp = requests.get(
                report_file['url'],
                headers={"Authorization": f"Bearer {self.access_token}"}
            )
            csv_data = str(resp.text)

            # If not on first file then skip the header line
            if first_file:
                first_file = False
            else:
                csv_data = csv_data.split("\n")
                csv_data = '\n'.join(csv_data[1:])
            with open(fpath, "a") as outfile:
                outfile.write(csv_data)

    def wait_for_report(self, report_id: str, pause: int = 5,
                        max_requests: int = 50, silent: bool = True) -> None:
        """Wait until report is finished being created and return response with file URLs"""
        for i in range(max_requests):
            time.sleep(pause)
            if not silent:
                print(i)
            resp = self.get_request_report(report_id)
            if json.loads(resp.text)['isReportReady']:
                break
        return json.loads(resp.text)

    def n_days(
            self,
            days: int,
            agency_id: str,
            report_type: str,
            advertiser_id: str = None,
            columns: list = None,
            verify_single_timezone: bool = None,
            statistics_currency: str = "agency",
            include_removed_entities: bool = None,
            download_format: str = "csv",
            pause: int = 5,
            max_requests: int = 50,
            silent: bool = True,
            tmp_directory: str = None
        ) -> None:
        """Return a DataFrame with data going back n days"""

        # Calculate start and end date
        todays_date = datetime.datetime.now().date()
        start_date = todays_date - datetime.timedelta(days=days)

        # Request restatement file
        RESTATEMENT_FNAME = "tmp_restate.csv"
        if tmp_directory is not None:
            RESTATEMENT_FNAME = os.path.join(tmp_directory, RESTATEMENT_FNAME)

        self.request_and_download_report(
            fpath=RESTATEMENT_FNAME,
            agency_id=agency_id,
            report_type=report_type,
            advertiser_id=advertiser_id,
            columns=columns,
            start_date=start_date,
            end_date=todays_date,
            verify_single_timezone=verify_single_timezone,
            statistics_currency=statistics_currency,
            include_removed_entities=include_removed_entities,
            download_format=download_format,
            pause=pause,
            max_requests=max_requests,
            silent=silent
        )

        # Load restatement file
        n_days_df = pd.read_csv(RESTATEMENT_FNAME)
        os.remove(RESTATEMENT_FNAME)

        return n_days_df

    def request_and_download_report(
            self,
            fpath: str,
            agency_id: str,
            report_type: str,
            advertiser_id: str = None,
            columns: list = None,
            start_date: str = None,
            end_date: str = None,
            verify_single_timezone: bool = None,
            statistics_currency: str = "agency",
            include_removed_entities: bool = None,
            max_rows_per_file: int = 1_000_000,
            download_format: str = "csv",
            pause: int = 5,
            max_requests: int = 50,
            silent: bool = True
        ):
        """Make a POST request for a report, wait for it to be created
        and then download it.
        """
        post_resp = self.post_request_report(
            agency_id,
            report_type,
            advertiser_id,
            columns,
            start_date,
            end_date,
            verify_single_timezone,
            statistics_currency,
            include_removed_entities,
            max_rows_per_file,
            download_format
        )
        get_resp = self.wait_for_report(post_resp['id'], pause, max_requests, silent)
        self.download_report(fpath, get_resp['files'])

    @classmethod
    def from_refresh_token(cls, refresh_token: str, cid: str, csc: str) -> "SearchAds360":
        """
        Returns an instance of SearchAds360 with a valid
        access token generated from a refresh token and client credentials
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
        return cls(access_token=access_token)

class BaseColumn(ABC):
    """Abstract base class for column subclasses"""
    def __init__(self, name: str) -> None:
        self.name = name

    @abstractmethod
    def format_payload(self) -> dict:
        """Return an appropriately formatted dictionary expected by SA360 API"""
        # pylint: disable=unnecessary-pass
        pass

class Column(BaseColumn):
    """Column provided by SA360"""
    def format_payload(self) -> Dict[str, str]:
        """Return an appropriately formatted dictionary for a normal column
        as expected by SA360 API
        """
        return {"columnName": self.name}

class FloodlightColumn(BaseColumn):
    """Custom floodlight column defined by advertiser in SA360"""
    def format_payload(self) -> Dict[str, str]:
        """Return an appropriately formatted dictionary for a floodlight column
        as expected by SA360 API
        """
        return {"savedColumnName": self.name, "platformSource": "floodlight"}

def create_report_payload(
        agency_id: str, report_type: str, advertiser_id: str = None,
        columns: list = None, start_date: str = None, end_date: str = None,
        verify_single_time_zone: bool = None, statistics_currency: str = "agency",
        include_removed_entities: bool = None, max_rows_per_file: int = 1_000_000,
        download_format: str = None
    ) -> str:
    """Return a serialized JSON string containing how we want an SA360 report formatted"""
    json_payload = {
        "reportScope": {
            "agencyId": agency_id
        },
        "reportType": report_type
    }

    # Reduce scrope to advertiser ID
    if advertiser_id is not None:
        json_payload["reportScope"]["advertiserId"] = advertiser_id

    # Segment report by specified columns
    if columns is not None:
        json_payload["columns"] = []
        for col in columns:
            if isinstance(col, str):
                col = Column(col)
            json_payload["columns"].append(col.format_payload())

    # Set time range
    if start_date is not None or end_date is not None:
        json_payload["timeRange"] = {}
        if start_date is not None:
            start_date = _validate_datetime(start_date)
            json_payload["timeRange"]["startDate"] = start_date
        if end_date is not None:
            end_date = _validate_datetime(end_date)
            json_payload["timeRange"]["endDate"] = end_date

    if statistics_currency is not None:
        json_payload["statisticsCurrency"] = statistics_currency
    if verify_single_time_zone is not None:
        json_payload["verifySingleTimeZone"] = verify_single_time_zone
    if include_removed_entities is not None:
        json_payload["includeRemovedEntities"] = include_removed_entities
    if download_format is not None:
        json_payload["maxRowsPerFile"] = max_rows_per_file
        json_payload["downloadFormat"] = download_format

    return json.dumps(json_payload)

def filter_zero_rows(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Return a DataFrame that has had rows with zero values specified by columns filtered out"""
    filtered_df = df[columns]
    filtered_df = filtered_df.fillna(0)
    filtered_df = filtered_df.loc[~(filtered_df == 0).all(axis=1)]
    df = df.loc[filtered_df.index]

    # HACK: duplicate values are being introuduces somewhere so drop them
    df = df.drop_duplicates()
    return df

def sftp_upload_to_sa360(username: str, password: str, known_hosts: str, local_fpath: str,
                         remote_fpath: str = None) -> None:
    """Uploads local file to SA360 partner upload via SFTP

    Parameters
    ----------
    username : str
        Username for SA360 SFTP server
    password : str
        Password for SA360 SFTP server
    known_hosts : str
        Filepath to known_hosts file that contains known host fingerprint for
        partnerupload.google.com
    local_fpath : str
        Filepath to local file for uploading
    remote_fpath : str
        (Optional) Filepath of remote file on server to upload to. Default is
        the filename from local_fpath
    """
    cnopts = pysftp.CnOpts(knownhosts=known_hosts)
    with pysftp.Connection(
            host=endpoints.SA360_PARTNER_UPLOAD_SFTP_HOST,
            port=endpoints.SA360_PARTNER_UPLOAD_SFTP_PORT,
            username=username,
            password=password,
            cnopts=cnopts
        ) as sftp:
        sftp.put(localpath=local_fpath, remotepath=remote_fpath)

def _validate_datetime(date_obj):
    if isinstance(date_obj, str):
        try:
            date_obj = datetime.datetime.strptime(date_obj, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"{date_obj} is an incorrect date format; must be YYYY-MM-DD")
    return date_obj.strftime("%Y-%m-%d")