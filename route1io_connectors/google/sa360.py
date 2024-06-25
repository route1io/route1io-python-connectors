"""SA360 connectors

This module contains code for accessing data from Search Ads 360.
"""

import json
from typing import Optional, Dict, Union

import pandas as pd
import requests

def get_sa360_data(access_token: str, account_id: str, query: str, login_customer_id: Optional[Union[str, None]] = None) -> "pd.DataFrame":
    """Return SA360 data requested from the SA360 Reporting API and processed
    into a pd.DataFrame
    """
    report_url = _get_report_url(account_id)
    headers = _get_post_request_header(access_token, login_customer_id)
    next_page_token = None
    while True:
        data = _get_post_request_payload(query, next_page_token)
        resp = requests.post(
            report_url,
            headers=headers,
            data=data
        )
        _validate_http_response(resp)
        resp_dict = json.loads(resp.text)
        next_page_token = resp_dict.get("nextPageToken")
        if next_page_token is None:
            break

def _validate_http_response(resp: "requests.Response") -> None:
    """Raise error if HTTP status isn't 200"""
    if resp.status_code != 200:
        raise requests.HTTPError(f"HTTP Error {resp.status_code}: {resp.text}", response=resp)

def _get_post_request_payload(query: str, page_token: Optional[Union[str, None]] = None) -> Dict[str, Union[bool, str]]:
    """Return dictionary of POST request payload data"""
    return {
        "query": query,
        "pageToken": page_token
    }

def _get_post_request_header(access_token: str, login_customer_id: str) -> Dict[str, str]:
    """Return dictionary of POST request header data"""
    headers={"Authorization": f"Bearer {access_token}"}
    if login_customer_id is not None:
        headers["login_customer_id"] = login_customer_id
    return headers

def _get_report_url(account_id: str) -> str:
    """Return string to report URL for a given account_id"""
    return f"https://searchads360.googleapis.com/v0/customers/{account_id}/searchAds360:search"