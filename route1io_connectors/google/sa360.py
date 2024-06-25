"""SA360 connectors

This module contains code for accessing data from Search Ads 360.
"""

from typing import Optional, Dict, Union, NoneType

import pandas as pd

def get_sa360_data(access_token: str, account_id: str, query: str, login_customer_id: Optional[Union[str, None]] = None) -> "pd.DataFrame":
    """Return SA360 data requested from the SA360 Reporting API and processed
    into a pd.DataFrame
    """
    report_url = _get_report_url(account_id)
    headers = _get_post_request_header(access_token, login_customer_id)
    data = _get_post_request_payload(query)

def _get_post_request_payload(query: str, page_token: Optional[Union[str, None]] = None) -> Dict[str, Union[bool, str]]:
    """Return dictionary of POST request payload data"""
    pass

def _get_post_request_header(access_token: str, login_customer_id: str) -> Dict[str, str]:
    """Return dictionary of POST request header data"""
    headers={"Authorization": f"Bearer {access_token}"}
    if login_customer_id is not None:
        headers["login_customer_id"] = login_customer_id
    return headers

def _get_report_url(account_id: str) -> str:
    """Return string to report URL for a given account_id"""
    return f"https://searchads360.googleapis.com/v0/customers/{account_id}/searchAds360:search"