"""SA360 connectors

This module contains code for accessing data from Search Ads 360.
"""

from typing import Optional, Dict

import pandas as pd

def get_sa360_data(access_token: str, account_id: str, query: str, login_customer_id: Optional[str] = None) -> "pd.DataFrame":
    """Return SA360 data requested from the SA360 Reporting API and processed
    into a pd.DataFrame
    """
    report_url = _get_report_url(account_id)
    headers = _get_post_request_header(access_token, login_customer_id)

def _get_post_request_header(access_token: str, login_customer_id: str) -> Dict[str, str]:
    """Return header for POST request"""
    headers={"Authorization": f"Bearer {access_token}"}
    if login_customer_id is not None:
        headers["login_customer_id"] = login_customer_id
    return headers

def _get_report_url(account_id: str) -> str:
    return f"https://searchads360.googleapis.com/v0/customers/{account_id}/searchAds360:search"