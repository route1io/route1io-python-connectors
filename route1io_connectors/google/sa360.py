"""
This module provides functionality to interact with the SA360 Reporting API to
fetch advertising data. The primary function, `get_sa360_data`, queries the
SA360 API and retrieves data based on specified account and query parameters.
This data is returned in the form of a pandas DataFrame, making it suitable for
further data analysis and manipulation.

The module handles authentication via access tokens, supports pagination to
fetch all relevant data, and allows for querying under manager account contexts
through the optional `login_customer_id` parameter. It requires the `requests`
and `pandas` packages for HTTP requests and data handling, respectively.

Internal utility functions are included for tasks such as validating HTTP
responses, constructing POST request payloads and headers, and generating API
request URLs. These functions support the main data retrieval function and are
not intended for direct external use.
"""

import json
from typing import Optional, Dict, Union

import pandas as pd
import requests

def get_sa360_data(
        access_token: str, account_id: str, query: str,
        login_customer_id: Optional[Union[str, None]] = None
    ) -> pd.DataFrame:
    """Return pd.DataFrame of SA360 data requested from the SA360 Reporting API

    Parameters
    ==========
    access_token : str
        Authenticated and refreshed access token
    account_id : str
        ID of the account we want to query data from
    query : str
        Query for building report we want returned. See the following link for
        details on the grammar
        https://developers.google.com/search-ads/reporting/query/query-language
    login_customer_id : Optional[Union[str, None]] = None
        ID of the manager account that has access to the lower level account.
        Required if access to the account is derived from a manager account.
        See the following link for details
        https://developers.google.com/search-ads/reporting/concepts/login-customer-id

    Returns
    =======
    response_df : pd.DataFrame
        DataFrame containing the data requested from the API

    Examples
    ========
    Here is a code sample that uses `get_sa360_data` to fetch campaign performance data:

    >>> access_token = 'YOUR_ACCESS_TOKEN_HERE'
    >>> account_id = 'YOUR_ACCOUNT_ID_HERE'
    >>> query = 'SELECT campaign.name, metrics.clicks, metrics.impressions, metrics.cost_micros FROM campaign WHERE segments.date DURING LAST_7_DAYS'
    >>> df = get_sa360_data(access_token, account_id, query)
    >>> print(df.head())
    """
    report_url = _get_report_url(account_id)
    headers = _get_post_request_header(access_token, login_customer_id)
    next_page_token = None
    response_dfs = []
    while True:
        data = _get_post_request_payload(query, next_page_token)
        resp = requests.post(
            report_url,
            headers=headers,
            data=data
        )
        _validate_http_response(resp)
        resp_dict = json.loads(resp.text)
        resp_df = pd.json_normalize(resp_dict["results"], sep="_")
        response_dfs.append(resp_df)
        next_page_token = resp_dict.get("nextPageToken")
        if next_page_token is None:
            break
    response_df = pd.concat(response_dfs)
    return response_df

def _validate_http_response(resp: requests.Response) -> None:
    """Raise error if HTTP status isn't 200"""
    if resp.status_code != 200:
        raise requests.HTTPError(
            f"HTTP Error {resp.status_code}: {resp.text}",
            response=resp
        )

def _get_post_request_payload(
        query: str, page_token: Optional[Union[str, None]] = None
    ) -> Dict[str, Union[bool, str]]:
    """Return dictionary of POST request payload data"""
    return {
        "query": query,
        "pageToken": page_token
    }

def _get_post_request_header(access_token: str, login_customer_id: str) -> Dict[str, str]:
    """Return dictionary of POST request header data"""
    headers={"Authorization": f"Bearer {access_token}"}
    if login_customer_id is not None:
        headers["login-customer-id"] = login_customer_id
    return headers

def _get_report_url(account_id: str) -> str:
    """Return string to report URL for a given account_id"""
    return f"https://searchads360.googleapis.com/v0/customers/{account_id}/searchAds360:search"