"""Google Sheets connectors

This module contains functions for interacting with Google Analytics reporting
"""
from typing import List, Union, Dict, Tuple
import itertools

import numpy as np
import pandas as pd
from googleapiclient.discovery import build

def connect_to_google_analytics(credentials: "google.oauth2.credentials.Credentials"
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
    google_drive_conn = build('analyticsreporting', 'v4', credentials=credentials)
    return google_drive_conn

def get_google_analytics_data(
        analytics,
        view_id: str,
        dimensions: List[str] = None,
        metrics: List[str] = None,
        start_date: str = "7daysAgo",
        end_date: str = "today"
    ) -> "pd.DataFrame":
    """Return a pd.DataFrame of Google Analytics data between the requested
    dates for the specified view ID

    Parameters
    ----------
    view_id : str
        View ID that we want to view
    dimensions : List[str]
        List of dimensions
        https://ga-dev-tools.web.app/dimensions-metrics-explorer/
    metrics : List[str]
        List of metrics
        https://ga-dev-tools.web.app/dimensions-metrics-explorer/
    start_date : str
        Dynamic preset such as 7daysago or YYYY-MM-DD
    end_date : str
        Dynamic preset such as today or YYYY-MM-DD

    Returns
    -------
    df : pd.DataFrame
    """
    resp_df_arr = []
    next_page_token = None
    while True:
        resp = _request_google_analytics_data(
            analytics=analytics,
            view_id=view_id,
            dimensions=dimensions,
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
            next_page_token=next_page_token
        )
        resp_df = _process_raw_google_analytics_data(resp=resp)
        resp_df_arr.append(resp_df)

        next_page_token = _get_next_page_token(resp=resp)
        if next_page_token is None:
            break
        
    df = pd.concat(resp_df_arr)
    return df

def _get_next_page_token(resp: Dict[str, str]) -> Union[str, None]:
    """Return Boolean indicating if paginated data exists"""
    return resp["reports"][0].get("nextPageToken")

def _request_google_analytics_data(
        analytics,
        view_id: str,
        dimensions: List[str] = None,
        metrics: List[str] = None,
        start_date: str = "7daysAgo",
        end_date: str = "today",
        next_page_token = Union[str, None]
    ) -> Dict[str, Union[str, List, Dict, bool]]:
    """Returns response from reporting request to the Google Analytics Reporting API
    built from arguments

    Parameters
    ----------
    view_id : str
        View ID that we want to view
    dimensions : List[str]
        List of dimensions
        https://ga-dev-tools.web.app/dimensions-metrics-explorer/
    metrics : List[str]
        List of metrics
        https://ga-dev-tools.web.app/dimensions-metrics-explorer/
    start_date : str
        Dynamic preset such as 7daysago or YYYY-MM-DD
    end_date : str
        Dynamic preset such as today or YYYY-MM-DD

    Returns
    -------
    resp : Dict[str, Union[str, List, Dict, bool]]
    """
    return analytics.reports().batchGet(
        body={'reportRequests': _process_report_requests(
            view_id=view_id,
            dimensions=dimensions,
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
            next_page_token=next_page_token
        )}
    ).execute()

def _process_raw_google_analytics_data(resp: Dict[str, Union[str, List, Dict, bool]]) -> "pd.DataFrame":
    """ Return a DataFrame parsed and constructed from the raw response from
    Google Analytics"""
    resp_data = resp['reports'][0]
    columns_metadata = _process_columns(resp_data['columnHeader'])
    columns = list(columns_metadata)
    values = _process_rows(resp_data['data'])
    df = pd.DataFrame(values, columns=columns)
    df = df.astype(columns_metadata)
    return df

def _process_rows(values_resp) -> List[List[str]]:
    """Return list of lists containing values parsed from API response"""
    rows = values_resp['rows']
    processed_rows = []
    for row in rows:
        try:
            dimensions = row['dimensions']
        except KeyError:
            dimensions = []

        metrics = [metric['values'] for metric in row['metrics']]
        metrics = list(itertools.chain.from_iterable(metrics))

        processed_rows.append([*dimensions, *metrics])
    return processed_rows

def _process_columns(column_header_resp: Dict[str, str]) -> List[Tuple[str]]:
    """Return a dictionary containing column name and associated dtype as parsed
    from the Google Analytics API
    """
    dimensions_cols = _process_dimensions_columns(column_header_resp=column_header_resp)
    metrics_cols = _process_metrics_columns(column_header_resp=column_header_resp)
    columns_metadata = [*dimensions_cols, *metrics_cols]
    return {key.replace("ga:", ""): val for key, val in columns_metadata}

def _process_metrics_columns(column_header_resp) -> List[Tuple]:
    """Return list of tuple's containing metrics and their associated dtype"""
    metrics_col_data = column_header_resp['metricHeader']['metricHeaderEntries']
    metrics_cols = [(metric['name'], _lookup_dtype(metric['type']))
                    for metric in metrics_col_data]
    return metrics_cols

def _process_dimensions_columns(column_header_resp) -> List[Tuple[str, str]]:
    """Return list of tuple's containing dimensions and their associated dtype"""
    try:
        dimensions_col_data = column_header_resp['dimensions']
    except KeyError:
        dimensions_cols = []
    else:
        dimensions_cols = [(dimension, str) for dimension in dimensions_col_data]
    return dimensions_cols

def _lookup_dtype(resp_type: str) -> Dict[str, str]:
    """Return dtype for pd.DataFrame associated with column as determined
    from the API response
    """
    dtypes = {
        "INTEGER": np.int32,
        "FLOAT": np.float32,
        "TIME": str,
        "CURRENCY": np.float32
    }
    return dtypes[resp_type]

def _process_report_requests(
        view_id: str,
        dimensions: Union[List[str], None],
        metrics: Union[List[str], None],
        start_date: str,
        end_date: str,
        next_page_token: Union[str, None]
    ) -> Dict[str, str]:
    """Return a dictionary containing formatted data request to Google Analytics
    API"""
    report_requests = {
        "viewId": f"ga:{view_id}",
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "pageSize": 100_000
    }
    if next_page_token is not None:
        report_requests["pageToken"] = next_page_token
    if dimensions is not None:
        report_requests['dimensions'] = _process_dimensions(dimensions)
    if metrics is not None:
        report_requests['metrics'] = _process_metrics(metrics)
    return [report_requests]

def _process_dimensions(dimensions: List[str]) -> List[Dict[str, str]]:
    """Return list of dictionary's containing the dimensions formatted for Google
    Analytics Reporting API to accept the request"""
    return [{"name": f"ga:{dimension}"} for dimension in dimensions]

def _process_metrics(metrics: List[str]) -> List[Dict[str, str]]:
    """Return list of dictionary's containing the metrics formatted for Google
    Analytics Reporting API to accept the request"""
    return [{"expression": f"ga:{metric}"} for metric in metrics]