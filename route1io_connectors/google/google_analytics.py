"""Google Sheets connectors

This module contains functions for interacting with Google Analytics reporting
"""
from typing import List
import warnings

from googleapiclient.discovery import build
from google.analytics.data_v1beta import BetaAnalyticsDataClient

from .utils import _universal_analytics, _ga4

def connect_to_google_analytics(
        credentials: "google.oauth2.credentials.Credentials",
        ga4: bool = False
    ) -> "googleapiclient.discovery.Resource":
    """Return a connection to Google Drive

    Parameters
    ----------
    credentials : google.oath2.credentials.Credentials
        Valid Credentials object with necessary authentication

    Returns
    -------
    google_conn : googleapiclient.discovery.Resource
        Connection to Google Analytics API
    """
    warnings.warn("In the future ga4=True will become the default behavior for this function. Google is sunsetting Universal Analytics on July 1st, 2023 and is recommending you migrate to Google Analytics 4. More information can be found here: https://support.google.com/analytics/answer/11583528?hl=en", FutureWarning)
    if ga4: 
        google_conn = BetaAnalyticsDataClient(credentials=credentials)
    else:
        google_conn = build('analyticsreporting', 'v4', credentials=credentials)
        warnings.warn("Google is sunsetting Universal Analytics on July 1st, 2023 and is recommending you migrate to Google Analytics 4. More information can be found here: https://support.google.com/analytics/answer/11583528?hl=en", DeprecationWarning)
    return google_conn

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
    is_ga4_data = isinstance(analytics, BetaAnalyticsDataClient)
    if is_ga4_data:
        processing_func = _ga4.process_ga4_data
    else:
        processing_func = _universal_analytics.process_universal_analytics_data
    df = processing_func(
        analytics=analytics,
        view_id=view_id,
        dimensions=dimensions,
        metrics=metrics,
        start_date=start_date,
        end_date=end_date
    )
    return df
