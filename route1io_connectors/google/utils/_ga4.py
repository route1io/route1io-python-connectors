"""Code for requesting and processing data from GA4"""

from typing import List

from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

def process_ga4_data(
        analytics,
        view_id: str,
        dimensions: List[str] = None,
        metrics: List[str] = None,
        start_date: str = "7daysAgo",
        end_date: str = "today"
    ) -> "pd.DataFrame":
    """Return pd.DataFrame of GA4 data pulled via the 
    Google Analytics Data API"""
    resp = _request_ga4_data(
        analytics=analytics,
        view_id=view_id,
        dimensions=dimensions,
        metrics=metrics,
        start_date=start_date,
        end_date=end_date
    )

def _request_ga4_data(
        analytics,
        view_id: str,
        dimensions: List[str] = None,
        metrics: List[str] = None,
        start_date: str = "7daysAgo",
        end_date: str = "today"
    ):
    """Return response from reporting request to Google Analytics Data API"""
    request = RunReportRequest(
        property=f"properties/{view_id}",
        dimensions=[Dimension(dim) for dim in dimensions],
        metrics=[Metric(metric) for metric in metrics],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    return analytics.run_report(request)