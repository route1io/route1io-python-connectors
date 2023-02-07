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
    RunReportRequest(
        property=f"properties/{os.environ.get('GCP_PROPERTY_ID')}",
        dimensions=[Dimension(name="date"), Dimension(name="city")],
        metrics=[Metric(name="newUsers"), Metric(name="totalUsers")],
        date_ranges=[DateRange(start_date="2020-03-31", end_date="today")],
    )