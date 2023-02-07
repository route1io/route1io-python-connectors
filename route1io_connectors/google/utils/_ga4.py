"""Code for requesting and processing data from GA4"""

from typing import List

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
