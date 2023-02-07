"""Code for requesting and processing data from GA4"""

from typing import List, Dict
import itertools

import pandas as pd
import numpy as np
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
    resp_df = _process_raw_ga4_data(resp=resp)
    return resp_df

def _process_raw_ga4_data(resp) -> "pd.DataFrame":
    """Return a DataFrame containing the processed data extracted from GA4"""
    rows = []
    keys = _build_list_from_resp(resp.dimension_headers, resp.metric_headers, attr_name = "name")
    metric_dtypes = _build_metric_type_list_from_resp(resp)
    for row in resp.rows:
        values = _build_list_from_resp(row.dimension_values, row.metric_values, attr_name = "value")
        row_dict = dict(zip(keys, values))
        rows.append(row_dict)
    df = pd.DataFrame(rows)
    df = df.astype(metric_dtypes)
    return df

def _build_list_from_resp(*args, attr_name: str) -> List[str]:
    """Return list of strings of values parsed from header information in response"""
    return [getattr(val, attr_name) for val in list(itertools.chain.from_iterable(args))]

def _build_metric_type_list_from_resp(resp) -> Dict[str, str]:
    """Return a dict of strings detailing data type of the returned metric"""
    return {val.name: _lookup_dtype(val.type_.name) for val in resp.metric_headers}

def _lookup_dtype(resp_type: str) -> str:
    """Return dtype for pd.DataFrmae column associated with Google's provided dtype"""
    dtype_lookup_table = {
        "TYPE_INTEGER": np.int32
    }
    return dtype_lookup_table.get(resp_type, str)

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
        dimensions=[Dimension(name=dim) for dim in dimensions],
        metrics=[Metric(name=metric) for metric in metrics],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    return analytics.run_report(request)