"""Code for requesting and processing data from GA4"""

from typing import List, Dict, Optional
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
    dimensions: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None,
    start_date: str = "7daysAgo",
    end_date: str = "today",
) -> pd.DataFrame:
    """Return a DataFrame of GA4 data pulled via the Google Analytics Data API."""

    resp = _request_ga4_data(
        analytics=analytics,
        view_id=view_id,
        dimensions=dimensions,
        metrics=metrics,
        start_date=start_date,
        end_date=end_date,
    )

    return _process_raw_ga4_data(resp=resp)


def _process_raw_ga4_data(resp) -> pd.DataFrame:
    """Return a DataFrame containing the processed data extracted from GA4."""

    keys = _build_list_from_resp(
        resp.dimension_headers,
        resp.metric_headers,
        attr_name="name",
    )

    metric_dtypes = _build_metric_type_list_from_resp(resp)

    rows = []

    for row in resp.rows:
        values = _build_list_from_resp(
            row.dimension_values,
            row.metric_values,
            attr_name="value",
        )

        rows.append(dict(zip(keys, values)))

    # Supplying columns ensures that an empty response still produces a
    # correctly structured DataFrame rather than a DataFrame with no columns.
    df = pd.DataFrame(rows, columns=keys)

    if metric_dtypes:
        df = df.astype(metric_dtypes)

    return df


def _build_list_from_resp(*args, attr_name: str) -> List[str]:
    """Return response attributes flattened into a list."""

    return [
        getattr(value, attr_name)
        for value in itertools.chain.from_iterable(args)
    ]


def _build_metric_type_list_from_resp(resp) -> Dict[str, object]:
    """Return the pandas dtype for each returned GA4 metric."""

    return {
        header.name: _lookup_dtype(header.type_.name)
        for header in resp.metric_headers
    }


def _lookup_dtype(resp_type: str):
    """Return the pandas dtype associated with a GA4 response datatype."""

    dtype_lookup_table = {
        # Nullable integer is safer for empty data and missing values.
        "TYPE_INTEGER": "Int64",
        "TYPE_FLOAT": np.float64,
        "TYPE_SECONDS": np.float64,
        "TYPE_MILLISECONDS": np.float64,
        "TYPE_MINUTES": np.float64,
        "TYPE_HOURS": np.float64,
        "TYPE_STANDARD": np.float64,
        "TYPE_CURRENCY": np.float64,
        "TYPE_FEET": np.float64,
        "TYPE_MILES": np.float64,
        "TYPE_METERS": np.float64,
        "TYPE_KILOMETERS": np.float64,
    }

    return dtype_lookup_table.get(resp_type, str)


def _request_ga4_data(
    analytics,
    view_id: str,
    dimensions: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None,
    start_date: str = "7daysAgo",
    end_date: str = "today",
):
    """Return a response from the Google Analytics Data API."""

    dimensions = dimensions or []
    metrics = metrics or []

    request = RunReportRequest(
        property=f"properties/{view_id}",
        dimensions=[
            Dimension(name=dimension)
            for dimension in dimensions
        ],
        metrics=[
            Metric(name=metric)
            for metric in metrics
        ],
        date_ranges=[
            DateRange(
                start_date=start_date,
                end_date=end_date,
            )
        ],
    )

    return analytics.run_report(request=request)