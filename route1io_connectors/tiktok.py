"""TikTok

This module contains code for pulling data via TikTok Marketing API.
"""

import datetime
import json
from typing import Dict, List
from six import string_types
from urllib.parse import urlencode

import requests
import pandas as pd
import numpy as np

from .utils import date_range, endpoints

def get_tiktok_data(
        access_token: str, 
        advertiser_id: int, 
        data_level: str = "AUCTION_AD",
        dimensions: List[str] = ['ad_id', 'stat_time_day'],
        metrics: List[str] = [
            'campaign_name',
            'adgroup_name',
            'ad_id',
            'spend',
            'impressions',
            'reach',
            'clicks',
        ], 
        start_date: "datetime.datetime" = None,
        end_date: "datetime.datetime" = None,
    ) -> "pd.DataFrame":
    """Return pd.DataFrame of TikTok Marketing API data for an authorized advertiser.

    Parameters
    ----------
    access_token : str
        Valid access token with permissions to access advertiser's ad account
        data via API
        https://ads.tiktok.com/marketing_api/docs?id=1701890912382977
    advertiser_id : int
        Ad account we want to pull data from
    data_level : str
        Level of data to pull from. Campaign ID grouping needs AUCTION_CAMPAIGN,
        Adgroup ID grouping needs ADGROUP_ADGROUP, etc. Default is AUCTION_AD.
    dimensions : List[str]
        List of dimension(s) to group by. Each request can only have one ID dimension 
        and one time dimension. ID dimensions include advertiser_id, campaign_id,
        adgroup_id, and ad_id. Time dimensions include stat_time_day and stat_time_hour. 
        Default is ['ad_id', 'stat_time_day']
        https://ads.tiktok.com/marketing_api/docs?id=1707957200780290 
    start_date : datetime.date
        Inclusive datetime object start date to pull data. Default is today.
    end_date : datetime.date
        Inclusive datetime object end date to pull data. Default is seven days before end_date.

    Returns
    -------
    df : pd.DataFrame
        DataFrame containing search ad data between start and end date for the
        organization
    """
    if end_date is None: 
        end_date = datetime.datetime.today()
    if start_date is None:
        start_date = end_date - datetime.timedelta(days=7)
    date_ranges = date_range.calculate_date_ranges(start_date, end_date)
    date_range_dfs = []
    for start_date, end_date in date_ranges:
        query_param_str = _format_url_query_param_string(
            advertiser_id=advertiser_id,
            data_level=data_level,
            dimensions=dimensions,
            metrics=metrics,
            start_date=start_date,
            end_date=end_date
        )
        url = f"{endpoints.TIKTOK_REPORTING_ENDPOINT}?{query_param_str}"
        resp = requests.get(
            url=url,
            headers={"Access-Token": access_token}
        )
        date_range_dfs.append(_process_response(resp))
    df = pd.concat(date_range_dfs)
    return df

def _format_url_query_param_string(
        advertiser_id: int, 
        data_level: str, 
        dimensions: List[str], 
        metrics: List[str], 
        start_date: "datetime.date",                     
        end_date: "datetime.date"
    ) -> str:
    """Return a URL encoded query string with parameters to GET request from
    TikTok endpoint
    """
    query_param_dict = _format_query_param_dict(
        advertiser_id=advertiser_id,
        data_level=data_level,
        dimensions=dimensions,
        metrics=metrics,
        start_date=start_date,
        end_date=end_date
    )
    query_param_str = _url_encoded_query_param(query_param_dict=query_param_dict)
    return query_param_str

def _url_encoded_query_param(query_param_dict: Dict[str, str]) -> str:
    """Return URL encoded query parameters for GET requesting TikTok Marketing
    API reporting endpoint
    """
    url = urlencode(
        {k: v if isinstance(v, string_types) else json.dumps(v)
            for k, v in query_param_dict.items()}
    )
    return url

def _format_query_param_dict(
        advertiser_id: int, 
        data_level: str,
        dimensions: List[str],
        metrics: List[str],
        start_date: "datetime.date",                         
        end_date: "datetime.date"
    ) -> Dict[str, str]:
    """Return dictionary with data we will request from TikTok Marketing API
    reporting endpoint
    """
    return {
        'advertiser_id': advertiser_id,
        'service_type': 'AUCTION',
        'report_type': 'BASIC',
        'data_level': data_level,
        'dimensions': dimensions,
        'metrics': metrics,
        'start_date': start_date.strftime("%Y-%m-%d"),
        'end_date': end_date.strftime("%Y-%m-%d"),
        'page': 1,
        'page_size': 200
    }

def _process_response(resp: Dict[str, str]) -> "pd.DataFrame":
    """Return a DataFrame containing raw API response data"""
    resp_json = json.loads(resp.text)
    resp_data = resp_json['data']['list']
    rows = [{**row["metrics"], **row["dimensions"]} for row in resp_data]
    df = pd.DataFrame(rows)
    return df