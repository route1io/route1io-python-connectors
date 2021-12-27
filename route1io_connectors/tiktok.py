"""TikTok

This module contains code for sending messages to TikTok.
"""

import datetime
import json
from typing import Dict
from six import string_types
from urllib.parse import urlencode

import requests
import pandas as pd
import numpy as np

from .utils import date_range

ROOT_URL = "https://business-api.tiktok.com"
REPORTING_ENDPOINT = f"{ROOT_URL}/open_api/v1.2/reports/integrated/get"

def get_tiktok_data(access_token: str, advertiser_id: int, start_date: "datetime.datetime",
                    end_date: "datetime.datetime") -> "pd.DataFrame":
    """Return pd.DataFrame of TikTok Marketing API data for a given advertiser
    between start and end dates (inclusive)

    Parameters
    ----------
    access_token : str
        Valid access token with permissions to access advertiser's ad account
        data via API
        https://ads.tiktok.com/marketing_api/docs?id=1701890912382977
    advertiser_id: int
        Ad account we want to pull data from
    start_date : datetime.date
        Inclusive start date to pull data
    end_date : datetime.date
        Inclusive end date to pull data

    Returns
    -------
    full_df : pd.DataFrame
        DataFrame containing search ad data between start and end date for the
        organization
    """
    headers = {"Access-Token": access_token}
    date_ranges = date_range.calculate_date_ranges(start_date, end_date)
    date_range_dfs = []
    for start_date, end_date in date_ranges:
        query_param_str = _format_url_query_param_string(
            advertiser_id=advertiser_id,
            start_date=start_date,
            end_date=end_date
        )
        url = f"{REPORTING_ENDPOINT}?{query_param_str}"
        resp = requests.get(
            url=url,
            headers=headers
        )
        date_range_dfs.append(_process_response(resp))
    full_df = pd.concat(date_range_dfs)
    full_df = _process_output_df(df=full_df)
    return full_df

def _process_output_df(df: "pd.DataFrame") -> "pd.DataFrame":
    """Return DataFrame containing TikTok data pulled from the API"""
    df = df[["campaign_name", "stat_time_day", "adgroup_name", "spend",
             "impressions", "clicks", "total_on_web_order_value",
             'total_complete_payment_rate', 'total_page_event_search_value', 'total_user_registration_value',
             'total_consultation_value']]
    df = df.rename(columns={
        "campaign_name": "Campaign Name",
        "adgroup_name": "Ad Group Name",
        "stat_time_day": "Date",
        "spend": "Cost",
        "impressions": "Impression",
        "clicks": "Click",
        "total_on_web_order_value": "Place an Order",
        'total_consultation_value': "Total Phone Consultation",
        'total_complete_payment_rate': "Total Complete Payment",
        'total_page_event_search_value': "Total Search",
        'total_user_registration_value': "Total Registration",
    })
    df = df.astype({
        "Campaign Name": str,
        "Ad Group Name": str,
        "Date": str,
        "Cost": np.float32,
        "Impression": np.float32,
        "Click": np.float32,
        "Place an Order": np.float32,
        "Total Phone Consultation": np.float32,
        "Total Complete Payment": np.float32,
        "Total Search": np.float32,
        "Total Registration": np.float32
    })
    df["Date"] = pd.to_datetime(df["Date"])
    return df

def _format_url_query_param_string(advertiser_id: int, start_date: "datetime.date",
                             end_date: "datetime.date") -> str:
    """Return a URL encoded query string with parameters to GET request from
    TikTok endpoint
    """
    query_param_dict = _format_query_param_dict(
        advertiser_id=advertiser_id,
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

def _format_query_param_dict(advertiser_id: int, start_date: "datetime.date",
                             end_date: "datetime.date") -> Dict[str, str]:
    """Return dictionary with data we will request from TikTok Marketing API
    reporting endpoint
    """
    return {
        'advertiser_id': advertiser_id,
        'service_type': 'AUCTION',
        'report_type': 'BASIC',
        'data_level': 'AUCTION_ADGROUP',
        'dimensions': ['adgroup_id', 'stat_time_day'],
        'metrics': [
            'campaign_name',
            'adgroup_name',
            'spend',
            'impressions',
            'reach',
            'clicks',
            'conversion',
            'time_attr_total_on_web_order_value',
            'time_attr_total_shopping_value',
            'time_attr_total_search_value',
            "time_attr_total_on_web_register_value",
            'time_attr_total_phone_value'
        ],
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