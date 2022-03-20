"""LinkedIn

Connectors for pulling LinkedIn data

References
----------
LinkedIn Marketing APIs
    https://developer.linkedin.com/product-catalog/marketing
"""

import json
from typing import Dict, List

import requests
import pandas as pd

from .utils import endpoints

def get_linkedin_data(ad_account_id: str, access_token: str,
                      start_date: "datetime.date") -> "pd.DataFrame":
    """Return a DataFrame of LinkedIn data pulled via LinkedIn Marketing API"""
    campaigns_map = get_campaigns(access_token=access_token)
    ad_analytics_df = get_ad_analytics(ad_account_id=ad_account_id, access_token=access_token, start_date=start_date)
    ad_analytics_df['campaign'] = ad_analytics_df['id'].map(campaigns_map)
    ad_analytics_df = ad_analytics_df[["date", "campaign", "impressions", "clicks", "cost"]]    
    return ad_analytics_df

def get_ad_analytics(ad_account_id: str, access_token: str,
                     start_date: "datetime.date") -> "pd.DataFrame":
    """Return analytics data from LinkedIn adCampaignsV2 endpoint"""
    url = _format_analytics_request_url(
        ad_account_id=ad_account_id,
        start_date=start_date,
        fields=[
            "impressions",
            "clicks",
            "costInUsd",
            "dateRange",
            "pivotValue",
        ]
    )
    resp = _authorized_request(url=url, access_token=access_token)
    df = _process_ad_analytics_resp(resp=resp)
    return df

def _process_ad_analytics_resp(resp) -> "pd.DataFrame":
    json_data = json.loads(resp.text)
    date = lambda x: x['dateRange']['start']
    parsed_data = [
        {
            "date": f"{date(row)['year']}-{date(row)['month']}-{date(row)['day']}",
            "id": row["pivotValue"].split(":")[-1],
            "impressions": row["impressions"],
            "cost": row["costInUsd"],
            "clicks": row["clicks"]
        } for row in json_data['elements']
    ]
    return pd.DataFrame(parsed_data)

def get_campaigns(access_token: str) -> Dict[str, str]:
    """Return a dictionary map of LinkedIn campaign IDs to campaigns"""
    campaign_url = f"{endpoints.AD_CAMPAIGNS_ENDPOINT}?q=search"
    resp = _authorized_request(url=campaign_url, access_token=access_token)
    campaign_map = _process_campaigns_resp(resp=resp)
    return campaign_map

def _process_campaigns_resp(resp) -> "pd.DataFrame":
    """Return a DataFrame with the processed campaign data"""
    json_data = json.loads(resp.text)
    parsed_data = {str(campaign["id"]): campaign["name"] for campaign in json_data['elements']}
    return parsed_data

def _format_analytics_request_url(ad_account_id: str, start_date: "datetime.date",
                                  fields: List[str]) -> str:
    """Return analytics insights GET request URL formatted with the given parameters"""
    return f"{endpoints.AD_ANALYTICS_ENDPOINT}?q=analytics&pivot=CAMPAIGN&dateRange.start.day={start_date.day}&dateRange.start.month={start_date.month}&dateRange.start.year={start_date.year}&timeGranularity=DAILY&fields={','.join(fields)}&accounts=urn:li:sponsoredAccount:{ad_account_id}"

def _authorized_request(url: str, access_token: str) -> "requests.models.Response":
    """Return the response of an authorized GET request"""
    return requests.get(
        url=url,
        headers={
            "Authorization": f"Bearer {access_token}"
        }
    )