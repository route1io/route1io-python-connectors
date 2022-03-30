"""Adthena

Connector for pulling data via the Adthena API

References
----------
API Reference
    https://api.adthena.com/?json
Adthena Knowledge Base (useful additional information on the platform)
    https://support.adthena.com/hc/en-us
"""

import json
from typing import Dict, List
from jmespath import search

import requests
import pandas as pd

def get_share_of_clicks_trend(api_key: str, domain_id: str, date_start: str,
                              date_end: str, competitors: List[str] = None,
                              search_term_groups: List[str] = None, whole_market: bool = False,
                              traffic_type: str = "paid", device: str = "desktop") -> "pd.DataFrame":
    """Return DataFrame of share of clicks trend data"""

    resp = requests.get(
        url=_construct_share_of_clicks_trend_url(
            domain_id=domain_id, date_start=date_start, date_end=date_end,
            competitors=competitors, search_term_groups=search_term_groups,
            whole_market=whole_market, traffic_type=traffic_type, device=device),
        headers=_construct_header(api_key=api_key)
    )
    # ipdb.set_trace()
    df = _process_response(resp)
    df["Value"] = df["Value"]*100
    df["Week"] = pd.to_datetime(df["Date"]).dt.to_period('W-SAT').dt.start_time
    return df

def _construct_share_of_clicks_trend_url(domain_id: str, date_start: str,
                                         date_end: str, competitors: List[str],
                                         search_term_groups: List[str],
                                         whole_market: bool, traffic_type: str, device: str) -> str:
    """Return URL for calling share of clicks trend API"""
    base_url = _construct_base_api_url(domain_id)
    query_params = _construct_api_url_query_params(
        date_start=date_start, date_end=date_end, competitors=competitors,
        search_term_groups=search_term_groups, whole_market=whole_market,
        traffic_type=traffic_type, device=device
    )
    url = f"{base_url}/share-of-clicks-trend/all?{query_params}"
    return url

def _construct_api_url_query_params(date_start: str, date_end: str, competitors: List[str],
                                    search_term_groups: List[str], whole_market: bool,
                                    traffic_type: str, device: str) -> str:
    """Return query parameters formatted from user input"""
    query_param = f"periodstart={date_start}&periodend={date_end}"
    query_param += f"&traffictype={traffic_type}"
    query_param += f"&device={device}"
    if competitors is not None:
        query_param += _combine_query_params('competitor', competitors)
    if search_term_groups is not None:
        query_param += _combine_query_params('kg', search_term_groups)
    if whole_market:
        query_param += "&wholemarket=true"
    return query_param

def _construct_base_api_url(domain_id: str) -> "str":
    """Return base URL from given domaind ID"""
    return f"https://api.adthena.com/wizard/{domain_id}"

def _process_response(resp) -> "pd.DataFrame":
    """Return DataFrame of processed response data"""
    resp_dict = json.loads(resp.text)
    all_data = []
    for competitor_data in resp_dict:
        competitor = competitor_data["Competitor"]
        data = competitor_data["Data"]
        for date_dict in data:
            date_dict["Competitor"] = competitor
            all_data.append(date_dict)
    df = pd.DataFrame(all_data)
    return df

def _combine_query_params(key: str, values: List[str]) -> str:
    """Return string of combined query parameters"""
    return f"&{key}=" + f"&{key}=".join(values)

def _construct_header(api_key: str) -> Dict[str, str]:
    """Return header dictionary for POST request with API key"""
    return {"Adthena-api-key": api_key, "Accept": "application/json"}

if __name__ == "__main__":
    import os
    # Add Adthena API key and domain ID as environment vars to call function
    API_KEY = os.environ.get("ADTHENA_API_KEY")
    DOMAIN_ID = os.environ.get("ADTHENA_DOMAIN_ID")
    df = get_share_of_clicks_trend(
        api_key=API_KEY,
        domain_id=DOMAIN_ID,
        date_start="2022-03-10",
        date_end="2022-03-23",
        whole_market=True
    )