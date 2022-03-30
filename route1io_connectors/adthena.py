"""Adthena

Connector for pulling data via the Adthena API

References
----------
API Reference
    https://api.adthena.com/?json
"""

import json
from typing import Dict, List

import requests
import pandas as pd

def get_share_of_clicks_trend(api_key: str, domain_id: str, date_start: str,
                              date_end: str, competitors: List[str] = None,
                              search_term_groups: List[str] = None, whole_market: bool = False,
                              traffictype: str = "paid") -> "pd.DataFrame":
    """Return DataFrame of share of clicks trend data"""
    url = f"https://api.adthena.com/wizard/{domain_id}/share-of-clicks-trend/all?periodstart={date_start}&periodend={date_end}&traffictype=paid&device=mobile"
    if competitors is not None:
        url += _combine_query_params('competitor', competitors)
    if search_term_groups is not None:
        url += _combine_query_params('kg', search_term_groups)
    if whole_market:
        url += "&wholemarket=true"
    resp = requests.get(
        url=url,
        headers=_construct_header(api_key=api_key)
    )
    # ipdb.set_trace()
    df = _process_response(resp)
    df["Value"] = df["Value"]*100
    df["Week"] = pd.to_datetime(df["Date"]).dt.to_period('W-SAT').dt.start_time
    return df

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
    API_KEY = os.environ.get("ADTHENA_API_KEY")
    DOMAIN_ID = os.environ.get("ADTHENA_DOMAIN_ID")
    df = get_share_of_clicks_trend(
        api_key=API_KEY,
        domain_id=DOMAIN_ID,
        date_start="2022-03-10",
        date_end="2022-03-23",
        whole_market=True
    )