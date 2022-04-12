"""Adthena

Connector for pulling data via the Adthena API

References
----------
API Reference
    https://api.adthena.com/?json
Adthena Knowledge Base (useful additional information on the platform)
    https://support.adthena.com/hc/en-us
"""

import asyncio
import json
from typing import Dict, List

import aiohttp
import pandas as pd

def get_share_of_clicks_trend(
        api_key: str, domain_id: str, date_start: str, date_end: str, competitors: List[str] = None,
        search_term_groups: List[str] = None, whole_market: bool = False, traffic_type: str = "paid",
        device: str = "desktop"
    ) -> "pd.DataFrame":
    """Return pandas.DataFrame of share of clicks trend data

    Parameters
    ----------
    api_key : str
        Valid API key for accessing the Adthena API
    domain_id : str
        Domain ID of the account you are accessing data for
    date_start : str
        Start date to pull data for in YYYY-MM-DD
    date_end : str
        End date to pull data for in YYYY-MM-DD
    competitors : List[str] = None
        List of competitors to pull data for
    search_term_groups : List[str] = None
        List of search term groups to pull data for
    whole_market: bool = False
        Pull data from Whole Market or My Terms
    traffic_type: str = "paid"
        Traffic type to pull i.e. paid/organic/total/totalpaid
        (where totalpaid = PLA + Text Ads, and total includes everything.)
    device: str = "desktop"
        Device to pull data for

    Returns
    -------
    df : pd.DataFrame
        DataFrame constructed from processed JSON response from Adthena API
        with the given parameters of data
    """
    urls = []
    if search_term_groups is not None:
        for search_term in search_term_groups:
            url = _construct_share_of_clicks_trend_url(
            domain_id=domain_id, date_start=date_start, date_end=date_end,
            competitors=competitors, search_term_groups=[search_term],
            whole_market=whole_market, traffic_type=traffic_type, device=device)
            urls.append((url, search_term))
    else:
        url = _construct_share_of_clicks_trend_url(domain_id=domain_id,
            date_start=date_start, date_end=date_end,
            competitors=competitors, search_term_groups=None,
            whole_market=whole_market, traffic_type=traffic_type, device=device)
        urls.append((url, None))
    response_df = asyncio.run(
        _request_all_urls(
            urls=urls,
            headers=_construct_header(api_key=api_key)
        )
    )
    return response_df

async def _request_all_urls(urls: List[str], headers: Dict[str, str]) -> List[str]:
    """Return responses asynchronously requested from Adthena"""
    async with aiohttp.ClientSession() as session:
        responses = []
        for url, search_term in urls:
            async with session.get(url, headers=headers) as resp:
                responses.append((await resp.text(), search_term))
    response_dfs = []
    for resp, search_term in responses:
        df = _process_response(resp, search_term)
        response_dfs.append(df)
    response_df = pd.concat(response_dfs)
    return response_df

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
        search_term_groups = [term.replace(" ", "+") for term in search_term_groups]
        query_param += _combine_query_params('kg', search_term_groups)
    if whole_market:
        query_param += "&wholemarket=true"
    return query_param

def _construct_base_api_url(domain_id: str) -> "str":
    """Return base URL from given domaind ID"""
    return f"https://api.adthena.com/wizard/{domain_id}"

def _process_response(resp: str, search_term: str) -> "pd.DataFrame":
    """Return DataFrame of processed response data"""
    resp_dict = json.loads(resp)
    all_data = []
    for competitor_data in resp_dict:
        competitor = competitor_data["Competitor"]
        data = competitor_data["Data"]
        for date_dict in data:
            date_dict["Competitor"] = competitor
            all_data.append(date_dict)
    df = pd.DataFrame(all_data)
    df = df.assign(Search_Term=search_term)
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