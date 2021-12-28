"""Google Ads connectors

This module contains functions for pulling data via Google Ads
"""

import datetime

import pandas as pd
import numpy as np

from google.ads.googleads.client import GoogleAdsClient

GOOGLEADS_VERSION = "v8"

# TODO: the API pull scripts can definitely be refactored/combined
def connect_to_googleads(google_yaml_fpath: str) -> "GoogleAdsClient":
    """Return a connection to Google Ads API via YAML file with necessary credentials

    Parameters
    ----------
    google_yaml_fpath : str
        Fpath to YAML file containing Google Ads credentials

    Returns
    -------
    GoogleAdsClient
        Connection to Google Ads API via Python wrapper
    """
    return GoogleAdsClient.load_from_storage(google_yaml_fpath, version=GOOGLEADS_VERSION)

def get_ad_group_data(googleads_client: "GoogleAdsClient", customer_id: str,
                    start_date: "datetime.date") -> "pd.DataFrame":
    ga_service = googleads_client.get_service("GoogleAdsService")
    search_request = googleads_client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = customer_id
    search_request.query = _ad_group_gaql_query(start_date=start_date)

    # HACK: accessing the resp data has to occur in same scope as 
    # service object otherwise segfault
    raw_resp = ga_service.search_stream(search_request)
    resp_data = []
    for batch in raw_resp:
        for row in batch.results:
            campaign = row.campaign
            ad_group = row.ad_group
            metrics = row.metrics
            segments = row.segments
            resp_data.append({
                "Campaign": campaign.name,
                "Ad group": ad_group.name,
                "Day": pd.to_datetime(datetime.datetime.strptime(segments.date, "%Y-%m-%d")),
                "Clicks": metrics.clicks,
                "Impr.": metrics.impressions,
                "Cost": metrics.cost_micros/1E6,
            })
    df = pd.DataFrame(resp_data)
    df = df.groupby(
        ["Campaign", "Ad group", "Day"],
        as_index=False
    ).sum()
    return df

def get_keyword_data(googleads_client: "GoogleAdsClient", customer_id: str, 
                    start_date: "datetime.date") -> "pd.DataFrame":
    ga_service = googleads_client.get_service("GoogleAdsService")
    search_request = googleads_client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = customer_id
    search_request.query = _keyword_gaql_query(start_date=start_date)

    # HACK: accessing the resp data has to occur in same scope as 
    # service object otherwise segfault
    raw_resp = ga_service.search_stream(search_request)
    resp_data = []
    for batch in raw_resp:
        for row in batch.results:
            campaign = row.campaign
            ad_group = row.ad_group
            metrics = row.metrics
            segments = row.segments
            ad_group_criterion = row.ad_group_criterion
            resp_data.append({
                "Day": segments.date, 
                "Impr. (Top) %": metrics.search_top_impression_share, 
                "Search impr. share": metrics.search_impression_share, 
                "Impr.": metrics.impressions, 
                "Cost": metrics.cost_micros/1E6,
                "Clicks": metrics.clicks, 
                "Conversions": metrics.conversions, 
                "Impr. (Abs. Top) %": metrics.absolute_top_impression_percentage,
                "Campaign": campaign.name,
                "Ad group": ad_group.name,
                "Search keyword": ad_group_criterion.keyword.text,
                "Search keyword match type": f"{ad_group_criterion.keyword.match_type.name.title()} match"
            })
    df = pd.DataFrame(resp_data)

    df = df.groupby(
        ["Search keyword", "Search keyword match type", "Campaign", "Ad group", "Day"],
        as_index=False
    ).agg({
        "Clicks":np.sum,
        "Impr.":np.sum,
        "Cost":np.sum, 
        "Impr. (Abs. Top) %": lambda x: np.average(x, weights=df.loc[x.index, "Impr."]) if (df.loc[x.index, "Impr."].sum()) > 0 else np.average(x), 
        "Impr. (Top) %": lambda x: np.average(x, weights=df.loc[x.index, "Impr."]) if (df.loc[x.index, "Impr."].sum()) > 0 else np.average(x),
        "Search impr. share": lambda x: np.average(x, weights=df.loc[x.index, "Impr."]) if (df.loc[x.index, "Impr."].sum()) > 0 else np.average(x)
    })
    return df

def get_keyword_conversion_data(googleads_client: "GoogleAdsClient", customer_id: str, 
                    start_date: "datetime.date") -> "pd.DataFrame":
    ga_service = googleads_client.get_service("GoogleAdsService")
    search_request = googleads_client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = customer_id
    search_request.query = _keyword_conversions_gaql_query(start_date=start_date)

    # HACK: accessing the resp data has to occur in same scope as 
    # service object otherwise segfault
    raw_resp = ga_service.search_stream(search_request)
    resp_data = []
    for batch in raw_resp:
        for row in batch.results:
            campaign = row.campaign
            ad_group = row.ad_group
            metrics = row.metrics
            segments = row.segments
            ad_group_criterion = row.ad_group_criterion
            resp_data.append({
                "Day": segments.date, 
                "Campaign": campaign.name,
                "Ad group": ad_group.name,
                "Search keyword": ad_group_criterion.keyword.text,
                "Search keyword match type": f"{ad_group_criterion.keyword.match_type.name.title()} match",
                "Conversion action": segments.conversion_action_name,
                "Conversions": metrics.conversions, 
            })
    df = pd.DataFrame(resp_data)
    df = df.pivot_table(
        index=["Search keyword", "Search keyword match type", "Campaign", "Ad group", "Day"],
        columns="Conversion action",
        values="Conversions"
    ).reset_index()
    return df

def get_ad_group_conversion_data(googleads_client: "GoogleAdsClient", customer_id: str,
                    start_date: "datetime.date") -> "pd.DataFrame":
    ga_service = googleads_client.get_service("GoogleAdsService")
    search_request = googleads_client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = customer_id
    search_request.query = _ad_group_conversions_gaql_query(start_date=start_date)

    # HACK: accessing the resp data has to occur in same scope as
    # service object otherwise segfault
    raw_resp = ga_service.search_stream(search_request)
    resp_data = []
    for batch in raw_resp:
        for row in batch.results:
            campaign = row.campaign
            ad_group = row.ad_group
            metrics = row.metrics
            segments = row.segments
            resp_data.append({
                "Day": segments.date,
                "Campaign": campaign.name,
                "Ad group": ad_group.name,
                "Conversion action": segments.conversion_action_name,
                "Conversions": metrics.conversions,
            })
    df = pd.DataFrame(resp_data)
    df = df.pivot_table(
        index=["Campaign", "Ad group", "Day"],
        columns="Conversion action",
        values="Conversions"
    ).reset_index()
    return df

def _ad_group_gaql_query(start_date: "datetime.date") -> str:
    return f"""
        SELECT
          campaign.name,
          ad_group.name,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          segments.date
        FROM ad_group WHERE segments.date BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND '{datetime.datetime.today().strftime("%Y-%m-%d")}'
    """

def _ad_group_conversions_gaql_query(start_date: "datetime.date") -> str:
    return f"""
        SELECT
            segments.date,
            campaign.name,
            ad_group.name,
            segments.conversion_action_name,
            metrics.conversions
        FROM ad_group
        WHERE segments.date BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND '{datetime.datetime.today().strftime("%Y-%m-%d")}'
    """

def _keyword_gaql_query(start_date: "datetime.date") -> str:
    return f"""
        SELECT
            segments.date,
            metrics.top_impression_percentage,
            metrics.search_top_impression_share,
            metrics.search_impression_share,
            metrics.impressions,
            metrics.cost_micros,
            metrics.clicks,
            metrics.conversions,
            metrics.absolute_top_impression_percentage,
            campaign.name,
            ad_group.name,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type
        FROM keyword_view
        WHERE segments.date BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND '{datetime.datetime.today().strftime("%Y-%m-%d")}'
    """

def _keyword_conversions_gaql_query(start_date: "datetime.date") -> str:
    return f"""
        SELECT
            segments.date,
            campaign.name,
            ad_group.name,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            segments.conversion_action_name,
            metrics.conversions
        FROM keyword_view
        WHERE segments.date BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND '{datetime.datetime.today().strftime("%Y-%m-%d")}'
    """