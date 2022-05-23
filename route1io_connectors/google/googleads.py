"""Google Ads connectors

This module contains functions for pulling data via Google Ads
"""

import datetime

import pandas as pd
import numpy as np

from google.ads.googleads.client import GoogleAdsClient

GOOGLEADS_VERSION = "v10"

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

def get_google_ads_data(googleads_client: "GoogleAdsClient", customer_id: str, query: str) -> "pd.DataFrame":
    ga_service = googleads_client.get_service("GoogleAdsService")
    search_request = googleads_client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = customer_id
    search_request.query = query

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
    return df

if __name__ == "__main__":
    googleads_client = connect_to_googleads("../google-ads.yaml")
