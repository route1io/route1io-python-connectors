"""Google Ads connectors

This module contains functions for pulling data via Google Ads
"""

import pandas as pd

from google.ads.googleads.client import GoogleAdsClient
from google.protobuf import json_format

GOOGLEADS_VERSION = "v10"

# TODO: the API pull scripts can definitely be refactored/combined
def connect_to_google_ads(google_yaml_fpath: str) -> "GoogleAdsClient":
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

def get_google_ads_data(google_ads_client: "GoogleAdsClient", customer_id: str, query: str) -> "pd.DataFrame":
    """Return a connection to Google Ads API via YAML file with necessary credentials

    Parameters
    ----------
    google_ads_client : GoogleAdsClient
        Authenticated client for accessing Google Ads API
    customer_id : str
        Customer ID of the customer whose data will be accessed
    query : str
        Valid GAQL query
        See https://developers.google.com/google-ads/api/fields/v10/overview_query_builder
        for more details

    Returns
    -------
    df : pd.DataFrame
        DataFrame of data pulled via API and GAQL query
    """
    ga_service = google_ads_client.get_service("GoogleAdsService")
    search_request = google_ads_client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = customer_id
    search_request.query = query

    resp_data = [
        pd.json_normalize(
            json_format.MessageToDict(row)
        ) for batch in ga_service.search_stream(search_request)
        for row in batch.results
    ]
    try:
        df = pd.concat(resp_data)
    except ValueError:
        df = pd.DataFrame()
    return df

if __name__ == "__main__":
    googleads_client = connect_to_google_ads("../google-ads.yaml")
