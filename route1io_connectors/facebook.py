import time

import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad

def get_insights(
        access_token: str,
        ad_account_id: str,
        fields: list = None,
        params: dict = None
    ) -> "pd.DataFrame":
    """
    Return a pandas.DataFrame containing insights associated with a Facebook ad
    account.

    Parameters
    ----------
    access_token : str
        Access token with ads_read permission generated from a Facebook app.
    ad_account_id : str
        Valid ad account ID (i.e. act_123456789101112)
    fields : list
        List of valid Facebook Marketing API fields.
        See Fields table at https://developers.facebook.com/docs/marketing-api/insights/parameters/v10.0
        for docs on available fields.
    params : dict
        Dict of valid Facebook Marketing API parameters.
        See Parameters table at https://developers.facebook.com/docs/marketing-api/insights/parameters/v10.0
        for docs on available parameters.

    Returns
    -------
    insights_df : pandas.DataFrame
        pandas.DataFrame containing the requested insights data from the
        Facebook Marketing API
    """

    # Default fields/params
    if fields is None:
        fields = ["campaign_name", "adset_name", "ad_name", "clicks",
                  "impressions", "reach", "ctr", "actions", "spend"]
    if params is None:
        params = {"date_preset": "last_30d", "time_increment": 1}

    # Get insights data from the Facebook Marketing API
    ad_account = _connect_ad_account(
        access_token=access_token, ad_account_id=ad_account_id
    )
    ads = _get_ads(ad_account=ad_account)
    insights = []
    for i, ad in enumerate(ads):
        # Facebook seems to have a problem with too many requests too quickly
        # print(i)
        time.sleep(.5)
        insights += _get_ad_insights(ad=ad, fields=fields, params=params)

    # Process the returned insights data
    insight_dicts = [_construct_insights_dict(insight) for insight in insights]
    insights_df = pd.DataFrame(insight_dicts)
    insights_df = _wrangle_dataframe(insights_df)
    return insights_df

def get_age_gender_insights(
        access_token: str,
        ad_account_id: str,
    ) -> "pd.DataFrame":
    """
    Return a pandas.DataFrame containing insights from the last 30 days associated with a Facebook
    ad account broken down by age and gender.

    Parameters
    ----------
    access_token : str
        Access token with ads_read permission generated from a Facebook app.
    ad_account_id : str
        Valid ad account ID (i.e. act_123456789101112)

    Returns
    -------
    age_gender_df : pandas.DataFrame
        pandas.DataFrame containing the requested insights data from the
        Facebook Marketing API broken down by age and gender

    """
    fields = ["campaign_name", "adset_name", "ad_name", "clicks",
              "impressions", "reach", "ctr", "actions", "spend"]
    params = {
        "date_preset": "last_30d",
        "time_increment": 1, "breakdowns": ["age", "gender"]
    }
    age_gender_df = get_insights(
        access_token=access_token,
        ad_account_id=ad_account_id,
        fields=fields,
        params=params
    )
    return age_gender_df

def get_region_insights(
        access_token: str,
        ad_account_id: str,
    ) -> "pd.DataFrame":
    """
    Return a pandas.DataFrame containing insights from the last 30 days associated with a Facebook
    ad account broken down by region.

    Parameters
    ----------
    access_token : str
        Access token with ads_read permission generated from a Facebook app.
    ad_account_id : str
        Valid ad account ID (i.e. act_123456789101112)

    Returns
    -------
    region_df : pandas.DataFrame
        pandas.DataFrame containing the requested insights data from the
        Facebook Marketing API broken down by region

    """
    fields = ["campaign_name", "adset_name", "ad_name", "clicks",
            "impressions", "reach", "ctr", "actions", "spend"]
    params = {
        "date_preset": "last_30d",
        "time_increment": 1, "breakdowns": ["region"]
    }
    region_df = get_insights(
                    access_token=access_token,
                    ad_account_id=ad_account_id,
                    fields=fields,
                    params=params
                )
    return region_df

def _connect_ad_account(access_token: str, ad_account_id: str) -> "AdAccount":
    """Return a connection to a Facebook Ad Account"""
    FacebookAdsApi.init(access_token=access_token)
    return AdAccount(ad_account_id)

def _get_ads(ad_account: "AdAccount") -> list:
    """Return list of Ad instances associated with an ad account"""
    ads = ad_account.get_ads()
    return list(ads)

def _get_ad_insights(ad: "Ad", fields: list, params: dict) -> list:
    """Return list of insights for a specific ad"""
    insights = ad.get_insights(
        fields=fields,
        params=params
    )
    return list(insights)

def _construct_insights_dict(insight) -> dict:
    """Return a list of dictionaries of parsed insights data"""
    data_dict = {key: val for key,
                 val in insight.items() if key != "actions"}
    try:
        actions_dict = {action["action_type"]: action["value"]
                        for action in insight["actions"]}
    except KeyError:
        actions_dict = {}
    combined_dict = {**data_dict, **actions_dict}
    return combined_dict

def _wrangle_dataframe(insights_df: "pd.DataFrame") -> "pd.DataFrame":
    """Return a wrangled DataFrame that is in the final expected format"""
    insights_df = insights_df.fillna(0)
    try:
        insights_df = insights_df.sort_values("date_start")
    except KeyError:
        insights_df = pd.DataFrame()
    return insights_df
