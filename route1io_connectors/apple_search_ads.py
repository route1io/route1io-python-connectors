"""Apple Search Ads

This module contains code for accessing data from Apple Search Ads.

References
----------
Apple Search Ads documentation
    https://developer.apple.com/documentation/apple_search_ads
Implementing OAuth for the Apple Search Ads API
    https://developer.apple.com/documentation/apple_search_ads/implementing_oauth_for_the_apple_search_ads_api
"""

# pylint: disable=invalid-name

import datetime
import json
from typing import Tuple, Dict

import jwt
import requests
import pandas as pd

from .utils import date_range, endpoints

ACCESS_TOKEN_EXPIRY = 3600

def get_apple_data(access_token: str, org_id: int, start_date: "datetime.datetime",
                   end_date: "datetime.datetime") -> "pd.DataFrame":
    """Return pd.DataFrame of Apple Search Ads data for a given organization
    between start and end dates (inclusive)

    Parameters
    ----------
    access_token : str
        Valid access token with permissions to access organization's ad account
        https://developer.apple.com/documentation/apple_search_ads/implementing_oauth_for_the_apple_search_ads_api
    org_id : int
        Organization whose ad account we are accessing data from
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
    date_ranges = date_range.calculate_date_ranges(start_date, end_date)
    date_range_dfs = []
    for start_date, end_date in date_ranges:
        resp = requests.post(
            url=endpoints.APPLE_CAMPAIGN_API_ENDPOINT,
            json=_post_json_data(start_date=start_date, end_date=end_date),
            headers=_post_request_header(access_token=access_token, org_id=org_id)
        )
        json_resp = json.loads(resp.text)
        date_range_dfs.append(_process_resp(json_resp))
    full_df = pd.concat(date_range_dfs)
    full_df = _process_output_df(full_df=full_df)
    return full_df

def request_access_token(client_id: str, client_secret: str) -> dict:
    """Return JSON response after POST requesting an access token

    Parameters
    ----------
    client_id : str
        Valid client ID
    client_secret : str
        Valid client secret

    Returns
    -------
    Response
        POST request response with refreshed access token
    """
    return requests.post(
        url=endpoints.OAUTH2_API_ENDPOINT,
        headers={
            "Host": "appleid.apple.com",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "searchadsorg"
        }
    )

def validate_client_secret(client_secret: str, client_id: str, team_id: str, key_id: str, private_key: str) -> str:
    """Return client secret after being validated. If expired than create a
    new one

    Parameters
    ----------
    client_secret : str
        Client secret loaded from file

    Returns
    -------
    client_secret : str
        Client secret validated and refreshed if necessary
    """
    token_payload = jwt.decode(client_secret, verify=False, algorithms="ES256")
    if _token_expired(token_payload["exp"]):
        client_secret = refresh_client_secret(
            client_id=client_id,
            team_id=team_id,
            key_id=key_id,
            private_key=private_key
        )
    return client_secret

def refresh_client_secret(client_id: str, team_id: str, key_id: str, private_key: str) -> str:
    """Return a refreshed client secret

    Parameters
    ----------
    client_id : str
        Valid client ID of the user requesting the data
    team_id : str
        Valid team ID from Apple Search Ads platform
    key_id : str
        Valid key ID from Apple Search Ads platform

    Returns
    -------
    client_secret : str
        Refreshed, valid client secret
    """

    # Necessary metadata
    AUDIENCE = 'https://appleid.apple.com'
    ALGORITHM = 'ES256'

    # Datetimes associated with JWT
    issued_at_timestamp, expiration_timestamp = _calculate_expiration_timestamp(86400*180)

    # JWT payload
    headers = {
        "alg": ALGORITHM,
        "kid": key_id
    }
    payload = {
        "sub": client_id,
        "aud": AUDIENCE,
        "iat": issued_at_timestamp,
        "exp": expiration_timestamp,
        "iss": team_id
    }

    # Encoded secret
    client_secret = jwt.encode(
        payload=payload,
        headers=headers,
        algorithm=ALGORITHM,
        key=private_key
    )
    client_secret = client_secret.decode("utf-8")

    return client_secret

def refresh_access_token(client_id: str, client_secret: str) -> str:
    """Returns a refreshed access token

    Parameters
    ----------
    client_id : str
        Valid client ID of the user requesting the data
    client_secret : str
        Valid client secret token

    Returns
    -------
    credentials_json
        Credentials JSON with valid access token
    """
    issued_at_timestamp, expiration_timestamp = _calculate_expiration_timestamp(ACCESS_TOKEN_EXPIRY)
    resp = request_access_token(client_id=client_id, client_secret=client_secret)

    # Add issue and expiration dates to the created credentials
    credentials_json = json.loads(resp.text)
    credentials_json.update({
        "iat": issued_at_timestamp,
        "exp": expiration_timestamp
    })

    return credentials_json

def _token_expired(exp_timestamp: int, cushion: int = 0) -> bool:
    """Return boolean checking if current UTC is equal to or greater than
    expiration timestamp

    Parameters
    ----------
    exp_timestamp : int
        UNIX timestamp for expiration
    cushion : int, optional
        Offset cushion to subtract from expiration date - done to prevent
        a token that's going to expire in i.e. 10 seconds that wouldn't be
        refreshed but would be invalid by the time it can be used

    Returns
    -------
    expired : bool
        True if current time is greater than or equal to expiration else False
    """

    expired = datetime.datetime.utcnow() >= datetime.datetime.fromtimestamp(exp_timestamp - int(cushion))
    return expired

def _calculate_expiration_timestamp(expiration_offset: "datetime.datetime") -> Tuple[int, int]:
    """Return a UNIX timestamp for the issued time and the expiration time in UTC

    Parameters
    ----------
    expiration_offset : int
        Time in seconds from issued timestamp to expire

    Returns
    -------
    issued_at_timestamp : int
        UNIX timestamp for time issued
    expiration_timestmap : int
        UNIX timestamp for expiration
    """
    issued_at_timestamp = int(datetime.datetime.utcnow().timestamp())
    expiration_timestamp = issued_at_timestamp + expiration_offset
    return issued_at_timestamp, expiration_timestamp

def _process_output_df(full_df: "pd.DataFrame") -> "pd.DataFrame":
    """Return output DataFrame after processing"""
    full_df = full_df.sort_values(["campaign_name", "date"])
    full_df = full_df[
        ["date", "campaign_id", "campaign_name", "impressions", "spend", "taps",
         "installs", "new_downloads", "redownloads", "lat_on_installs",
         "lat_off_installs", "ttr", "cpa", "cpt", "cpm", "conversion_rate"]
    ]
    return full_df

def _post_json_data(start_date: "datetime.date", end_date: "datetime.date") -> Dict[str, str]:
    """Return dictionary of JSON data to be POSTed to API endpoint"""
    return {
        "startTime": f"{start_date.strftime('%Y-%m-%d')}",
        "endTime": f"{end_date.strftime('%Y-%m-%d')}",
        "selector": {
            "orderBy": [
                {
                    "field": "countryOrRegion",
                    "sortOrder": "ASCENDING"
                }
            ],
        },
        "groupBy": [
            "countryOrRegion"
        ],
        "timeZone": "UTC",
        "returnRowTotals": False,
        "granularity": "DAILY",
        "returnGrandTotals": False
    }

def _process_resp(resp: dict) -> "pd.DataFrame":
    """Return DataFrame containing the processed raw response data"""
    campaign_resps = resp["data"]["reportingDataResponse"]["row"]
    campaign_dfs = [_process_campaign(campaign) for campaign in campaign_resps]
    df = pd.concat(campaign_dfs)
    return df

def _process_campaign(campaign: dict) -> "pd.DataFrame":
    """Return DataFrame containing processed campaign raw API response data"""
    processed_results = [_process_result(result) for result in campaign["granularity"]]
    campaign_df = pd.DataFrame(processed_results)
    campaign_df = campaign_df.assign(
        campaign_id=campaign["metadata"]["campaignId"],
        campaign_name=campaign["metadata"]["campaignName"]
    )
    return campaign_df

def _process_result(result: Dict[str, str]) -> Dict[str, str]:
    """Return dictionary of parsed data from raw API response"""
    return {
        "impressions": result["impressions"],
        "taps": result["taps"],
        "installs": result["installs"],
        "new_downloads": result["newDownloads"],
        "redownloads": result["redownloads"],
        "lat_on_installs": result["latOnInstalls"],
        "lat_off_installs": result["latOffInstalls"],
        "ttr": result["ttr"],
        "cpa": result["avgCPA"]["amount"],
        "cpt": result["avgCPT"]["amount"],
        "cpm": result["avgCPM"]["amount"],
        "spend": result["localSpend"]["amount"],
        "conversion_rate": result["conversionRate"],
        "date": result["date"]
    }

def _post_request_header(access_token: str, org_id: int) -> Dict[str, str]:
    """Return an authorized header for requesting from API endpoints"""
    return {
        "Authorization": f"Bearer {access_token}",
        "X-AP-Context": f"orgId={org_id}"
    }