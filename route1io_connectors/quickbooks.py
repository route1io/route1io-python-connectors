import json
import datetime
from typing import Dict, List, Tuple, Optional

import requests
import networkx as nx
import pandas as pd
from dateutil.relativedelta import relativedelta

def request_access_token_refresh(
        access_token: str,
        refresh_token: str,
        client_id: str,
        client_secret: str
    ) -> str:
    """
    Refreshes the access token using the provided refresh token and client credentials.

    Sends a POST request to the TSheets API grant endpoint to obtain a new access token.
    This function is typically used when the current access token has expired.

    Parameters
    ----------
    access_token : str
        The current access token used for authentication.
    refresh_token : str
        The refresh token provided by the API for generating a new access token.
    client_id : str
        The client identifier registered with the API.
    client_secret : str
        The client secret key associated with the client ID.

    Returns
    -------
    str
        A JSON string containing the new access token and related authentication details.

    Notes
    -----
    The function assumes that the API response is valid JSON and that authentication
    credentials are correct. If invalid credentials are supplied, the API may return
    an error response instead of a token payload.
    """
    resp = requests.post(
        url="https://rest.tsheets.com/api/v1/grant",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token
        }
    )
    token_response = json.loads(resp.text)
    return token_response

def get_timesheets(
        access_token: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
    """
    Retrieves and processes timesheet data from the TSheets API within a specified date range.

    This function orchestrates data retrieval and transformation by calling internal
    helper functions to fetch raw results, process metadata, and merge them into
    a unified DataFrame for analysis.

    Parameters
    ----------
    access_token : str
        The API access token for authentication.
    start_date : str
        The start date for data retrieval in 'YYYY-MM-DD' format.
    end_date : str
        The end date for data retrieval in 'YYYY-MM-DD' format.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing processed timesheet records with associated user and
        jobcode details.

    Notes
    -----
    The returned DataFrame includes merged metadata and normalized identifiers.
    Pagination and multi-month ranges are handled automatically by internal functions.
    """
    results, metadata = _request_data(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date
    )
    metadata = _process_metadata(metadata)
    df = _process_results(results, metadata)
    return df

def _process_results(results: List[Dict], metadata: List[Dict]) -> pd.DataFrame:
    """Returns a DataFrame from merging timesheet results with supplemental metadata."""

    records = []
    for page in results:
        for timesheet_id, timesheet in page['timesheets'].items():
            record = {"timesheet_id": str(timesheet_id)}
            record.update(timesheet)  # merge all timesheet attributes
            records.append(record)
    df = pd.DataFrame(records)
    df["user_id"] = df["user_id"].astype(str)
    df["jobcode_id"] = df["jobcode_id"].astype(str)
    df = (
        df
            .merge(metadata[0], on="user_id", how="left")
            .merge(metadata[1], on="jobcode_id", how="left")
    )
    df = df.rename(columns={"duration": "duration_seconds"})
    return df

def _process_metadata(metadata: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Returns user and jobcode DataFrames extracted from API metadata."""
    users = [page['users'] for page in metadata]
    jobcodes = [page['jobcodes'] for page in metadata]

    users_df = _create_users_lookup(users)
    jobcodes_df = _create_jobcode_lookup(jobcodes)

    return users_df, jobcodes_df

def _create_users_lookup(users: List[Dict]) -> pd.DataFrame:
    """Returns a DataFrame containing user details built from user metadata."""
    records = []
    for page in users:
        for user_id, user_data in page.items():
            record = {"user_id": str(user_id)}
            record.update(user_data)  # merge all user attributes
            records.append(record)
    users_df = pd.DataFrame(records)
    users_df = users_df[["user_id", "first_name", "last_name"]]
    users_df = users_df.drop_duplicates()
    return users_df

def _graph_jobcodes(jobcodes: List[Dict]) -> pd.DataFrame:
    """Returns a DataFrame representing hierarchical jobcode paths constructed from metadata."""
    G = _create_jobcodes_graph_edges(jobcodes)
    leaf_lookup = _get_leaf_lookup(G)
    jobcode_df = pd.DataFrame.from_dict(
        leaf_lookup,
        orient='index',
        columns=['jobcode']
    )
    return jobcode_df

def _get_leaf_lookup(G: nx.DiGraph) -> Dict[str, str]:
    """Returns a dictionary mapping leaf jobcodes to full hierarchy paths within a directed graph."""
    roots = [n for n in G if G.in_degree(n) == 0]
    leaves = [n for n in G if G.out_degree(n) == 0]

    leaf_lookup = {}
    for root in roots:
        for leaf in leaves:
            if nx.has_path(G, root, leaf):
                for path in nx.all_simple_paths(G, source=root, target=leaf):
                    path_str = " > ".join(G.nodes[x].get("name", str(x)) for x in path)
                    leaf_lookup[leaf] = path_str
    return leaf_lookup


def _create_jobcodes_graph_edges(jobcodes: List[Dict]) -> nx.DiGraph:
    """Returns a directed graph representing parent-child relationships between jobcodes."""    G = nx.DiGraph()
    for page in jobcodes:
        for jobcode_id, jobcode in page.items():
            job_id = str(jobcode_id)
            parent_id = str(jobcode.get("parent_id")) if jobcode.get("parent_id") else None
            G.add_node(job_id, **jobcode)
            if parent_id and parent_id not in ("None", "0", "", "null"):
                G.add_edge(parent_id, job_id)
    return G

def _create_jobcode_lookup(jobcodes: List[Dict]) -> pd.DataFrame:
    """Returns a DataFrame of jobcode identifiers with hierarchy levels extracted and cleaned."""
    jobcode_df = (
        _graph_jobcodes(jobcodes)
            .reset_index()
            .rename(columns={"index": "jobcode_id"})
            .pipe(_split_jobcode_index)
            .fillna("")
            .drop(columns=["jobcode"])
            .drop_duplicates()
    )
    return jobcode_df

def _split_jobcode_index(jobcode_df: pd.DataFrame) -> pd.DataFrame:
    """Returns a DataFrame with jobcode hierarchy split into separate level columns."""
    parts = jobcode_df["jobcode"].str.split(">", expand=True)
    parts.columns = [f"jobcode_{i+1}" for i in range(parts.shape[1])]
    jobcode_df = pd.concat([jobcode_df, parts], axis=1)
    return jobcode_df

def _request_data(
        access_token: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Tuple[List[Dict], List[Dict]]:
    """Returns combined results and metadata retrieved from the TSheets API over a date range."""

    if start_date is None:
        start_date = datetime.date.today() - relativedelta(months=1)
    else:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()

    if end_date is None:
        end_date = datetime.date.today()
    else:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    results = []
    metadata = []

    current_date = start_date
    final_date = end_date

    while current_date <= final_date:
        # Compute this iteration's end of month (or final_date)
        next_month = (current_date.replace(day=1) + relativedelta(months=1))
        month_end = next_month - relativedelta(days=1)
        period_end = month_end if month_end < final_date else final_date

        print(current_date.strftime("%Y-%m-%d"), "→", period_end.strftime("%Y-%m-%d"))

        current_results, current_metadata = _fetch_current_period(
            access_token=access_token,
            start_date=current_date,
            end_date=period_end
        )

        results += current_results
        metadata += current_metadata

        # Move to the first day of the next month
        current_date = next_month

    return results, metadata

def _fetch_current_period(
        access_token: str,
        start_date: datetime.date,
        end_date: datetime.date
    ) -> Tuple[List[Dict], List[Dict]]:
    """Returns results and metadata for a specific monthly period from the TSheets API."""
    current_results = []
    current_metadata = []
    page = 1
    active = True
    while active:
        print(f"Fetching page {page}")
        resp = _fetch_page(
            access_token=access_token,
            start_date=start_date,
            end_date=end_date,
            page=page
        )
        current_results.append(resp["results"])
        current_metadata.append(resp["supplemental_data"])
        page += 1
        active = resp.get("more", False)
    return current_results, current_metadata

def _fetch_page(
        access_token: str,
        start_date: datetime.date,
        end_date: datetime.date,
        page: int
    ) -> Dict:
    """Returns a parsed JSON dictionary containing timesheet data for a single API page."""
    resp = requests.get(
        "https://rest.tsheets.com/api/v1/timesheets",
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "page": page,
        },
    )
    resp = json.loads(resp.text)
    return resp