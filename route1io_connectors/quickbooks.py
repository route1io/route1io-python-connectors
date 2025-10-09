import json
import datetime

import requests
import networkx as nx
import pandas as pd
from dateutil.relativedelta import relativedelta

def request_access_token_refresh(access_token, refresh_token, client_id, client_secret) -> str:
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

def get_timesheets(access_token, start_date, end_date) -> pd.DataFrame:
    results, metadata = _request_data(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date
    )
    metadata = _process_metadata(metadata)
    df = _process_results(results, metadata)
    return df

def _process_results(results, metadata):
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

def _process_metadata(metadata):
    users = [page['users'] for page in metadata]
    jobcodes = [page['jobcodes'] for page in metadata]

    users_df = _create_users_lookup(users)
    jobcodes_df = _create_jobcode_lookup(jobcodes)

    return users_df, jobcodes_df

def _create_users_lookup(users):
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

def _graph_jobcodes(jobcodes):
    G = _graph_jobcodes(jobcodes)
    leaf_lookup = _get_leaf_lookup(G)
    jobcode_df = pd.DataFrame.from_dict(
        leaf_lookup,
        orient='index',
        columns=['jobcode']
    )
    return jobcode_df

def _get_leaf_lookup(G):
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

def _graph_jobcodes(jobcodes):
    G = nx.DiGraph()
    for page in jobcodes:
        for jobcode_id, jobcode in page.items():
            job_id = str(jobcode_id)
            parent_id = str(jobcode.get("parent_id")) if jobcode.get("parent_id") else None
            G.add_node(job_id, **jobcode)
            if parent_id and parent_id not in ("None", "0", "", "null"):
                G.add_edge(parent_id, job_id)
    return G

def _create_jobcode_lookup(jobcodes):
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

def _split_jobcode_index(jobcode_df):
    parts = jobcode_df["jobcode"].str.split(">", expand=True)
    parts.columns = [f"jobcode_{i+1}" for i in range(parts.shape[1])]
    jobcode_df = pd.concat([jobcode_df, parts], axis=1)
    return jobcode_df

def _request_data(access_token, start_date=None, end_date=None):

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

        page = 1
        active = True
        while active:
            print(f"Fetching page {page}")
            resp = _fetch_page(
                access_token=access_token,
                start_date=current_date,
                end_date=period_end,
                page=page
            )
            results.append(resp["results"])
            metadata.append(resp["supplemental_data"])
            page += 1
            active = resp.get("more", False)

        # Move to the first day of the next month
        current_date = next_month

    return results, metadata

def _fetch_page(access_token, start_date, end_date, page):
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