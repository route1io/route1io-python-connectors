import os

from route1io_connectors.google import gsheets, credentials

def extract(config):
    # Connect to S3
    gsheets_credentials = credentials.refresh_token_from_credentials(
        refresh_token=os.environ.get("GCP_REFRESH_TOKEN"),
        client_id=os.environ.get("GCP_CLIENT_ID"),
        client_secret=os.environ.get("GCP_CLIENT_SECRET")
    )
    client = gsheets.connect_to_gsheets(gsheets_credentials)

    fpath = os.path.join(
        os.environ.get("ROUTE1_WORKING_DIR"),
        config['filename']
    )

    # Ensure local directories exist for filename
    os.makedirs(os.path.dirname(fpath), exist_ok=True)

    # Download
    gsheets.download_gsheets_spreadsheet(
        client,
        fpath,
        config["sheet_id"],
        config["sheet_name"]
    )
    return fpath
