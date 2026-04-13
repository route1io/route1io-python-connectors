import os

import requests

def extract(config):
    # Build destination filepath
    fpath = os.path.join(
        os.environ.get("ROUTE1_WORKING_DIR"),
        config["filename"]
    )

    # Ensure local directories exist
    os.makedirs(os.path.dirname(fpath), exist_ok=True)

    # Execute GET request
    response = requests.get(config["url"])

    if response.status_code != 200:
        raise ValueError(
            f"HTTP extraction failed for {config.get('name', '<unknown>')} "
            f"with status code {response.status_code}"
        )

    # Write response content to file
    with open(fpath, "wb") as f:
        f.write(response.content)

    return fpath