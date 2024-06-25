"""SA360 connectors

This module contains code for accessing data from Search Ads 360.
"""

from typing import Optional

import pandas as pd

def get_sa360_data(access_token: str, account_id: str, query: str, login_customer_id: Optional[str] = None) -> "pd.DataFrame":
    """Return SA360 data requested from the SA360 Reporting API and processed
    into a pd.DataFrame
    """
    pass