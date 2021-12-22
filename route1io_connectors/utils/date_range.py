

import datetime
from typing import List, Tuple

def calculate_date_ranges(total_start_date, total_end_date,
                          days_incr: int = 30) -> List[Tuple["datetime.date", "datetime.date"]]:
    """Return a list of tuples with date ranges. DAILY granular pulls can only be for
    90 days worth of data so we have to do multiple pulls"""
    date_ranges = []
    active = True
    sub_start_time = total_start_date
    while active:
        sub_end_time = sub_start_time + datetime.timedelta(days=days_incr)
        if sub_end_time >= total_end_date:
            sub_end_time = total_end_date
            active = False
        date_ranges.append((sub_start_time, sub_end_time))
        sub_start_time = sub_end_time + datetime.timedelta(days=1)
    return date_ranges