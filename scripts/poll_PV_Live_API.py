#!/usr/bin/env python3

"""
Put this in your cron tab to poll the PV Live API every 15 minutes, and log the output to syslog:

crontab -e
*/15 * * * * ~/miniconda3/bin/conda run -n analysis_of_pv_live ~/dev/ocf/analysis_of_pv_live/scripts/poll_PV_Live_API.py 2>&1 | logger -t pvlive
"""

from datetime import datetime
from pathlib import Path
from typing import Final

import pandas as pd
import pytz
from pvlive_api import PVLive

START_DATE: Final[datetime] = datetime(2023, 4, 28, tzinfo=pytz.utc)
END_DATE: Final[datetime] = pytz.UTC.localize(datetime.utcnow())
OUTPUT_PATH: Path = Path("/home/jack/data/PV/PV_Live/poll_API_every_15_minutes/")


def download_all_gsps_from_pv_live(start: datetime, end: datetime) -> pd.DataFrame:
    df_for_all_gsps = []
    pvl = PVLive()
    for gsp_id in pvl.gsp_ids:
        data_for_gsp = pvl.between(
            start=start, end=end, entity_type="gsp", entity_id=gsp_id, dataframe=True,
            extra_fields="updated_gmt")
        data_for_gsp = data_for_gsp.set_index(["datetime_gmt", "gsp_id"])
        df_for_all_gsps.append(data_for_gsp)
    return pd.concat(df_for_all_gsps).sort_index()



if __name__ == "__main__":
    print("Downloading data from PV Live API...")
    date_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    df = download_all_gsps_from_pv_live(start=START_DATE, end=END_DATE)
    filename = "pvlive-{}.parquet".format(date_str)
    full_filename = OUTPUT_PATH / filename
    print(f"Saving to {full_filename}")
    df.to_parquet(full_filename, compression="brotli")
    print("Done!")
