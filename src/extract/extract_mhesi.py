"""
Extract MHESI data from data.mhesi.go.th Open Data API.
Saves raw data to data/bronze/mhesi_raw.csv
"""
import os
import logging
import requests
import pandas as pd
from src.config import BRONZE_DIR

log = logging.getLogger(__name__)

STAGING_FILE = os.path.join(BRONZE_DIR, "mhesi_raw.csv")
BASE_URL = "https://data.mhesi.go.th/api/3/action/datastore_search"
RESOURCE_ID = "e6e2fad4-4018-4ac6-93eb-1370aeef8ed1"
LIMIT = 5000


def extract_mhesi() -> pd.DataFrame:
    if os.path.exists(STAGING_FILE):
        log.info("Loading from staging: %s", STAGING_FILE)
        return pd.read_csv(STAGING_FILE)

    log.info("Fetching data from MHESI API...")
    all_records = []
    offset = 0

    while True:
        url = f"{BASE_URL}?resource_id={RESOURCE_ID}&limit={LIMIT}&offset={offset}"
        response = requests.get(url, timeout=30).json()
        records = response["result"]["records"]
        if not records:
            break
        all_records.extend(records)
        offset += LIMIT
        log.info("  Fetched %d records so far...", len(all_records))
        if len(records) < LIMIT:
            break

    df = pd.DataFrame(all_records)
    assert len(df) >= 5000, f"Too few MHESI records: {len(df)}"

    os.makedirs(os.path.dirname(STAGING_FILE), exist_ok=True)
    df.to_csv(STAGING_FILE, index=False)
    log.info("Saved %d records to %s", len(df), STAGING_FILE)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = extract_mhesi()
    print(df.head())
    print(f"Total: {len(df)} records, {len(df.columns)} columns")
