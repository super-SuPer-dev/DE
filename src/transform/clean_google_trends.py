"""
Transform raw Google Trends bronze data into cleaned silver layer.
Saves to data/clean/google_trends_clean.csv
"""
import os
import logging
import pandas as pd
from src.config import RAW_DIR, CLEAN_DIR
from src.load.init_db import get_university_map

log = logging.getLogger(__name__)

RAW_FILE = os.path.join(RAW_DIR, "google_trends_raw.csv")
CLEAN_FILE = os.path.join(CLEAN_DIR, "google_trends_clean.csv")


def clean_google_trends() -> pd.DataFrame:
    log.info("Cleaning Google Trends data...")

    if not os.path.exists(RAW_FILE):
        log.warning("Bronze file not found: %s", RAW_FILE)
        return pd.DataFrame()

    df = pd.read_csv(RAW_FILE)
    if df.empty:
        return df

    uni_map = get_university_map()
    df["university_id"] = df["university_name"].map(uni_map)

    # Drop rows where university couldn't be mapped
    unmapped = df["university_id"].isna().sum()
    if unmapped > 0:
        log.warning("Dropping %d rows with unmapped university names", unmapped)
    df = df.dropna(subset=["university_id"])

    df["interest_score"] = pd.to_numeric(df["interest_score"], errors="coerce").fillna(0).astype(int)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").astype(str)

    # Drop duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["university_id", "date", "keyword"])
    log.info("Dropped %d duplicates", before - len(df))

    os.makedirs(os.path.dirname(CLEAN_FILE), exist_ok=True)
    df.to_csv(CLEAN_FILE, index=False)
    log.info("Saved %d cleaned records to %s", len(df), CLEAN_FILE)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = clean_google_trends()
    print(df.head())
