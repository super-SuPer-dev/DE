"""
Transform raw YouTube bronze data into cleaned silver layer.
Saves to data/silver/youtube_clean.csv
"""
import os
import logging
import pandas as pd
from src.config import BRONZE_DIR, SILVER_DIR
from src.load.init_db import get_university_map

log = logging.getLogger(__name__)

BRONZE_FILE = os.path.join(BRONZE_DIR, "youtube_raw.csv")
SILVER_FILE = os.path.join(SILVER_DIR, "youtube_clean.csv")


def clean_youtube() -> pd.DataFrame:
    log.info("Cleaning YouTube data...")
    df = pd.read_csv(BRONZE_FILE)

    uni_map = get_university_map()
    df["university_id"] = df["university_search_term"].map(uni_map)

    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce").fillna(0).astype(int)
    df["like_count"] = pd.to_numeric(df["like_count"], errors="coerce").fillna(0).astype(int)
    df["comment_count"] = pd.to_numeric(df["comment_count"], errors="coerce").fillna(0).astype(int)

    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    df["publish_year"] = df["published_at"].dt.year
    df["publish_month"] = df["published_at"].dt.month
    df["published_at"] = df["published_at"].astype(str)

    before = len(df)
    df = df.drop_duplicates(subset=["video_id"])
    log.info("Dropped %d duplicate video IDs", before - len(df))

    assert df["university_id"].notna().all(), "Some rows have unmapped university_id!"
    assert (df["view_count"] >= 0).all(), "Negative view counts found!"

    os.makedirs(os.path.dirname(SILVER_FILE), exist_ok=True)
    df.to_csv(SILVER_FILE, index=False)
    log.info("Saved %d cleaned records to %s", len(df), SILVER_FILE)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = clean_youtube()
    print(df.head())
