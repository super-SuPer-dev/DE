"""
Extract Wikipedia pageview data for Thai universities.
Uses the Wikimedia REST API (free, no API key needed).
Saves raw data to data/raw/wikipedia_raw.csv

Uses DAILY granularity to get 5,000+ records.
API docs: https://wikimedia.org/api/rest_v1/
"""
import os
import time
import logging
import requests
import pandas as pd
from src.config import UNIVERSITIES, WIKIPEDIA_ARTICLES, RAW_DIR

log = logging.getLogger(__name__)

STAGING_FILE = os.path.join(RAW_DIR, "wikipedia_raw.csv")
BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
HEADERS = {"User-Agent": "UniversityPopularityDE/1.0 (student project)"}


def _fetch_daily_pageviews(article_title: str, project: str = "th.wikipedia") -> list:
    """Fetch DAILY pageview data for a Wikipedia article (last 2 years)."""
    encoded_title = requests.utils.quote(article_title)
    url = (
        f"{BASE_URL}/{project}/all-access/all-agents/"
        f"{encoded_title}/daily/20240101/20260316"
    )

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 404:
            log.debug("  Article not found: %s", article_title)
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("items", [])
    except requests.RequestException as e:
        log.warning("  Error fetching %s: %s", article_title, e)
        return []


def extract_wikipedia() -> pd.DataFrame:
    if os.path.exists(STAGING_FILE):
        log.info("Loading Wikipedia pageviews from staging: %s", STAGING_FILE)
        return pd.read_csv(STAGING_FILE)

    log.info("Fetching Wikipedia daily pageview data for %d universities...", len(UNIVERSITIES))
    all_rows = []

    for uni_name in UNIVERSITIES:
        article_title = WIKIPEDIA_ARTICLES.get(uni_name, uni_name)

        log.info("  Fetching: %s", article_title)
        items = _fetch_daily_pageviews(article_title)

        for item in items:
            timestamp = item.get("timestamp", "")
            year = int(timestamp[:4]) if len(timestamp) >= 4 else 0
            month = int(timestamp[4:6]) if len(timestamp) >= 6 else 0
            day = int(timestamp[6:8]) if len(timestamp) >= 8 else 0

            all_rows.append({
                "university_name": uni_name,
                "article_title": article_title,
                "year": year,
                "month": month,
                "day": day,
                "date": f"{year:04d}-{month:02d}-{day:02d}",
                "pageviews": item.get("views", 0),
            })

        time.sleep(0.2)

    df = pd.DataFrame(all_rows)

    if df.empty:
        log.warning("No Wikipedia data collected.")
        return df

    os.makedirs(os.path.dirname(STAGING_FILE), exist_ok=True)
    df.to_csv(STAGING_FILE, index=False)
    log.info("Saved %d Wikipedia daily pageview records to %s", len(df), STAGING_FILE)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = extract_wikipedia()
    if not df.empty:
        print(df.head(20))
    print(f"Total: {len(df)} records")
