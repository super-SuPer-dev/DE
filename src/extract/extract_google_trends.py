"""
Extract Google Trends interest data for Thai universities.
Uses pytrends library (free, no API key needed).
Saves raw data to data/bronze/google_trends_raw.csv

Strategy: Query one university at a time with generous delays (10s+)
to avoid Google's 429 rate limits. Uses progress tracking for resume.
"""
import os
import time
import json
import logging
import random
import pandas as pd
from pytrends.request import TrendReq
from src.config import UNIVERSITIES, BRONZE_DIR

log = logging.getLogger(__name__)

STAGING_FILE = os.path.join(BRONZE_DIR, "google_trends_raw.csv")
PROGRESS_FILE = os.path.join(BRONZE_DIR, "google_trends_progress.json")

# Multiple timeframes for more data points
TIMEFRAMES = [
    ("today 12-m", "12m"),
    ("today 3-m", "3m"),
    ("2024-01-01 2024-06-30", "H1_2024"),
    ("2024-07-01 2024-12-31", "H2_2024"),
    ("2025-01-01 2025-06-30", "H1_2025"),
    ("2023-01-01 2023-12-31", "2023"),
]

KEYWORD_TEMPLATES = [
    ("{uni}", "base"),
    ("เรียน {uni}", "study"),
    ("สมัคร {uni}", "apply"),
]

DELAY_MIN = 10
DELAY_MAX = 20


def _shorten_name(uni: str) -> str:
    short = uni.replace("มหาวิทยาลัย", "").replace("สถาบัน", "").strip()
    return short if short else uni


def extract_google_trends() -> pd.DataFrame:
    if os.path.exists(STAGING_FILE):
        log.info("Loading Google Trends from staging: %s", STAGING_FILE)
        return pd.read_csv(STAGING_FILE)

    log.info("Fetching Google Trends data for %d universities...", len(UNIVERSITIES))

    # Load progress
    done_queries = set()
    all_rows = []
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            progress = json.load(f)
        done_queries = set(progress.get("done", []))
        all_rows = progress.get("rows", [])
        log.info("Resuming: %d queries done, %d rows collected.", len(done_queries), len(all_rows))

    pytrends = TrendReq(hl="th", tz=420)
    consecutive_errors = 0

    for uni in UNIVERSITIES:
        short = _shorten_name(uni)

        for template, tpl_name in KEYWORD_TEMPLATES:
            keyword = template.format(uni=short)

            for timeframe, tf_name in TIMEFRAMES:
                query_key = f"{keyword}|{tf_name}"
                if query_key in done_queries:
                    continue

                if consecutive_errors >= 3:
                    log.warning("Too many consecutive errors. Saving progress.")
                    _save_progress(done_queries, all_rows)
                    log.info("Run again later to resume. %d rows collected so far.", len(all_rows))
                    if all_rows:
                        df = pd.DataFrame(all_rows)
                        os.makedirs(os.path.dirname(STAGING_FILE), exist_ok=True)
                        df.to_csv(STAGING_FILE, index=False)
                        return df
                    return pd.DataFrame()

                for attempt in range(3):
                    try:
                        pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo="TH")
                        interest_df = pytrends.interest_over_time()

                        if not interest_df.empty:
                            if "isPartial" in interest_df.columns:
                                interest_df = interest_df.drop(columns=["isPartial"])
                            interest_df = interest_df.reset_index()

                            for _, row in interest_df.iterrows():
                                all_rows.append({
                                    "date": str(row["date"]),
                                    "interest_score": int(row[keyword]),
                                    "keyword": keyword,
                                    "university_name": uni,
                                    "timeframe": tf_name,
                                })

                        done_queries.add(query_key)
                        consecutive_errors = 0
                        break

                    except Exception as e:
                        if "429" in str(e) and attempt < 2:
                            wait = 60 * (attempt + 1)
                            log.info("  Rate limited on '%s'. Waiting %ds...", keyword, wait)
                            time.sleep(wait)
                        else:
                            log.warning("  Failed '%s' (%s): %s", keyword, tf_name, e)
                            consecutive_errors += 1
                            done_queries.add(query_key)
                            break

                # Random delay between requests
                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                time.sleep(delay)

            # Save progress periodically
            _save_progress(done_queries, all_rows)

        log.info("  %s: done (%d rows collected)", short, len(all_rows))

    if not all_rows:
        log.warning("No Google Trends data collected.")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows).drop_duplicates()
    os.makedirs(os.path.dirname(STAGING_FILE), exist_ok=True)
    df.to_csv(STAGING_FILE, index=False)
    log.info("Saved %d Google Trends records to %s", len(df), STAGING_FILE)

    # Clean up
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

    return df


def _save_progress(done: set, rows: list):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"done": list(done), "rows": rows}, f, ensure_ascii=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = extract_google_trends()
    if not df.empty:
        print(df.head())
    print(f"Total: {len(df)} records")
