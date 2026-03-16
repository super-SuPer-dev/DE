"""
Extract YouTube video data for Thai universities using YouTube Data API v3.
Saves raw data to data/raw/youtube_raw.csv

Requirements:
  - Get a free API key at https://console.cloud.google.com/
  - Enable "YouTube Data API v3" in your project
  - Set YOUTUBE_API_KEY in a .env file or environment variable
"""
import os
import time
import json
import csv as csv_mod
import logging
import requests
import pandas as pd
from src.config import UNIVERSITIES, YOUTUBE_API_KEY, RAW_DIR

log = logging.getLogger(__name__)

STAGING_FILE = os.path.join(RAW_DIR, "youtube_raw.csv")
API_KEY = YOUTUBE_API_KEY

SEARCH_TEMPLATES = [
    "รีวิว {uni}",
    "เรียนที่ {uni} เป็นยังไง",
    "ชีวิตนักศึกษา {uni}",
    "experience at {uni}",
    "student life {uni}",
    "campus tour {uni}",
    "tour {uni}",
    "{uni} campus",
    "inside {uni}",
    "สอบเข้า {uni}",
    "TCAS {uni}",
    "admission {uni}",
    "how to get into {uni}",
    "เรียน {uni}",
    "study at {uni}",
    "{uni} university review",
    "vlog {uni}",
    "day in the life {uni}",
    "student vlog {uni}",
    "{uni} engineering",
    "{uni} medicine",
    "{uni} business school",
    "มหาวิทยาลัย {uni}",
    "{uni} มหาลัย",
    "{uni} รีวิว",
]

MAX_RESULTS_PER_QUERY = 100
SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"


def check_api_response(resp: dict, context: str = "") -> bool:
    """Return True if response is OK. Log and return False if quota/error."""
    if "error" in resp:
        code = resp["error"].get("code")
        msg = resp["error"].get("message", "")
        if code == 403 or "quota" in msg.lower():
            log.warning("QUOTA EXHAUSTED%s", f" ({context})" if context else "")
            log.info("YouTube API free tier = 10,000 units/day. Reset at midnight Pacific.")
        else:
            log.error("API Error %s: %s", code, msg)
        return False
    return True


def extract_youtube() -> pd.DataFrame:
    PROGRESS_FILE = STAGING_FILE.replace(".csv", "_progress.json")

    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            progress = json.load(f)
        done_queries = set(progress.get("done_queries", []))
        log.info("Resuming: %d queries already done.", len(done_queries))
    else:
        done_queries = set()

    total_queries = len(UNIVERSITIES) * len(SEARCH_TEMPLATES)
    if os.path.exists(STAGING_FILE) and len(done_queries) >= total_queries:
        df = pd.read_csv(STAGING_FILE)
        log.info("All queries done. Loading from staging: %s (%s records)", STAGING_FILE, f"{len(df):,}")
        return df

    if not API_KEY:
        raise ValueError("YOUTUBE_API_KEY not set. Add it to your .env file.")

    log.info("Starting YouTube API extraction...")
    log.info("Budget: 10,000 units/day | Remaining queries: %d", total_queries - len(done_queries))

    os.makedirs(os.path.dirname(STAGING_FILE), exist_ok=True)
    write_header = not os.path.exists(STAGING_FILE)
    csv_out = open(STAGING_FILE, "a", encoding="utf-8", newline="")

    fieldnames = ["video_id", "title", "published_at", "channel_title",
                  "view_count", "like_count", "comment_count",
                  "university_search_term", "search_query"]
    writer = csv_mod.DictWriter(csv_out, fieldnames=fieldnames)
    if write_header:
        writer.writeheader()

    total_written = 0
    quota_exhausted = False

    try:
        for uni in UNIVERSITIES:
            if quota_exhausted:
                break
            for template in SEARCH_TEMPLATES:
                if quota_exhausted:
                    break
                query = template.format(uni=uni)

                if query in done_queries:
                    continue

                log.debug("  Searching: '%s'", query)
                params = {
                    "part": "id",
                    "q": query,
                    "type": "video",
                    "maxResults": 50,
                    "key": API_KEY,
                    "relevanceLanguage": "th",
                }
                resp = requests.get(SEARCH_URL, params=params, timeout=15).json()

                if not check_api_response(resp, context=query):
                    quota_exhausted = True
                    break

                video_ids = [
                    item["id"]["videoId"]
                    for item in resp.get("items", [])
                    if item.get("id", {}).get("kind") == "youtube#video"
                ]

                if video_ids:
                    stat_resp = requests.get(VIDEOS_URL, params={
                        "part": "snippet,statistics",
                        "id": ",".join(video_ids),
                        "key": API_KEY,
                    }, timeout=15).json()

                    if not check_api_response(stat_resp, context="videos.list"):
                        quota_exhausted = True
                        break

                    for item in stat_resp.get("items", []):
                        snippet = item.get("snippet", {})
                        stats = item.get("statistics", {})
                        writer.writerow({
                            "video_id": item["id"],
                            "title": snippet.get("title"),
                            "published_at": snippet.get("publishedAt"),
                            "channel_title": snippet.get("channelTitle"),
                            "view_count": stats.get("viewCount"),
                            "like_count": stats.get("likeCount"),
                            "comment_count": stats.get("commentCount"),
                            "university_search_term": uni,
                            "search_query": query,
                        })
                        total_written += 1
                    csv_out.flush()

                done_queries.add(query)
                with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                    json.dump({"done_queries": list(done_queries)}, f)

                time.sleep(0.5)

            log.info("  Collected %s total records so far...", f"{total_written:,}")
    finally:
        csv_out.close()

    if total_written == 0 and not os.path.exists(STAGING_FILE):
        raise RuntimeError("No records collected. API quota likely exhausted.")

    df = pd.read_csv(STAGING_FILE)
    log.info("Saved %s records to %s", f"{len(df):,}", STAGING_FILE)

    if quota_exhausted:
        log.warning("Quota hit after %d queries. Progress saved. Run again tomorrow.", len(done_queries))

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = extract_youtube()
    print(df.head())
    print(f"Total: {len(df)} records")
