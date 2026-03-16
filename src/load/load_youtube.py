"""
Load cleaned YouTube silver data into the SQLite Gold layer.
Inserts into fact_youtube_engagement table.
"""
import os
import logging
import pandas as pd
from src.load.init_db import get_connection
from src.config import SILVER_DIR

log = logging.getLogger(__name__)

SILVER_FILE = os.path.join(SILVER_DIR, "youtube_clean.csv")

COLUMNS = [
    "university_id", "video_id", "title", "channel_title",
    "published_at", "publish_year", "publish_month",
    "view_count", "like_count", "comment_count", "search_query",
]


def load_youtube():
    log.info("Loading YouTube data into SQLite...")
    df = pd.read_csv(SILVER_FILE)
    df = df[COLUMNS]

    conn = get_connection()
    try:
        df.to_sql("_staging_youtube", conn, if_exists="replace", index=False)
        conn.execute("""
            INSERT OR IGNORE INTO fact_youtube_engagement (
                university_id, video_id, title, channel_title,
                published_at, publish_year, publish_month,
                view_count, like_count, comment_count, search_query
            )
            SELECT
                university_id, video_id, title, channel_title,
                published_at, publish_year, publish_month,
                view_count, like_count, comment_count, search_query
            FROM _staging_youtube
        """)
        conn.execute("DROP TABLE IF EXISTS _staging_youtube")
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM fact_youtube_engagement").fetchone()[0]
        log.info("fact_youtube_engagement now has %s rows.", f"{count:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_youtube()
