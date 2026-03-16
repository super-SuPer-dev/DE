"""
Load cleaned Google Trends data into the SQLite Gold layer.
"""
import os
import logging
import pandas as pd
from src.load.init_db import get_connection
from src.config import SILVER_DIR

log = logging.getLogger(__name__)

SILVER_FILE = os.path.join(SILVER_DIR, "google_trends_clean.csv")


def load_google_trends():
    if not os.path.exists(SILVER_FILE):
        log.warning("Silver file not found: %s. Skipping.", SILVER_FILE)
        return

    log.info("Loading Google Trends data into SQLite...")
    df = pd.read_csv(SILVER_FILE)

    if df.empty:
        log.warning("No Google Trends data to load.")
        return

    fact_df = df[["university_id", "date", "interest_score", "keyword"]]

    conn = get_connection()
    try:
        fact_df.to_sql("_staging_trends", conn, if_exists="replace", index=False)
        conn.execute("DELETE FROM fact_google_trends")
        conn.execute("""
            INSERT INTO fact_google_trends (
                university_id, date, interest_score, keyword
            )
            SELECT university_id, date, interest_score, keyword
            FROM _staging_trends
        """)
        conn.execute("DROP TABLE IF EXISTS _staging_trends")
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM fact_google_trends").fetchone()[0]
        log.info("fact_google_trends now has %s rows.", f"{count:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_google_trends()
