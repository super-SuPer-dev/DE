"""
Load Wikipedia pageview data into the SQLite Gold layer.
"""
import os
import logging
import pandas as pd
from src.load.init_db import get_connection, get_university_map
from src.config import BRONZE_DIR

log = logging.getLogger(__name__)

BRONZE_FILE = os.path.join(BRONZE_DIR, "wikipedia_raw.csv")


def load_wikipedia():
    if not os.path.exists(BRONZE_FILE):
        log.warning("Wikipedia bronze file not found: %s. Skipping.", BRONZE_FILE)
        return

    log.info("Loading Wikipedia pageview data into SQLite...")
    df = pd.read_csv(BRONZE_FILE)

    if df.empty:
        log.warning("No Wikipedia data to load.")
        return

    uni_map = get_university_map()
    df["university_id"] = df["university_name"].map(uni_map)
    df = df.dropna(subset=["university_id"])

    df["pageviews"] = pd.to_numeric(df["pageviews"], errors="coerce").fillna(0).astype(int)

    # Ensure date column exists
    if "date" not in df.columns:
        df["date"] = df.apply(
            lambda r: f"{int(r['year']):04d}-{int(r['month']):02d}-{int(r.get('day', 1)):02d}", axis=1
        )
    if "day" not in df.columns:
        df["day"] = 1

    fact_df = df[["university_id", "article_title", "date", "year", "month", "day", "pageviews"]]

    conn = get_connection()
    try:
        fact_df.to_sql("_staging_wiki", conn, if_exists="replace", index=False)
        conn.execute("DELETE FROM fact_wikipedia_pageviews")
        conn.execute("""
            INSERT INTO fact_wikipedia_pageviews (
                university_id, article_title, date, year, month, day, pageviews
            )
            SELECT university_id, article_title, date, year, month, day, pageviews
            FROM _staging_wiki
        """)
        conn.execute("DROP TABLE IF EXISTS _staging_wiki")
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM fact_wikipedia_pageviews").fetchone()[0]
        log.info("fact_wikipedia_pageviews now has %s rows.", f"{count:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_wikipedia()
