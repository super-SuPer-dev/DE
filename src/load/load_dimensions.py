"""
Load static dimension tables into the SQLite database.
"""
import logging
from src.config import UNIVERSITIES
from src.load.init_db import get_connection

log = logging.getLogger(__name__)


def load_dimensions():
    log.info("Populating dim_university...")
    conn = get_connection()
    try:
        cur = conn.cursor()
        for idx, uni in enumerate(UNIVERSITIES, start=1):
            cur.execute("""
                INSERT INTO dim_university (university_id, name_th)
                VALUES (?, ?)
                ON CONFLICT(university_id) DO UPDATE SET name_th=excluded.name_th
            """, (idx, uni))
        conn.commit()
        count = cur.execute("SELECT COUNT(*) FROM dim_university").fetchone()[0]
        log.info("dim_university now has %d rows.", count)
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_dimensions()
