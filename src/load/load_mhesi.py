"""
Load Bronze MHESI data to SQLite Gold Layer.
"""
import os
import logging
import json
import pandas as pd
from src.load.init_db import get_connection, get_university_map
from src.config import BRONZE_DIR

log = logging.getLogger(__name__)


def load_mhesi():
    log.info("Transforming & Loading MHESI data...")
    conn = get_connection()

    uni_map = get_university_map()

    bronze_file = os.path.join(BRONZE_DIR, "mhesi_raw.csv")
    df = pd.read_csv(bronze_file, low_memory=False)

    uni_col = next((c for c in df.columns if "UNIV" in c and "TH" in c), None)
    if uni_col is None:
        uni_col = "UNIV_NAME_TH" if "UNIV_NAME_TH" in df.columns else df.columns[0]

    df["university_id"] = df[uni_col].map(uni_map)
    df = df[df["university_id"].notna()].copy()

    std_col = "ALL STD" if "ALL STD" in df.columns else "TOTAL_STD"
    if std_col in df.columns:
        df["total_students"] = pd.to_numeric(df[std_col], errors="coerce").fillna(0).astype(int)
    else:
        df["total_students"] = 0

    year_col = "ACADEMIC_YEAR"
    if year_col in df.columns:
        df["academic_year"] = pd.to_numeric(df[year_col], errors="coerce").fillna(2566).astype(int)
    else:
        df["academic_year"] = 2566

    df["raw_data"] = df.apply(lambda row: json.dumps(row.to_dict(), ensure_ascii=False, default=str), axis=1)

    fact_cols = ["university_id", "academic_year", "total_students", "raw_data"]
    fact_df = df[fact_cols]

    try:
        fact_df.to_sql("_staging_mhesi", conn, if_exists="replace", index=False)
        conn.execute("DELETE FROM fact_mhesi_enrollment")
        conn.execute("""
            INSERT INTO fact_mhesi_enrollment (
                university_id, academic_year, total_students, raw_data
            )
            SELECT university_id, academic_year, total_students, raw_data
            FROM _staging_mhesi
        """)
        conn.execute("DROP TABLE IF EXISTS _staging_mhesi")
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM fact_mhesi_enrollment").fetchone()[0]
        log.info("fact_mhesi_enrollment now has %s rows.", f"{count:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_mhesi()
