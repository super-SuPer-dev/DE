"""
Load Bronze TCAS data to SQLite Gold Layer.
Uses staging table pattern to avoid duplicates.
"""
import os
import logging
import pandas as pd
from src.load.init_db import get_connection, get_university_map
from src.config import BRONZE_DIR

log = logging.getLogger(__name__)


def load_tcas():
    log.info("Transforming & Loading TCAS data...")
    conn = get_connection()

    uni_map = get_university_map()

    bronze_file = os.path.join(BRONZE_DIR, "tcas_raw.csv")
    df = pd.read_csv(bronze_file, low_memory=False)

    df["university_id"] = df["สถาบัน"].map(uni_map)
    df = df[df["university_id"].notna()].copy()

    df.rename(columns={
        "รับ": "seats_available",
        "สมัคร": "applicants",
        "ชื่อหลักสูตร": "branch_name",
    }, inplace=True)

    for col in ["seats_available", "applicants"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        else:
            df[col] = 0

    # Use extracted tcas_round_name from extract_tcas (not hardcoded)
    if "tcas_round_name" not in df.columns:
        df["tcas_round_name"] = "Unknown"

    fact_cols = ["university_id", "tcas_round_name", "branch_name",
                 "seats_available", "applicants"]
    for c in fact_cols:
        if c not in df.columns:
            df[c] = None

    fact_df = df[fact_cols]

    try:
        fact_df.to_sql("_staging_tcas", conn, if_exists="replace", index=False)
        conn.execute("DELETE FROM fact_tcas_admission")
        conn.execute("""
            INSERT INTO fact_tcas_admission (
                university_id, tcas_round_name, branch_name,
                seats_available, applicants
            )
            SELECT university_id, tcas_round_name, branch_name,
                   seats_available, applicants
            FROM _staging_tcas
        """)
        conn.execute("DROP TABLE IF EXISTS _staging_tcas")
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM fact_tcas_admission").fetchone()[0]
        log.info("fact_tcas_admission now has %s rows.", f"{count:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_tcas()
