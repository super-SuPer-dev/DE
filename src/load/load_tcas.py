"""
Load TCAS data to SQLite database.
Uses staging table pattern to avoid duplicates.
Includes score columns (score_max, score_min, score_mean, score_sd).
"""
import os
import logging
import pandas as pd
from src.load.init_db import get_connection, get_university_map
from src.config import RAW_DIR

log = logging.getLogger(__name__)

# Different TCAS years use different Thai column names for scores.
# We coalesce them into a single value per row.
SCORE_MAX_CANDIDATES = [
    "คะแนนสูงสุด",
    "คะแนนสูงสุด ประมวลผลครั้งที่ 1",
    "คะแนนสูงสุด หลังประมวลผลรอบ 2",
    "คะแนนสูงสุด ประมวลผลครั้งที่ 2",
]
SCORE_MIN_CANDIDATES = [
    "คะแนนต่ำสุด",
    "คะแนนต่ำสุด ประมวลผลครั้งที่ 1",
    "คะแนนต่ำสุด หลังประมวลผลรอบ 2",
    "คะแนนต่ำสุด ประมวลผลครั้งที่ 2",
]


def _coalesce(row, candidates):
    """Return the first non-null value from a list of column candidates."""
    for col in candidates:
        if col in row.index and pd.notna(row[col]):
            return float(row[col])
    return None


def load_tcas():
    log.info("Transforming & Loading TCAS data...")
    conn = get_connection()

    uni_map = get_university_map()

    raw_file = os.path.join(RAW_DIR, "tcas_raw.csv")
    df = pd.read_csv(raw_file, low_memory=False)

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

    # Coalesce score columns from multiple Thai column name variants
    df["score_max"] = df.apply(lambda r: _coalesce(r, SCORE_MAX_CANDIDATES), axis=1)
    df["score_min"] = df.apply(lambda r: _coalesce(r, SCORE_MIN_CANDIDATES), axis=1)

    # Mean and SD are less common but include when available
    if "คะแนนเฉลี่ย" in df.columns:
        df["score_mean"] = pd.to_numeric(df["คะแนนเฉลี่ย"], errors="coerce")
    else:
        df["score_mean"] = None

    if "SD" in df.columns:
        df["score_sd"] = pd.to_numeric(df["SD"], errors="coerce")
    else:
        df["score_sd"] = None

    fact_cols = ["university_id", "tcas_round_name", "branch_name",
                 "seats_available", "applicants",
                 "score_max", "score_min", "score_mean", "score_sd"]
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
                seats_available, applicants,
                score_max, score_min, score_mean, score_sd
            )
            SELECT university_id, tcas_round_name, branch_name,
                   seats_available, applicants,
                   score_max, score_min, score_mean, score_sd
            FROM _staging_tcas
        """)
        conn.execute("DROP TABLE IF EXISTS _staging_tcas")
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM fact_tcas_admission").fetchone()[0]
        score_count = conn.execute(
            "SELECT COUNT(*) FROM fact_tcas_admission WHERE score_min IS NOT NULL"
        ).fetchone()[0]
        log.info("fact_tcas_admission: %s rows (%s with scores).",
                 f"{count:,}", f"{score_count:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_tcas()
