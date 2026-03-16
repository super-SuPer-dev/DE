"""
Database query functions for the Student Admission Demo.
All data access is centralized here for testability.
"""
import sqlite3
import pandas as pd
from src.config import DB_PATH


def _conn():
    return sqlite3.connect(DB_PATH)


# ─── University & Program Lists ──────────────────────────────────────

def get_university_list() -> pd.DataFrame:
    """Return all universities as DataFrame with university_id, name_th."""
    conn = _conn()
    df = pd.read_sql(
        "SELECT university_id, name_th FROM dim_university ORDER BY name_th",
        conn,
    )
    conn.close()
    return df


def get_programs(university_id: int) -> list[str]:
    """Return distinct program/branch names for a university (from latest TCAS round)."""
    conn = _conn()
    df = pd.read_sql("""
        SELECT DISTINCT branch_name
        FROM fact_tcas_admission
        WHERE university_id = ?
          AND branch_name IS NOT NULL
          AND branch_name != ''
        ORDER BY branch_name
    """, conn, params=(university_id,))
    conn.close()
    return df["branch_name"].tolist()


# ─── Admission Statistics ────────────────────────────────────────────

def get_admission_stats(university_id: int, branch_name: str = None) -> dict:
    """
    Return admission statistics for a university (optionally filtered by program).
    Returns dict with: score_min, score_max, score_mean, avg_applicants,
    avg_seats, competition_ratio, num_rounds.
    """
    conn = _conn()
    where = "WHERE t.university_id = ?"
    params = [university_id]
    if branch_name:
        where += " AND t.branch_name = ?"
        params.append(branch_name)

    df = pd.read_sql(f"""
        SELECT
            MIN(t.score_min) AS score_min_overall,
            MAX(t.score_max) AS score_max_overall,
            AVG(t.score_min) AS score_min_avg,
            AVG(t.score_max) AS score_max_avg,
            AVG(t.score_mean) AS score_mean_avg,
            SUM(t.applicants) AS total_applicants,
            SUM(t.seats_available) AS total_seats,
            COUNT(DISTINCT t.tcas_round_name) AS num_rounds,
            COUNT(*) AS num_programs
        FROM fact_tcas_admission t
        {where}
        AND t.score_min IS NOT NULL
    """, conn, params=params)
    conn.close()

    if df.empty or df.iloc[0]["num_programs"] == 0:
        return None

    row = df.iloc[0]
    total_seats = row["total_seats"] if row["total_seats"] and row["total_seats"] > 0 else 1
    return {
        "score_min": row["score_min_avg"],
        "score_max": row["score_max_avg"],
        "score_min_overall": row["score_min_overall"],
        "score_max_overall": row["score_max_overall"],
        "score_mean": row["score_mean_avg"],
        "total_applicants": int(row["total_applicants"]),
        "total_seats": int(row["total_seats"]),
        "competition_ratio": row["total_applicants"] / total_seats,
        "num_rounds": int(row["num_rounds"]),
        "num_programs": int(row["num_programs"]),
    }


def get_score_range_for_program(university_id: int, branch_name: str) -> dict | None:
    """Return score range for a specific program at a university."""
    conn = _conn()
    df = pd.read_sql("""
        SELECT
            AVG(score_min) AS avg_min,
            AVG(score_max) AS avg_max,
            MIN(score_min) AS abs_min,
            MAX(score_max) AS abs_max,
            COUNT(*) AS n
        FROM fact_tcas_admission
        WHERE university_id = ?
          AND branch_name = ?
          AND score_min IS NOT NULL
    """, conn, params=(university_id, branch_name))
    conn.close()

    if df.empty or df.iloc[0]["n"] == 0:
        return None
    row = df.iloc[0]
    return {
        "avg_min": row["avg_min"],
        "avg_max": row["avg_max"],
        "abs_min": row["abs_min"],
        "abs_max": row["abs_max"],
        "n_records": int(row["n"]),
    }


# ─── Admission Chance Calculation ────────────────────────────────────

def calculate_admission_chance(student_score: float, stats: dict) -> dict:
    """
    Calculate admission chance based on student score vs historical data.
    Returns dict with: chance_pct, level, level_color, description.
    """
    if stats is None or stats.get("score_min") is None:
        return {
            "chance_pct": None,
            "level": "ไม่มีข้อมูล",
            "level_color": "gray",
            "description": "ไม่มีข้อมูลคะแนนเพียงพอสำหรับการประเมิน",
        }

    s_min = stats["score_min"]  # average min score
    s_max = stats["score_max"]  # average max score

    if s_max == s_min:
        s_max = s_min + 1  # avoid division by zero

    # Linear interpolation: score_min -> 30%, score_max -> 95%
    if student_score >= s_max:
        chance = min(98, 90 + (student_score - s_max) / s_max * 50)
        level = "โอกาสสูงมาก"
        color = "green"
        desc = f"คะแนนของคุณ ({student_score:.1f}) สูงกว่าคะแนนสูงสุดเฉลี่ย ({s_max:.1f})"
    elif student_score >= s_min:
        # Interpolate between 30% and 90%
        ratio = (student_score - s_min) / (s_max - s_min)
        chance = 30 + ratio * 60
        level = "มีโอกาส" if chance >= 50 else "โอกาสปานกลาง"
        color = "orange" if chance < 50 else "yellow"
        desc = f"คะแนนของคุณ ({student_score:.1f}) อยู่ระหว่างคะแนนต่ำสุด ({s_min:.1f}) และสูงสุด ({s_max:.1f})"
    else:
        # Below minimum
        gap = s_min - student_score
        chance = max(2, 30 - gap / s_min * 100)
        level = "โอกาสน้อย"
        color = "red"
        desc = f"คะแนนของคุณ ({student_score:.1f}) ต่ำกว่าคะแนนต่ำสุดเฉลี่ย ({s_min:.1f}) อยู่ {gap:.1f} คะแนน"

    return {
        "chance_pct": round(chance, 1),
        "level": level,
        "level_color": color,
        "description": desc,
    }


# ─── Popularity ──────────────────────────────────────────────────────

def get_popularity(university_id: int) -> dict:
    """
    Return popularity metrics for a university.
    Composite score from Wikipedia + MHESI + TCAS (YouTube excluded since only 10 unis have data).
    """
    conn = _conn()

    # Wikipedia pageviews
    wiki = conn.execute(
        "SELECT COALESCE(SUM(pageviews), 0) FROM fact_wikipedia_pageviews WHERE university_id = ?",
        (university_id,)
    ).fetchone()[0]

    # MHESI enrollment (latest)
    mhesi = conn.execute(
        "SELECT COALESCE(MAX(total_students), 0) FROM fact_mhesi_enrollment WHERE university_id = ?",
        (university_id,)
    ).fetchone()[0]

    # TCAS total applicants (popularity proxy)
    tcas_app = conn.execute(
        "SELECT COALESCE(SUM(applicants), 0) FROM fact_tcas_admission WHERE university_id = ?",
        (university_id,)
    ).fetchone()[0]

    # YouTube (optional, only for 10 unis)
    yt = conn.execute(
        "SELECT COALESCE(SUM(view_count), 0) FROM fact_youtube_engagement WHERE university_id = ?",
        (university_id,)
    ).fetchone()[0]

    conn.close()

    return {
        "wikipedia_views": int(wiki),
        "mhesi_students": int(mhesi),
        "tcas_applicants": int(tcas_app),
        "youtube_views": int(yt),
    }


def get_all_popularity_ranks() -> pd.DataFrame:
    """
    Return all universities ranked by composite popularity score.
    Score = normalized(Wikipedia) + normalized(MHESI) + normalized(TCAS applicants).
    """
    conn = _conn()
    df = pd.read_sql("""
        SELECT u.university_id, u.name_th,
            COALESCE(wiki.total_pv, 0) AS wikipedia,
            COALESCE(mhesi.students, 0) AS mhesi,
            COALESCE(tcas.total_app, 0) AS tcas
        FROM dim_university u
        LEFT JOIN (SELECT university_id, SUM(pageviews) AS total_pv
                   FROM fact_wikipedia_pageviews GROUP BY university_id) wiki
            ON wiki.university_id = u.university_id
        LEFT JOIN (SELECT university_id, MAX(total_students) AS students
                   FROM fact_mhesi_enrollment GROUP BY university_id) mhesi
            ON mhesi.university_id = u.university_id
        LEFT JOIN (SELECT university_id, SUM(applicants) AS total_app
                   FROM fact_tcas_admission GROUP BY university_id) tcas
            ON tcas.university_id = u.university_id
    """, conn)
    conn.close()

    # Normalize each metric to 0-100
    for col in ["wikipedia", "mhesi", "tcas"]:
        max_val = df[col].max()
        df[f"{col}_norm"] = (df[col] / max_val * 100) if max_val > 0 else 0

    df["popularity_score"] = (
        df["wikipedia_norm"] + df["mhesi_norm"] + df["tcas_norm"]
    ) / 3

    df["rank"] = df["popularity_score"].rank(ascending=False).astype(int)
    return df.sort_values("rank")


# ─── Competition History ─────────────────────────────────────────────

def get_competition_history(university_id: int) -> pd.DataFrame:
    """Return competition ratio per TCAS round for a university."""
    conn = _conn()
    df = pd.read_sql("""
        SELECT tcas_round_name,
            SUM(applicants) AS applicants,
            SUM(seats_available) AS seats,
            CAST(SUM(applicants) AS REAL) / NULLIF(SUM(seats_available), 0) AS ratio
        FROM fact_tcas_admission
        WHERE university_id = ?
        GROUP BY tcas_round_name
        ORDER BY tcas_round_name
    """, conn, params=(university_id,))
    conn.close()
    return df
