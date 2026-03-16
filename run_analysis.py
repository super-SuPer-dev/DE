import sqlite3
import pandas as pd

DB_PATH = "data/gold/university.db"

def run_analysis():
    conn = sqlite3.connect(DB_PATH)
    
    queries = {
        "Top 5 Universities by YouTube Views": """
            SELECT
                u.name_th,
                SUM(y.view_count) AS total_views
            FROM fact_youtube_engagement y
            JOIN dim_university u ON y.university_id = u.university_id
            GROUP BY u.name_th
            ORDER BY total_views DESC
            LIMIT 5;
        """,
        "TCAS Competition Ratio (Sample)": """
            SELECT
                u.name_th,
                CAST(SUM(t.applicants) AS REAL) / NULLIF(SUM(t.seats_available), 0) AS competition_ratio
            FROM fact_tcas_admission t
            JOIN dim_university u ON t.university_id = u.university_id
            GROUP BY u.name_th
            ORDER BY competition_ratio DESC
            LIMIT 5;
        """
    }
    
    for title, sql in queries.items():
        print(f"\n--- {title} ---")
        df = pd.read_sql(sql, conn)
        print(df)
        
    conn.close()

if __name__ == "__main__":
    run_analysis()
