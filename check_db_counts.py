import sqlite3
import os

DB_PATH = "data/gold/university.db"

def check_db():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} does not exist.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    tables = [
        "dim_university",
        "dim_faculty",
        "fact_tcas_admission",
        "fact_mhesi_enrollment",
        "fact_youtube_engagement",
        "fact_google_trends",
        "fact_wikipedia_pageviews",
    ]
    
    print(f"{'Table':<25} | {'Count':>10}")
    print("-" * 40)
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table:<25} | {count:>10}")
        except sqlite3.OperationalError as e:
            print(f"{table:<25} | Error: {e}")
            
    conn.close()

if __name__ == "__main__":
    check_db()
