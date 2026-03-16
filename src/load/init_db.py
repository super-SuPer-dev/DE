"""
Initialize the SQLite Gold layer database.
Creates all dimension and fact tables from sql/create_tables.sql.
"""
import os
import logging
import sqlite3
import pandas as pd
from src.config import DB_PATH, SQL_DIR

log = logging.getLogger(__name__)


def init_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    log.info("Connected to %s", DB_PATH)

    sql_file = os.path.join(SQL_DIR, "create_tables.sql")
    with open(sql_file, "r", encoding="utf-8") as f:
        sql_script = f.read()

    conn.executescript(sql_script)
    conn.commit()
    log.info("All tables created successfully.")
    return conn


def get_connection() -> sqlite3.Connection:
    """Return a connection to the existing database."""
    return sqlite3.connect(DB_PATH)


def get_university_map() -> dict:
    """Return a dict mapping Thai university name -> university_id from the Gold DB."""
    conn = get_connection()
    df = pd.read_sql("SELECT university_id, name_th FROM dim_university", conn)
    conn.close()
    return dict(zip(df["name_th"], df["university_id"]))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
    log.info("Database initialized at: %s", DB_PATH)
