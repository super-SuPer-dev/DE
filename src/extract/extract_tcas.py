"""
Extract TCAS max/min score Excel files from mytcas.com.
Saves raw data to data/raw/tcas_raw.csv
"""
import os
import logging
import requests
import pandas as pd
from src.config import RAW_DIR

log = logging.getLogger(__name__)

STAGING_FILE = os.path.join(RAW_DIR, "tcas_raw.csv")
DOWNLOADS_DIR = os.path.join(RAW_DIR, "downloads_tcas")

EXCEL_SOURCES = [
    {"name": "TCAS68 (รอบ 3 ครั้งที่ 2)", "url": "https://assets.mytcas.com/68/T68-stat-r3_2-maxmin-24May25.xlsx"},
    {"name": "TCAS68 (รอบ 3 ครั้งที่ 1)", "url": "https://assets.mytcas.com/68/T68-stat-r3_1-maxmin-20May25.xlsx"},
    {"name": "TCAS67", "url": "https://assets.mytcas.com/67/TCAS67_maxmin.xlsx"},
    {"name": "TCAS66", "url": "https://assets.mytcas.com/maxmin/TCAS66_maxmin.xlsx"},
    {"name": "TCAS65", "url": "https://assets.mytcas.com/maxmin/TCAS65_maxmin.xlsx"},
    {"name": "TCAS64", "url": "https://assets.mytcas.com/maxmin/TCAS64_maxmin.xlsx"},
    {"name": "TCAS63", "url": "https://assets.mytcas.com/maxmin/TCAS63_maxmin.xlsx"},
    {"name": "TCAS62", "url": "https://assets.mytcas.com/maxmin/TCAS62_maxmin.xlsx"},
]


def extract_tcas() -> pd.DataFrame:
    if os.path.exists(STAGING_FILE):
        log.info("Loading from staging: %s", STAGING_FILE)
        return pd.read_csv(STAGING_FILE)

    log.info("Downloading Excel files from mytcas.com...")
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)
    all_data = []

    for source in EXCEL_SOURCES:
        file_name = source["url"].split("/")[-1]
        file_path = os.path.join(DOWNLOADS_DIR, file_name)

        if not os.path.exists(file_path):
            log.info("  Downloading %s...", source["name"])
            resp = requests.get(source["url"], timeout=30)
            if resp.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(resp.content)
            else:
                log.warning("  FAILED: %s (status %d)", source["name"], resp.status_code)
                continue

        log.info("  Processing %s...", source["name"])
        try:
            df_year = pd.read_excel(file_path)
            df_year["tcas_round_name"] = source["name"]
            all_data.append(df_year)
        except Exception as e:
            log.error("  Error processing %s: %s", source["name"], e)

    df = pd.concat(all_data, ignore_index=True)
    assert len(df) >= 5000, f"Too few TCAS records: {len(df)}"

    os.makedirs(os.path.dirname(STAGING_FILE), exist_ok=True)
    df.to_csv(STAGING_FILE, index=False)
    log.info("Saved %d records to %s", len(df), STAGING_FILE)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = extract_tcas()
    print(df.head())
    print(f"Total: {len(df)} records, {len(df.columns)} columns")
