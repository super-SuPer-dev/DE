"""
Main pipeline orchestrator.
Runs all ETL steps in order: Extract -> Transform -> Load

Usage:
    uv run pipeline.py
"""
import sys
import os
import time
import logging

# Ensure project root is on path for src.* imports
sys.path.insert(0, os.path.dirname(__file__))

from src.config import setup_logging
from src.extract.extract_mhesi import extract_mhesi
from src.extract.extract_tcas import extract_tcas
from src.extract.extract_youtube import extract_youtube
from src.extract.extract_google_trends import extract_google_trends
from src.extract.extract_wikipedia import extract_wikipedia
from src.transform.clean_youtube import clean_youtube
from src.transform.clean_google_trends import clean_google_trends
from src.load.init_db import init_db
from src.load.load_youtube import load_youtube
from src.load.load_dimensions import load_dimensions
from src.load.load_tcas import load_tcas
from src.load.load_mhesi import load_mhesi
from src.load.load_google_trends import load_google_trends
from src.load.load_wikipedia import load_wikipedia

log = logging.getLogger(__name__)


def run_pipeline():
    setup_logging()

    print("=" * 60)
    print("  University Popularity - Data Engineering Pipeline")
    print("  Sources: TCAS | MHESI | YouTube | Google Trends | Wikipedia")
    print("=" * 60)
    start = time.time()

    # ── STEP 1: Initialize Gold layer ────────────────────────────
    log.info("[Step 1/10] Initializing SQLite database...")
    init_db()
    load_dimensions()

    # ── STEP 2: Extract TCAS (Bronze) ────────────────────────────
    try:
        log.info("[Step 2/10] Extracting TCAS data...")
        df_tcas = extract_tcas()
        log.info("  -> %s TCAS records in Bronze", f"{len(df_tcas):,}")
    except Exception as e:
        log.error("TCAS extraction failed: %s", e)

    # ── STEP 3: Extract MHESI (Bronze) ───────────────────────────
    try:
        log.info("[Step 3/10] Extracting MHESI data...")
        df_mhesi = extract_mhesi()
        log.info("  -> %s MHESI records in Bronze", f"{len(df_mhesi):,}")
    except Exception as e:
        log.error("MHESI extraction failed: %s", e)

    # ── STEP 4: Extract YouTube (Bronze) ─────────────────────────
    try:
        log.info("[Step 4/10] Extracting YouTube data...")
        df_youtube = extract_youtube()
        log.info("  -> %s YouTube videos in Bronze", f"{len(df_youtube):,}")
    except Exception as e:
        log.error("YouTube extraction failed: %s", e)

    # ── STEP 5: Extract Google Trends (Bronze) ───────────────────
    try:
        log.info("[Step 5/10] Extracting Google Trends data...")
        df_trends = extract_google_trends()
        log.info("  -> %s Google Trends records in Bronze", f"{len(df_trends):,}")
    except Exception as e:
        log.error("Google Trends extraction failed: %s", e)

    # ── STEP 6: Extract Wikipedia Pageviews (Bronze) ─────────────
    try:
        log.info("[Step 6/10] Extracting Wikipedia pageview data...")
        df_wiki = extract_wikipedia()
        log.info("  -> %s Wikipedia pageview records in Bronze", f"{len(df_wiki):,}")
    except Exception as e:
        log.error("Wikipedia extraction failed: %s", e)

    # ── STEP 7: Transform YouTube (Bronze -> Silver) ─────────────
    try:
        log.info("[Step 7/10] Transforming YouTube data...")
        clean_youtube()
    except Exception as e:
        log.error("YouTube transform failed: %s", e)

    # ── STEP 8: Transform Google Trends (Bronze -> Silver) ───────
    try:
        log.info("[Step 8/10] Transforming Google Trends data...")
        clean_google_trends()
    except Exception as e:
        log.error("Google Trends transform failed: %s", e)

    # ── STEP 9: Load all data into Gold layer ────────────────────
    log.info("[Step 9/10] Loading into SQLite Gold layer...")
    for loader_name, loader_fn in [
        ("YouTube", load_youtube),
        ("TCAS", load_tcas),
        ("MHESI", load_mhesi),
        ("Google Trends", load_google_trends),
        ("Wikipedia", load_wikipedia),
    ]:
        try:
            loader_fn()
        except Exception as e:
            log.error("%s load failed: %s", loader_name, e)

    # ── STEP 10: Generate plots ──────────────────────────────────
    try:
        log.info("[Step 10/10] Generating analytical plots...")
        from src.analysis.generate_plots import generate_all_plots
        generate_all_plots()
    except Exception as e:
        log.error("Plot generation failed: %s", e)

    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"  Pipeline complete! Total time: {elapsed:.1f}s")
    print(f"  Gold DB: data/gold/university.db")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    run_pipeline()
