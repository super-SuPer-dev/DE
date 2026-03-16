# Thai University Popularity — Data Engineering Project

## What Is This Project?

A **data engineering pipeline** + **interactive demo** that collects, cleans, and analyzes
the popularity of **57 Thai universities** using data from **5 different sources**:

| Source | Type | Records | Description |
|--------|------|---------|-------------|
| **TCAS** | Web-scraped Excel | 29,747 | Admission applicants, seats, scores, and competition rates from mytcas.com |
| **MHESI** | REST API | 14,793 | Official student enrollment numbers from the Ministry of Higher Education |
| **YouTube** | YouTube Data API v3 | 6,208 | Video views, likes, and comments for university channels |
| **Wikipedia** | Wikimedia REST API | 44,967 | Daily pageview counts for each university's Wikipedia article |
| **Google Trends** | pytrends | 0* | Search interest scores for university names |

> *Google Trends is rate-limited by IP; the extractor is fully coded with resume capability.

**Total: ~95,715 records** in an SQLite star-schema database.

---

## Architecture

```
                ┌──────────────────────────────────────────┐
                │           DATA SOURCES                   │
                │  TCAS  MHESI  YouTube  Wikipedia  GoogTr │
                └────────────┬─────────────────────────────┘
                             │  Extract
                ┌────────────▼─────────────────────────────┐
                │         RAW DATA  (data/raw/)            │
                │   CSV files exactly as received           │
                └────────────┬─────────────────────────────┘
                             │  Transform
                ┌────────────▼─────────────────────────────┐
                │        CLEAN DATA  (data/clean/)         │
                │   Deduplicated, standardized              │
                └────────────┬─────────────────────────────┘
                             │  Load
                ┌────────────▼─────────────────────────────┐
                │        DATABASE (data/university.db)     │
                │   SQLite Star Schema                      │
                │                                           │
                │   dim_university ◄──┐                     │
                │   dim_faculty   ◄──┤                     │
                │   dim_date      ◄──┤                     │
                │                    ├── fact_tcas          │
                │                    ├── fact_mhesi         │
                │                    ├── fact_youtube       │
                │                    ├── fact_wikipedia     │
                │                    └── fact_google_trends │
                └──────────────────────────────────────────┘
                             │
          ┌──────────────────┴──────────────────┐
          ▼                                     ▼
┌──────────────────────┐          ┌──────────────────────┐
│  ANALYSIS PLOTS      │          │  STREAMLIT DEMO      │
│  (output_plots/)     │          │  (src/demo/app.py)   │
│  13 visualizations   │          │  Admission checker   │
└──────────────────────┘          └──────────────────────┘
```

---

## Project Structure

```
university-popularity-de/
├── pipeline.py                  # Main orchestrator — runs all 10 steps
├── pyproject.toml               # Python dependencies (managed by uv)
├── .env                         # API keys (YOUTUBE_API_KEY)
│
├── src/
│   ├── config.py                # Centralized paths, university list, settings
│   ├── extract/                 # Data extraction from external sources
│   │   ├── extract_tcas.py      #   TCAS Excel files from mytcas.com
│   │   ├── extract_mhesi.py     #   MHESI government REST API
│   │   ├── extract_youtube.py   #   YouTube Data API v3 (resumable)
│   │   ├── extract_wikipedia.py #   Wikimedia pageviews REST API
│   │   └── extract_google_trends.py  # pytrends (resumable)
│   ├── transform/               # Data cleaning and standardization
│   │   ├── clean_youtube.py
│   │   └── clean_google_trends.py
│   ├── load/                    # Load into SQLite database
│   │   ├── init_db.py           #   Create tables from sql/create_tables.sql
│   │   ├── load_dimensions.py   #   Populate dim_university
│   │   ├── load_tcas.py         #   TCAS data with scores
│   │   ├── load_mhesi.py
│   │   ├── load_youtube.py
│   │   ├── load_wikipedia.py
│   │   └── load_google_trends.py
│   ├── analysis/
│   │   └── generate_plots.py    # 13 comprehensive visualizations
│   └── demo/                    # Streamlit interactive demo
│       ├── app.py               #   Main Streamlit app
│       └── queries.py           #   Database query functions
│
├── sql/
│   ├── create_tables.sql        # DDL for star schema
│   └── analysis.sql             # Sample analytical queries
│
├── data/
│   ├── raw/                     # Raw extracted data (CSV, Excel)
│   ├── clean/                   # Cleaned data
│   └── university.db            # SQLite star-schema database
│
└── output_plots/                # 13 PNG analysis charts
```

---

## How to Use

### Prerequisites

- **Python 3.12+** (via [uv](https://docs.astral.sh/uv/))
- **YouTube Data API Key** — free from [Google Cloud Console](https://console.cloud.google.com/)

### Quick Start

```bash
# 1. Install dependencies
uv sync

# 2. Create .env file with your YouTube API key
echo "YOUTUBE_API_KEY=your_key_here" > .env

# 3. Run the full pipeline (extract -> transform -> load -> analyze)
uv run pipeline.py
```

The pipeline will:
1. Initialize the SQLite database and dimension tables
2. Extract data from all 5 sources into `data/raw/`
3. Clean and transform data into `data/clean/`
4. Load everything into the star schema at `data/university.db`
5. Generate 13 analysis plots in `output_plots/`

### Run the Interactive Demo

```bash
uv run streamlit run src/demo/app.py
```

This opens a web app where you can:
- Enter your exam score
- Select 1-3 universities you want to attend
- See your **admission chance** (gauge chart), **competition ratio**, and **popularity ranking**
- Get a **recommendation** comparing your choices

### Run Only Analysis (if database already exists)

```bash
uv run python -c "
import sys; sys.path.insert(0,'.')
from src.analysis.generate_plots import generate_all_plots
generate_all_plots()
"
```

### Query the Database Directly

```bash
uv run python -c "
import sqlite3, pandas as pd
conn = sqlite3.connect('data/university.db')
print(pd.read_sql('SELECT * FROM dim_university LIMIT 10', conn))
conn.close()
"
```

Or use the queries in `sql/analysis.sql` with any SQLite client.

---

## Interactive Demo Features

The Streamlit demo (`src/demo/app.py`) provides:

| Feature | Description |
|---------|-------------|
| **Admission Chance** | Gauge chart comparing your score against historical min/max scores |
| **Competition Ratio** | How many applicants per seat (e.g., 7.6:1) |
| **Popularity Ranking** | Composite score from Wikipedia + MHESI + TCAS data (#1-57) |
| **Competition Trends** | Bar chart of applicants vs seats across TCAS rounds (TCAS62-68) |
| **Side-by-Side Comparison** | Table comparing all your chosen universities |
| **Recommendation** | Sorted advice from best to worst chance |

---

## Analysis Plots Generated

| # | Plot | Description |
|---|------|-------------|
| 01 | YouTube Views | Top 15 universities by total video views |
| 02 | TCAS Applicants | Top 15 universities by admission applicants |
| 03 | MHESI Enrollment | Top 15 universities by student enrollment |
| 04 | Wikipedia Pageviews | Top 15 universities by Wikipedia page visits |
| 05 | YouTube vs TCAS | Scatter plot — online interest vs admission demand |
| 06 | TCAS Competition Ratio | Top 15 most competitive universities (applicants/seats) |
| 07 | TCAS Trends | Applicants vs seats across different TCAS rounds |
| 08 | YouTube Growth | Video count and view growth by year (2015-2025) |
| 09 | YouTube Engagement | Top 15 universities by like-to-view ratio |
| 10 | Wikipedia Trends | Monthly pageview trends for top 5 universities |
| 11 | Wikipedia vs MHESI | Scatter — Wikipedia interest vs university size |
| 12 | Cross-Source Heatmap | University rankings across all 4 data sources |
| 13 | University Types | Comparison of public/private/Rajabhat/Rajamangala types |

---

## Key Design Decisions

- **Star Schema** — Dimension tables (`dim_university`, `dim_faculty`, `dim_date`) + fact tables for efficient analytical queries
- **Resumable Extraction** — YouTube and Google Trends use JSON progress files to resume after API failures
- **Staging Table Pattern** — All loaders use temporary staging tables with `INSERT OR IGNORE` to prevent duplicates on re-runs
- **Centralized Config** — Single `src/config.py` manages all paths, university lists, and API mappings
- **Fault-Tolerant Pipeline** — Each step wrapped in try/except so one failure doesn't stop the entire pipeline
- **Score Coalescing** — TCAS score columns vary across years; the loader handles all Thai column name variants automatically
