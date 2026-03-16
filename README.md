# University Popularity - Data Engineering Project

## Project Overview
A Data Engineering project analyzing Thai university popularity across three dimensions:
- **TCAS** admission competition rates (scraped from mytcas.com)
- **MHESI** student enrollment data (via government REST API)
- **YouTube** social media engagement (via YouTube Data API v3)

## Project Structure
```
university-popularity-de/
├── data/
│   ├── bronze/          # Raw extracted files (not committed to git)
│   ├── silver/          # Cleaned CSVs
│   └── gold/            # university.db (SQLite)
├── src/
│   ├── extract/         # extract_tcas.py, extract_mhesi.py, extract_youtube.py
│   ├── transform/       # clean_youtube.py
│   └── load/            # init_db.py, load_youtube.py
├── sql/
│   ├── create_tables.sql
│   └── analysis.sql
├── pipeline.py          # Main pipeline orchestrator
└── pyproject.toml
```

## Prerequisites
1. **Python** (via `uv`)
2. **YouTube Data API Key** - Get one free at [Google Cloud Console](https://console.cloud.google.com/)
   - Enable "YouTube Data API v3"
   - Create credentials -> API key
   - Add to `.env` file: `YOUTUBE_API_KEY=your_key_here`

## How to Run
```bash
# 1. Install dependencies
uv sync

# 2. Set up YouTube API Key
echo "YOUTUBE_API_KEY=your_key_here" > .env

# 3. Run the full pipeline
uv run pipeline.py
```

## Data Architecture (Medallion)
| Layer  | Location          | Description                  |
|--------|-------------------|------------------------------|
| Bronze | data/bronze/      | Raw JSON/CSV from sources    |
| Silver | data/silver/      | Cleaned & standardized CSV   |
| Gold   | data/gold/*.db    | SQLite Star Schema           |

## Key Insights (from sql/analysis.sql)
- Which university gets the most YouTube views?
- Does high YouTube engagement correlate with TCAS competition?
- Which universities have high hype but low admission competition?
