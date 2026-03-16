# Potential Improvements

## Priority 1 — Data Quality & Coverage

### 1. Google Trends Data Collection
- **Current state:** Extractor is fully coded and resumable but blocked by IP rate limiting (0 records)
- **Fix:** Run `extract_google_trends()` during off-peak hours with VPN rotation, or use a proxy service
- **Impact:** Adds a 5th dimension to cross-source analysis

### 2. More Universities in Wikipedia
- **Current state:** Only 28 of 57 universities have Wikipedia article mappings
- **Fix:** Add Wikipedia article titles for the remaining 29 universities in `config.py → WIKIPEDIA_ARTICLES`
- **Impact:** More complete cross-source comparison

### 3. Historical Depth
- **Current state:** Wikipedia covers Jan 2024 – Mar 2026; YouTube depends on channel age
- **Fix:** Extend Wikipedia extraction to 2020+ for longer trend analysis
- **Impact:** Enables year-over-year growth comparisons

---

## Priority 2 — Engineering Best Practices

### 4. Add Unit Tests
- **Current state:** No test suite
- **Fix:** Add `tests/` directory with pytest for each module:
  - Test `_short()` name shortening function
  - Test `get_university_map()` returns valid mapping
  - Test staging table pattern with an in-memory SQLite DB
  - Test config paths exist
- **Impact:** Prevents regressions during refactoring

### 5. Add Data Validation (Great Expectations or Pandera)
- **Current state:** No formal data quality checks between layers
- **Fix:** Add schema validation at each layer boundary:
  - Bronze: check required columns exist, no empty files
  - Silver: check no nulls in key fields, value ranges
  - Gold: check referential integrity, row count thresholds
- **Impact:** Catches data quality issues early

### 6. CI/CD Pipeline
- **Current state:** Manual execution only
- **Fix:** GitHub Actions workflow that:
  - Runs `uv sync` + linting (ruff)
  - Runs unit tests
  - Optionally runs the pipeline on a schedule (weekly)
- **Impact:** Automated quality assurance

### 7. Environment-Based Configuration
- **Current state:** Single `.env` file with one API key
- **Fix:** Support `DEV` / `PROD` environments with different DB paths, logging levels, and rate limits
- **Impact:** Cleaner separation for development vs production

---

## Priority 3 — Additional Data Sources

### 8. Facebook Pages API
- **Requires:** Facebook Developer account + page access token
- **Data:** Follower counts, post engagement, page likes
- **Challenge:** Strict API access policies since 2018

### 9. X (Twitter) API
- **Requires:** Twitter Developer account (paid since 2023)
- **Data:** Mention counts, sentiment, follower growth
- **Challenge:** Free tier is very limited; basic tier costs $100/month

### 10. LINE Official Account (Thailand-specific)
- **Requires:** LINE Developers API access
- **Data:** Follower counts for university LINE accounts
- **Challenge:** Limited public API; may need web scraping

### 11. QS / THE World University Rankings
- **Method:** Web scraping or public datasets
- **Data:** International ranking scores and category breakdowns
- **Impact:** Adds an "academic quality" dimension to popularity analysis

---

## Priority 4 — Analysis & Visualization

### 12. Interactive Dashboard
- **Current state:** Static PNG plots
- **Fix:** Build a Streamlit or Plotly Dash web app that:
  - Allows filtering by university type, year, region
  - Shows real-time cross-source comparisons
  - Includes a searchable university profile page
- **Impact:** Much more usable for stakeholders

### 13. Statistical Analysis
- **Current state:** Descriptive charts only
- **Fix:** Add:
  - Correlation matrix between all data sources
  - Time series forecasting (ARIMA/Prophet) for Wikipedia trends
  - Clustering analysis to group universities by popularity profile
  - Regression model: can YouTube views predict TCAS competition?
- **Impact:** Moves from "what happened" to "why" and "what will happen"

### 14. Sentiment Analysis on YouTube Comments
- **Requires:** YouTube comment extraction + Thai NLP library (PyThaiNLP)
- **Data:** Positive/negative sentiment scores per university
- **Impact:** Adds qualitative dimension to popularity metrics

---

## Priority 5 — Infrastructure

### 15. Replace SQLite with PostgreSQL
- **Current state:** SQLite is great for single-user but limited for concurrent access
- **Fix:** Docker Compose with PostgreSQL + optional Apache Superset for BI
- **Impact:** Production-ready data warehouse

### 16. Apache Airflow Orchestration
- **Current state:** Single `pipeline.py` script with sequential execution
- **Fix:** Convert each step to an Airflow DAG task with:
  - Retry policies per task
  - Email alerts on failure
  - Scheduling (daily/weekly)
  - Backfill capability
- **Impact:** Professional-grade pipeline orchestration

### 17. Data Lineage & Documentation
- **Fix:** Add dbt (data build tool) for:
  - SQL-based transformations with documentation
  - Automatic data lineage graphs
  - Built-in testing framework
- **Impact:** Enterprise-grade data transformation layer
