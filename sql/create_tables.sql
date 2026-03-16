CREATE TABLE IF NOT EXISTS dim_university (
    university_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name_th         TEXT NOT NULL,
    name_en         TEXT,
    short_name      TEXT,
    province        TEXT,
    university_type TEXT  -- public, private, rajabhat, rajamangala
);

CREATE TABLE IF NOT EXISTS dim_faculty (
    faculty_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    university_id   INTEGER NOT NULL,
    faculty_name_th TEXT NOT NULL,
    faculty_name_en TEXT,
    FOREIGN KEY (university_id) REFERENCES dim_university(university_id)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INTEGER PRIMARY KEY,  -- format YYYYMMDD
    full_date   TEXT NOT NULL,
    year        INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    quarter     INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL
);

-- Fact Table 1: TCAS Admission Data
CREATE TABLE IF NOT EXISTS fact_tcas_admission (
    tcas_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    university_id       INTEGER NOT NULL,
    faculty_id          INTEGER,
    tcas_round_name     TEXT NOT NULL,        -- e.g. "TCAS67"
    branch_name         TEXT,
    seats_available     INTEGER,
    applicants          INTEGER,
    score_max           REAL,
    score_min           REAL,
    score_mean          REAL,
    score_sd            REAL,
    FOREIGN KEY (university_id) REFERENCES dim_university(university_id),
    FOREIGN KEY (faculty_id)    REFERENCES dim_faculty(faculty_id)
);

-- Fact Table 2: MHESI Enrollment Data
CREATE TABLE IF NOT EXISTS fact_mhesi_enrollment (
    mhesi_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    university_id       INTEGER NOT NULL,
    academic_year       INTEGER,
    total_students      INTEGER,
    raw_data            TEXT,   -- JSON blob of original row for audit
    FOREIGN KEY (university_id) REFERENCES dim_university(university_id)
);

-- Fact Table 3: YouTube Engagement Data
CREATE TABLE IF NOT EXISTS fact_youtube_engagement (
    youtube_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    university_id       INTEGER NOT NULL,
    video_id            TEXT UNIQUE NOT NULL,
    title               TEXT,
    channel_title       TEXT,
    published_at        TEXT,
    publish_year        INTEGER,
    publish_month       INTEGER,
    view_count          INTEGER NOT NULL DEFAULT 0,
    like_count          INTEGER NOT NULL DEFAULT 0,
    comment_count       INTEGER NOT NULL DEFAULT 0,
    search_query        TEXT,
    FOREIGN KEY (university_id) REFERENCES dim_university(university_id)
);

-- Fact Table 4: Google Trends Interest Data
CREATE TABLE IF NOT EXISTS fact_google_trends (
    trend_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    university_id   INTEGER NOT NULL,
    date            TEXT NOT NULL,          -- YYYY-MM-DD
    interest_score  INTEGER,               -- 0-100 relative interest
    keyword         TEXT,
    FOREIGN KEY (university_id) REFERENCES dim_university(university_id)
);

-- Fact Table 5: Wikipedia Pageview Data (daily granularity)
CREATE TABLE IF NOT EXISTS fact_wikipedia_pageviews (
    wiki_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    university_id   INTEGER NOT NULL,
    article_title   TEXT NOT NULL,
    date            TEXT,                   -- YYYY-MM-DD
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    day             INTEGER,
    pageviews       INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (university_id) REFERENCES dim_university(university_id)
);
