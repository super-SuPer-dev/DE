-- Analysis queries for the Gold layer
-- Run these in your BI tool or directly in Python via sqlite3

-- ═══════════════════════════════════════════════════════════════
-- 1. YouTube: Total engagement by university
-- ═══════════════════════════════════════════════════════════════
SELECT
    u.name_th,
    COUNT(y.youtube_id)         AS total_videos,
    SUM(y.view_count)           AS total_views,
    SUM(y.like_count)           AS total_likes,
    SUM(y.comment_count)        AS total_comments,
    AVG(y.view_count)           AS avg_views_per_video
FROM fact_youtube_engagement y
JOIN dim_university u ON y.university_id = u.university_id
GROUP BY u.university_id, u.name_th
ORDER BY total_views DESC;


-- ═══════════════════════════════════════════════════════════════
-- 2. TCAS: Admission statistics by university and round
-- ═══════════════════════════════════════════════════════════════
SELECT
    u.name_th,
    t.tcas_round_name,
    COUNT(*)                    AS total_programs,
    SUM(t.applicants)           AS total_applicants,
    SUM(t.seats_available)      AS total_seats,
    CAST(SUM(t.applicants) AS REAL) / NULLIF(SUM(t.seats_available), 0) AS competition_ratio
FROM fact_tcas_admission t
JOIN dim_university u ON t.university_id = u.university_id
GROUP BY u.university_id, u.name_th, t.tcas_round_name
ORDER BY total_applicants DESC;


-- ═══════════════════════════════════════════════════════════════
-- 3. Cross-source: YouTube popularity vs TCAS competition
-- ═══════════════════════════════════════════════════════════════
SELECT
    u.name_th,
    yt_agg.total_views,
    yt_agg.total_videos,
    tcas_agg.total_applicants,
    tcas_agg.total_seats,
    CAST(tcas_agg.total_applicants AS REAL) / NULLIF(tcas_agg.total_seats, 0) AS competition_ratio
FROM dim_university u
JOIN (
    SELECT university_id, SUM(view_count) AS total_views, COUNT(*) AS total_videos
    FROM fact_youtube_engagement GROUP BY university_id
) yt_agg ON yt_agg.university_id = u.university_id
JOIN (
    SELECT university_id, SUM(applicants) AS total_applicants, SUM(seats_available) AS total_seats
    FROM fact_tcas_admission GROUP BY university_id
) tcas_agg ON tcas_agg.university_id = u.university_id
ORDER BY yt_agg.total_views DESC;


-- ═══════════════════════════════════════════════════════════════
-- 4. Google Trends: Average search interest by university
-- ═══════════════════════════════════════════════════════════════
SELECT
    u.name_th,
    AVG(g.interest_score)       AS avg_interest,
    MAX(g.interest_score)       AS peak_interest,
    COUNT(*)                    AS data_points
FROM fact_google_trends g
JOIN dim_university u ON g.university_id = u.university_id
GROUP BY u.university_id, u.name_th
ORDER BY avg_interest DESC;


-- ═══════════════════════════════════════════════════════════════
-- 5. Wikipedia: Total pageviews by university
-- ═══════════════════════════════════════════════════════════════
SELECT
    u.name_th,
    w.article_title,
    SUM(w.pageviews)            AS total_pageviews,
    AVG(w.pageviews)            AS avg_monthly_pageviews
FROM fact_wikipedia_pageviews w
JOIN dim_university u ON w.university_id = u.university_id
GROUP BY u.university_id, u.name_th, w.article_title
ORDER BY total_pageviews DESC;


-- ═══════════════════════════════════════════════════════════════
-- 6. Full cross-source comparison (all 5 sources)
-- ═══════════════════════════════════════════════════════════════
SELECT
    u.name_th,
    COALESCE(yt.total_views, 0)         AS youtube_views,
    COALESCE(tcas.total_applicants, 0)  AS tcas_applicants,
    COALESCE(mhesi.max_students, 0)     AS mhesi_students,
    COALESCE(gt.avg_interest, 0)        AS google_trends_score,
    COALESCE(wiki.total_pageviews, 0)   AS wikipedia_pageviews
FROM dim_university u
LEFT JOIN (
    SELECT university_id, SUM(view_count) AS total_views
    FROM fact_youtube_engagement GROUP BY university_id
) yt ON yt.university_id = u.university_id
LEFT JOIN (
    SELECT university_id, SUM(applicants) AS total_applicants
    FROM fact_tcas_admission GROUP BY university_id
) tcas ON tcas.university_id = u.university_id
LEFT JOIN (
    SELECT university_id, MAX(total_students) AS max_students
    FROM fact_mhesi_enrollment GROUP BY university_id
) mhesi ON mhesi.university_id = u.university_id
LEFT JOIN (
    SELECT university_id, AVG(interest_score) AS avg_interest
    FROM fact_google_trends GROUP BY university_id
) gt ON gt.university_id = u.university_id
LEFT JOIN (
    SELECT university_id, SUM(pageviews) AS total_pageviews
    FROM fact_wikipedia_pageviews GROUP BY university_id
) wiki ON wiki.university_id = u.university_id
ORDER BY youtube_views DESC;
