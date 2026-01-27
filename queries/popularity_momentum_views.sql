-- BigQuery Views and Queries for Popularity Momentum Analysis

-- ============================================================================
-- View 1: Latest Movie Popularity Metrics
-- Shows current popularity with day-over-day and week-over-week changes
-- ============================================================================
CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_latest_popularity_metrics` AS
WITH latest_snapshots AS (
  SELECT
    s.movie_id,
    m.title,
    m.release_date,
    s.snapshot_date,
    s.popularity,
    s.popularity_change_1d,
    s.popularity_change_7d,
    s.popularity_pct_change_1d,
    s.popularity_pct_change_7d,
    ROW_NUMBER() OVER (PARTITION BY s.movie_id ORDER BY s.snapshot_date DESC) AS rn
  FROM `{project_id}.{dataset}.movie_daily_snapshots` s
  JOIN `{project_id}.{dataset}.movies` m ON s.movie_id = m.movie_id
)
SELECT
  movie_id,
  title,
  release_date,
  snapshot_date AS last_updated,
  popularity AS current_popularity,
  popularity_change_1d AS daily_change,
  popularity_pct_change_1d AS daily_pct_change,
  popularity_change_7d AS weekly_change,
  popularity_pct_change_7d AS weekly_pct_change,
  CASE
    WHEN popularity_pct_change_1d > 10 THEN 'Surging'
    WHEN popularity_pct_change_1d > 5 THEN 'Rising'
    WHEN popularity_pct_change_1d > -5 THEN 'Stable'
    WHEN popularity_pct_change_1d > -10 THEN 'Declining'
    ELSE 'Falling'
  END AS momentum_status
FROM latest_snapshots
WHERE rn = 1;


-- ============================================================================
-- View 2: 7-Day Moving Average Popularity
-- Tracks popularity momentum with smoothed trends
-- ============================================================================
CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_popularity_7d_moving_average` AS
SELECT
  s.movie_id,
  m.title,
  s.snapshot_date,
  s.popularity,
  AVG(s.popularity) OVER (
    PARTITION BY s.movie_id
    ORDER BY s.snapshot_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS popularity_7d_ma,
  s.popularity - AVG(s.popularity) OVER (
    PARTITION BY s.movie_id
    ORDER BY s.snapshot_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS deviation_from_ma,
  STDDEV(s.popularity) OVER (
    PARTITION BY s.movie_id
    ORDER BY s.snapshot_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS popularity_volatility_7d
FROM `{project_id}.{dataset}.movie_daily_snapshots` s
JOIN `{project_id}.{dataset}.movies` m ON s.movie_id = m.movie_id
ORDER BY s.movie_id, s.snapshot_date;


-- ============================================================================
-- View 3: Week-over-Week Popularity Comparison
-- Compares current week vs previous week popularity
-- ============================================================================
CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_popularity_wow_comparison` AS
WITH weekly_aggregates AS (
  SELECT
    movie_id,
    DATE_TRUNC(snapshot_date, WEEK) AS week_start,
    AVG(popularity) AS avg_popularity,
    MAX(popularity) AS max_popularity,
    MIN(popularity) AS min_popularity
  FROM `{project_id}.{dataset}.movie_daily_snapshots`
  GROUP BY movie_id, DATE_TRUNC(snapshot_date, WEEK)
)
SELECT
  w.movie_id,
  m.title,
  w.week_start AS current_week,
  w.avg_popularity AS current_week_avg_popularity,
  LAG(w.avg_popularity, 1) OVER (PARTITION BY w.movie_id ORDER BY w.week_start) AS prev_week_avg_popularity,
  w.avg_popularity - LAG(w.avg_popularity, 1) OVER (PARTITION BY w.movie_id ORDER BY w.week_start) AS wow_change,
  SAFE_DIVIDE(
    w.avg_popularity - LAG(w.avg_popularity, 1) OVER (PARTITION BY w.movie_id ORDER BY w.week_start),
    LAG(w.avg_popularity, 1) OVER (PARTITION BY w.movie_id ORDER BY w.week_start)
  ) * 100 AS wow_pct_change,
  w.max_popularity AS peak_popularity,
  w.min_popularity AS low_popularity
FROM weekly_aggregates w
JOIN `{project_id}.{dataset}.movies` m ON w.movie_id = m.movie_id
ORDER BY w.movie_id, w.week_start;


-- ============================================================================
-- View 4: Top Gaining Movies (Popularity Momentum)
-- Identifies movies with highest popularity gains
-- ============================================================================
CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_top_gaining_movies` AS
WITH latest_metrics AS (
  SELECT
    s.movie_id,
    m.title,
    m.release_date,
    s.snapshot_date,
    s.popularity,
    s.popularity_pct_change_7d,
    ma.popularity_7d_ma,
    ma.popularity_wow_pct_change,
    ROW_NUMBER() OVER (PARTITION BY s.movie_id ORDER BY s.snapshot_date DESC) AS rn
  FROM `{project_id}.{dataset}.movie_daily_snapshots` s
  JOIN `{project_id}.{dataset}.movies` m ON s.movie_id = m.movie_id
  LEFT JOIN `{project_id}.{dataset}.movie_metrics_aggregated` ma
    ON s.movie_id = ma.movie_id AND s.snapshot_date = ma.calculation_date
)
SELECT
  movie_id,
  title,
  release_date,
  snapshot_date AS last_updated,
  popularity AS current_popularity,
  popularity_7d_ma,
  popularity_pct_change_7d AS weekly_pct_gain,
  popularity_wow_pct_change AS wow_pct_change,
  RANK() OVER (ORDER BY popularity_pct_change_7d DESC) AS momentum_rank
FROM latest_metrics
WHERE rn = 1
  AND popularity_pct_change_7d IS NOT NULL
  AND popularity_pct_change_7d > 0
ORDER BY popularity_pct_change_7d DESC
LIMIT 100;


-- ============================================================================
-- View 5: Top Declining Movies (Losing Traction)
-- Identifies movies losing popularity fastest
-- ============================================================================
CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_top_declining_movies` AS
WITH latest_metrics AS (
  SELECT
    s.movie_id,
    m.title,
    m.release_date,
    s.snapshot_date,
    s.popularity,
    s.popularity_pct_change_7d,
    ma.popularity_7d_ma,
    ma.popularity_wow_pct_change,
    ROW_NUMBER() OVER (PARTITION BY s.movie_id ORDER BY s.snapshot_date DESC) AS rn
  FROM `{project_id}.{dataset}.movie_daily_snapshots` s
  JOIN `{project_id}.{dataset}.movies` m ON s.movie_id = m.movie_id
  LEFT JOIN `{project_id}.{dataset}.movie_metrics_aggregated` ma
    ON s.movie_id = ma.movie_id AND s.snapshot_date = ma.calculation_date
)
SELECT
  movie_id,
  title,
  release_date,
  snapshot_date AS last_updated,
  popularity AS current_popularity,
  popularity_7d_ma,
  popularity_pct_change_7d AS weekly_pct_decline,
  popularity_wow_pct_change AS wow_pct_change,
  RANK() OVER (ORDER BY popularity_pct_change_7d ASC) AS decline_rank
FROM latest_metrics
WHERE rn = 1
  AND popularity_pct_change_7d IS NOT NULL
  AND popularity_pct_change_7d < 0
ORDER BY popularity_pct_change_7d ASC
LIMIT 100;


-- ============================================================================
-- Query 1: Daily Popularity Momentum Report
-- Use this query to generate daily reports on popularity trends
-- ============================================================================
-- Query: Top movies by popularity momentum (last 7 days)
SELECT
  m.title,
  m.release_date,
  DATE_DIFF(CURRENT_DATE(), m.release_date, DAY) AS days_since_release,
  lpm.current_popularity,
  lpm.daily_change,
  lpm.daily_pct_change,
  lpm.weekly_change,
  lpm.weekly_pct_change,
  lpm.momentum_status,
  STRING_AGG(g.name, ', ' ORDER BY g.name) AS genres
FROM `{project_id}.{dataset}.v_latest_popularity_metrics` lpm
JOIN `{project_id}.{dataset}.movies` m ON lpm.movie_id = m.movie_id
LEFT JOIN UNNEST(m.genres) g
WHERE lpm.weekly_pct_change > 5  -- Only movies gaining traction
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY lpm.weekly_pct_change DESC
LIMIT 50;


-- ============================================================================
-- Query 2: Popularity Trend Analysis by Release Period
-- Analyzes how movies trend based on time since release
-- ============================================================================
WITH release_cohorts AS (
  SELECT
    m.movie_id,
    m.title,
    m.release_date,
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), m.release_date, DAY) <= 30 THEN 'New (0-30 days)'
      WHEN DATE_DIFF(CURRENT_DATE(), m.release_date, DAY) <= 90 THEN 'Recent (31-90 days)'
      WHEN DATE_DIFF(CURRENT_DATE(), m.release_date, DAY) <= 180 THEN 'Established (91-180 days)'
      ELSE 'Catalog (180+ days)'
    END AS release_cohort,
    lpm.weekly_pct_change,
    lpm.current_popularity
  FROM `{project_id}.{dataset}.movies` m
  JOIN `{project_id}.{dataset}.v_latest_popularity_metrics` lpm ON m.movie_id = lpm.movie_id
  WHERE lpm.weekly_pct_change IS NOT NULL
)
SELECT
  release_cohort,
  COUNT(*) AS num_movies,
  AVG(weekly_pct_change) AS avg_weekly_pct_change,
  AVG(current_popularity) AS avg_popularity,
  PERCENTILE_CONT(weekly_pct_change, 0.5) OVER (PARTITION BY release_cohort) AS median_weekly_pct_change,
  SUM(CASE WHEN weekly_pct_change > 10 THEN 1 ELSE 0 END) AS surging_count,
  SUM(CASE WHEN weekly_pct_change < -10 THEN 1 ELSE 0 END) AS falling_count
FROM release_cohorts
GROUP BY release_cohort
ORDER BY
  CASE release_cohort
    WHEN 'New (0-30 days)' THEN 1
    WHEN 'Recent (31-90 days)' THEN 2
    WHEN 'Established (91-180 days)' THEN 3
    ELSE 4
  END;
