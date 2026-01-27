-- BigQuery Views and Queries for Viral Coefficient Analysis

-- ============================================================================
-- View 1: Vote Velocity Metrics
-- Tracks rate of vote_count increase to identify high engagement movies
-- ============================================================================
CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_vote_velocity_metrics` AS
WITH vote_metrics AS (
  SELECT
    s.movie_id,
    m.title,
    m.release_date,
    s.snapshot_date,
    s.vote_count,
    s.vote_average,
    s.vote_count_change_1d,
    s.vote_count_change_7d,
    s.vote_count_pct_change_1d,
    s.vote_count_pct_change_7d,
    -- Calculate vote velocity (avg daily votes over 7 days)
    SAFE_DIVIDE(s.vote_count_change_7d, 7) AS vote_velocity_7d,
    -- Calculate vote acceleration (change in velocity)
    SAFE_DIVIDE(s.vote_count_change_7d, 7) - SAFE_DIVIDE(
      LAG(s.vote_count_change_7d, 7) OVER (PARTITION BY s.movie_id ORDER BY s.snapshot_date),
      7
    ) AS vote_acceleration,
    ROW_NUMBER() OVER (PARTITION BY s.movie_id ORDER BY s.snapshot_date DESC) AS rn
  FROM `{project_id}.{dataset}.movie_daily_snapshots` s
  JOIN `{project_id}.{dataset}.movies` m ON s.movie_id = m.movie_id
)
SELECT
  movie_id,
  title,
  release_date,
  snapshot_date AS last_updated,
  vote_count AS current_vote_count,
  vote_average,
  vote_count_change_1d AS daily_vote_increase,
  vote_count_change_7d AS weekly_vote_increase,
  vote_count_pct_change_7d AS weekly_vote_pct_increase,
  vote_velocity_7d AS avg_daily_votes_7d,
  vote_acceleration,
  CASE
    WHEN vote_velocity_7d >= 100 THEN 'Viral'
    WHEN vote_velocity_7d >= 50 THEN 'High Engagement'
    WHEN vote_velocity_7d >= 20 THEN 'Moderate Engagement'
    WHEN vote_velocity_7d >= 5 THEN 'Low Engagement'
    ELSE 'Minimal Engagement'
  END AS engagement_level
FROM vote_metrics
WHERE rn = 1;


-- ============================================================================
-- View 2: Viral Coefficient Scores
-- Combines vote velocity and popularity changes into viral score
-- ============================================================================
CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_viral_coefficient_scores` AS
SELECT
  ma.movie_id,
  m.title,
  m.release_date,
  ma.calculation_date,
  ma.vote_velocity_7d,
  ma.vote_acceleration_7d,
  ma.popularity_wow_pct_change,
  ma.viral_coefficient,
  ma.trend_category,
  -- Additional context
  s.vote_count AS current_vote_count,
  s.popularity AS current_popularity,
  s.vote_average,
  -- Viral score percentile
  PERCENT_RANK() OVER (ORDER BY ma.viral_coefficient) * 100 AS viral_score_percentile,
  CASE
    WHEN ma.viral_coefficient >= 80 THEN 'Extremely Viral'
    WHEN ma.viral_coefficient >= 60 THEN 'Highly Viral'
    WHEN ma.viral_coefficient >= 40 THEN 'Moderately Viral'
    WHEN ma.viral_coefficient >= 20 THEN 'Slightly Viral'
    ELSE 'Not Viral'
  END AS virality_category
FROM `{project_id}.{dataset}.movie_metrics_aggregated` ma
JOIN `{project_id}.{dataset}.movies` m ON ma.movie_id = m.movie_id
JOIN `{project_id}.{dataset}.movie_daily_snapshots` s
  ON ma.movie_id = s.movie_id AND ma.calculation_date = s.snapshot_date
WHERE ma.viral_coefficient IS NOT NULL;


-- ============================================================================
-- View 3: Top Viral Movies (Trending Topics)
-- Identifies movies becoming trending topics based on viral coefficient
-- ============================================================================
CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_top_viral_movies` AS
WITH latest_viral_scores AS (
  SELECT
    movie_id,
    title,
    release_date,
    calculation_date,
    viral_coefficient,
    vote_velocity_7d,
    vote_acceleration_7d,
    popularity_wow_pct_change,
    current_vote_count,
    current_popularity,
    vote_average,
    viral_score_percentile,
    virality_category,
    trend_category,
    ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY calculation_date DESC) AS rn
  FROM `{project_id}.{dataset}.v_viral_coefficient_scores`
)
SELECT
  movie_id,
  title,
  release_date,
  DATE_DIFF(calculation_date, release_date, DAY) AS days_since_release,
  calculation_date AS last_updated,
  viral_coefficient,
  virality_category,
  trend_category,
  vote_velocity_7d AS avg_daily_votes,
  vote_acceleration_7d AS vote_acceleration,
  current_vote_count AS total_votes,
  vote_average AS rating,
  current_popularity,
  popularity_wow_pct_change AS wow_popularity_change,
  viral_score_percentile,
  RANK() OVER (ORDER BY viral_coefficient DESC) AS viral_rank
FROM latest_viral_scores
WHERE rn = 1
  AND viral_coefficient >= 20  -- Only show movies with meaningful virality
ORDER BY viral_coefficient DESC
LIMIT 100;


-- ============================================================================
-- View 4: Emerging Viral Trends
-- Identifies movies in early stages of going viral (high acceleration)
-- ============================================================================
CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_emerging_viral_trends` AS
WITH recent_metrics AS (
  SELECT
    ma.movie_id,
    m.title,
    m.release_date,
    ma.calculation_date,
    ma.vote_velocity_7d,
    ma.vote_acceleration_7d,
    ma.viral_coefficient,
    ma.trend_category,
    s.vote_count,
    s.popularity,
    -- Calculate if movie is accelerating in virality
    ma.vote_acceleration_7d > 0 AS is_accelerating,
    -- Calculate if movie has crossed viral threshold recently
    LAG(ma.viral_coefficient, 7) OVER (PARTITION BY ma.movie_id ORDER BY ma.calculation_date) AS viral_coef_7d_ago,
    ROW_NUMBER() OVER (PARTITION BY ma.movie_id ORDER BY ma.calculation_date DESC) AS rn
  FROM `{project_id}.{dataset}.movie_metrics_aggregated` ma
  JOIN `{project_id}.{dataset}.movies` m ON ma.movie_id = m.movie_id
  JOIN `{project_id}.{dataset}.movie_daily_snapshots` s
    ON ma.movie_id = s.movie_id AND ma.calculation_date = s.snapshot_date
  WHERE ma.vote_acceleration_7d IS NOT NULL
)
SELECT
  movie_id,
  title,
  release_date,
  calculation_date AS last_updated,
  vote_velocity_7d,
  vote_acceleration_7d,
  viral_coefficient AS current_viral_score,
  viral_coef_7d_ago AS viral_score_7d_ago,
  viral_coefficient - COALESCE(viral_coef_7d_ago, 0) AS viral_score_change_7d,
  trend_category,
  vote_count,
  popularity,
  is_accelerating
FROM recent_metrics
WHERE rn = 1
  AND is_accelerating = TRUE
  AND vote_velocity_7d >= 10  -- At least 10 votes per day
  AND (viral_coefficient > 30 OR vote_acceleration_7d > 5)
ORDER BY vote_acceleration_7d DESC
LIMIT 100;


-- ============================================================================
-- View 5: Vote Engagement Timeline
-- Shows how vote engagement evolves over time for movies
-- ============================================================================
CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_vote_engagement_timeline` AS
SELECT
  s.movie_id,
  m.title,
  s.snapshot_date,
  s.vote_count,
  s.vote_average,
  s.vote_count_change_1d,
  -- Cumulative vote velocity (running average)
  AVG(s.vote_count_change_1d) OVER (
    PARTITION BY s.movie_id
    ORDER BY s.snapshot_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS vote_velocity_7d_ma,
  -- Peak vote detection
  MAX(s.vote_count_change_1d) OVER (
    PARTITION BY s.movie_id
    ORDER BY s.snapshot_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS peak_daily_votes_so_far,
  -- Engagement trend
  CASE
    WHEN s.vote_count_change_1d > AVG(s.vote_count_change_1d) OVER (
      PARTITION BY s.movie_id
      ORDER BY s.snapshot_date
      ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
    ) THEN 'Increasing'
    WHEN s.vote_count_change_1d < AVG(s.vote_count_change_1d) OVER (
      PARTITION BY s.movie_id
      ORDER BY s.snapshot_date
      ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
    ) THEN 'Decreasing'
    ELSE 'Stable'
  END AS engagement_trend
FROM `{project_id}.{dataset}.movie_daily_snapshots` s
JOIN `{project_id}.{dataset}.movies` m ON s.movie_id = m.movie_id
ORDER BY s.movie_id, s.snapshot_date;


-- ============================================================================
-- Query 1: Daily Viral Movies Report
-- Identifies which movies are becoming trending topics
-- ============================================================================
SELECT
  tvm.movie_id,
  tvm.title,
  tvm.release_date,
  tvm.days_since_release,
  tvm.virality_category,
  tvm.viral_coefficient,
  tvm.avg_daily_votes,
  tvm.total_votes,
  tvm.rating,
  tvm.current_popularity,
  tvm.wow_popularity_change,
  STRING_AGG(g.name, ', ' ORDER BY g.name) AS genres,
  m.overview
FROM `{project_id}.{dataset}.v_top_viral_movies` tvm
JOIN `{project_id}.{dataset}.movies` m ON tvm.movie_id = m.movie_id
LEFT JOIN UNNEST(m.genres) g
WHERE tvm.viral_rank <= 50
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13
ORDER BY tvm.viral_coefficient DESC;


-- ============================================================================
-- Query 2: Viral vs Non-Viral Comparison
-- Compares characteristics of viral vs non-viral movies
-- ============================================================================
WITH viral_classification AS (
  SELECT
    m.movie_id,
    m.title,
    CASE WHEN vcs.viral_coefficient >= 40 THEN 'Viral' ELSE 'Non-Viral' END AS viral_status,
    vcs.viral_coefficient,
    vcs.vote_velocity_7d,
    vcs.current_vote_count,
    vcs.vote_average,
    vcs.current_popularity,
    DATE_DIFF(vcs.calculation_date, m.release_date, DAY) AS days_since_release,
    m.budget,
    m.revenue
  FROM `{project_id}.{dataset}.movies` m
  JOIN (
    SELECT movie_id, viral_coefficient, vote_velocity_7d, current_vote_count,
           vote_average, current_popularity, calculation_date,
           ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY calculation_date DESC) AS rn
    FROM `{project_id}.{dataset}.v_viral_coefficient_scores`
  ) vcs ON m.movie_id = vcs.movie_id AND vcs.rn = 1
)
SELECT
  viral_status,
  COUNT(*) AS num_movies,
  AVG(viral_coefficient) AS avg_viral_coefficient,
  AVG(vote_velocity_7d) AS avg_vote_velocity,
  AVG(current_vote_count) AS avg_total_votes,
  AVG(vote_average) AS avg_rating,
  AVG(current_popularity) AS avg_popularity,
  AVG(days_since_release) AS avg_days_since_release,
  AVG(CASE WHEN budget > 0 THEN budget ELSE NULL END) AS avg_budget,
  AVG(CASE WHEN revenue > 0 THEN revenue ELSE NULL END) AS avg_revenue,
  PERCENTILE_CONT(vote_velocity_7d, 0.5) OVER (PARTITION BY viral_status) AS median_vote_velocity
FROM viral_classification
GROUP BY viral_status
ORDER BY viral_status;


-- ============================================================================
-- Query 3: Viral Breakout Detection
-- Identifies movies that recently crossed viral threshold
-- ============================================================================
WITH viral_history AS (
  SELECT
    vcs.movie_id,
    vcs.title,
    vcs.calculation_date,
    vcs.viral_coefficient,
    LAG(vcs.viral_coefficient, 1) OVER (PARTITION BY vcs.movie_id ORDER BY vcs.calculation_date) AS prev_day_viral_coef,
    LAG(vcs.viral_coefficient, 7) OVER (PARTITION BY vcs.movie_id ORDER BY vcs.calculation_date) AS prev_week_viral_coef
  FROM `{project_id}.{dataset}.v_viral_coefficient_scores` vcs
)
SELECT
  movie_id,
  title,
  calculation_date AS breakout_date,
  viral_coefficient AS current_viral_score,
  prev_week_viral_coef AS viral_score_7d_ago,
  viral_coefficient - prev_week_viral_coef AS viral_score_increase,
  CASE
    WHEN prev_week_viral_coef < 20 AND viral_coefficient >= 40 THEN 'Major Breakout'
    WHEN prev_week_viral_coef < 40 AND viral_coefficient >= 60 THEN 'Strong Breakout'
    WHEN viral_coefficient - prev_week_viral_coef >= 20 THEN 'Rapid Growth'
    ELSE 'Steady Growth'
  END AS breakout_type
FROM viral_history
WHERE viral_coefficient >= 40  -- Currently viral
  AND prev_week_viral_coef IS NOT NULL
  AND viral_coefficient > prev_week_viral_coef  -- Increasing
  AND calculation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)  -- Last 30 days
ORDER BY viral_coefficient - prev_week_viral_coef DESC
LIMIT 50;


-- ============================================================================
-- Query 4: Genre-wise Viral Performance
-- Analyzes which genres tend to go viral
-- ============================================================================
WITH genre_viral_metrics AS (
  SELECT
    g.name AS genre,
    vcs.movie_id,
    vcs.viral_coefficient,
    vcs.vote_velocity_7d,
    vcs.virality_category
  FROM `{project_id}.{dataset}.v_viral_coefficient_scores` vcs
  JOIN `{project_id}.{dataset}.movies` m ON vcs.movie_id = m.movie_id
  CROSS JOIN UNNEST(m.genres) AS g
  WHERE vcs.calculation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
)
SELECT
  genre,
  COUNT(DISTINCT movie_id) AS num_movies,
  AVG(viral_coefficient) AS avg_viral_coefficient,
  MAX(viral_coefficient) AS max_viral_coefficient,
  AVG(vote_velocity_7d) AS avg_vote_velocity,
  SUM(CASE WHEN virality_category IN ('Extremely Viral', 'Highly Viral') THEN 1 ELSE 0 END) AS highly_viral_count,
  SAFE_DIVIDE(
    SUM(CASE WHEN virality_category IN ('Extremely Viral', 'Highly Viral') THEN 1 ELSE 0 END),
    COUNT(DISTINCT movie_id)
  ) * 100 AS viral_movie_percentage
FROM genre_viral_metrics
GROUP BY genre
HAVING COUNT(DISTINCT movie_id) >= 10  -- Only genres with sufficient sample size
ORDER BY avg_viral_coefficient DESC;
