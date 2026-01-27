# TMDB Content Analytics Pipeline

A comprehensive data pipeline for tracking movie popularity momentum and viral coefficients using TMDB API, Apache Airflow, and Google BigQuery.

## Overview

This pipeline tracks 1000 most popular movies from TMDB and monitors their engagement metrics daily to identify trending movies and viral content.

### Key Features

- **Initial Load**: Fetch and store 1000 most popular movies from TMDB
- **Incremental Updates**: Daily ingestion of movie changes using TMDB changes API
- **Popularity Momentum**: Track how popularity scores change day-over-day with 7-day moving averages
- **Viral Coefficient**: Calculate rate of vote count increase to identify trending topics
- **Automated Workflow**: Airflow DAGs for scheduled data ingestion and metric computation

## Metrics Tracked

### 1. Popularity Momentum
- **Day-over-day popularity changes**
- **7-day moving average** of popularity scores
- **Week-over-week % change** in popularity
- **Trend classification**: Surging, Rising, Stable, Declining, Falling

**Use Case**: *"Which movies are gaining vs. losing traction?"*

### 2. Viral Coefficient
- **Vote velocity**: Rate of vote_count increase over time
- **Vote acceleration**: Change in vote velocity
- **Viral score**: Normalized 0-100 score based on engagement
- **Virality categories**: Extremely Viral, Highly Viral, Moderately Viral

**Use Case**: *"Which movies are becoming trending topics?"*

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│  TMDB API   │─────▶│   Airflow    │─────▶│  BigQuery   │
│             │      │   DAGs       │      │   Tables    │
└─────────────┘      └──────────────┘      └─────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │   Metrics    │
                     │ Computation  │
                     └──────────────┘
```

### Components

1. **Data Ingestion Layer** ([scripts/](scripts/))
   - `tmdb_client.py`: TMDB API client with rate limiting
   - `bigquery_loader.py`: BigQuery data loader and metric computation
   - `initial_load.py`: Script for initial 1000 movies ingestion

2. **Orchestration Layer** ([dags/](dags/))
   - `tmdb_daily_ingestion_dag.py`: Daily incremental updates
   - `tmdb_weekly_refresh_dag.py`: Weekly full refresh of tracked movies

3. **Data Storage** ([schema/](schema/))
   - `movies`: Dimension table with movie metadata
   - `movie_daily_snapshots`: Fact table with daily metrics
   - `movie_metrics_aggregated`: Pre-computed analytics
   - `ingestion_log`: Pipeline execution logs

4. **Analytics Layer** ([queries/](queries/))
   - `popularity_momentum_views.sql`: Views for popularity analysis
   - `viral_coefficient_views.sql`: Views for viral trend analysis

## Setup

### Prerequisites

- **Docker** and **Docker Compose** (recommended) OR
- Python 3.8+
- Google Cloud Platform account with BigQuery enabled
- TMDB API key ([Get one here](https://www.themoviedb.org/settings/api))

### Option 1: Docker Installation (Recommended)

Docker makes it easy to run the entire pipeline including Airflow without manual dependency management.

**✨ Features:**
- **Bind mounts enabled** - Edit code locally, changes reflect immediately in containers
- **No rebuild needed** - Modify Python scripts, DAGs, and configs without rebuilding
- See [BIND_MOUNTS.md](BIND_MOUNTS.md) for live development workflow

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd content-analytics-pipeline
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env and add your TMDB_API_KEY and GCP_PROJECT_ID
   ```

3. **Set up Google Cloud credentials**
   ```bash
   # Download your GCP service account key and save as gcp-credentials.json
   # in the project root directory
   ```

4. **Initialize Airflow with Docker**
   ```bash
   # Create required directories
   mkdir -p ./dags ./logs ./plugins ./config

   # Set Airflow UID (on Linux)
   echo -e "AIRFLOW_UID=$(id -u)" >> .env

   # Initialize the database
   docker-compose up airflow-init
   ```

5. **Start all services**
   ```bash
   docker-compose up -d
   ```

6. **Create BigQuery tables (REQUIRED!)**
   ```bash
   # This creates the dataset and all required tables
   docker-compose exec airflow-webserver python /opt/airflow/scripts/setup_bigquery.py
   ```

7. **Access Airflow UI**
   - Open http://localhost:8080
   - Username: `airflow` (or as set in `.env`)
   - Password: `airflow` (or as set in `.env`)

8. **Run initial load (to load 1000 movies)**
   ```bash
   docker-compose exec airflow-webserver python /opt/airflow/scripts/initial_load.py
   ```

9. **Verify data loaded**
   ```bash
   # Check in BigQuery Console or query from container
   docker-compose exec airflow-webserver python -c "
   from google.cloud import bigquery
   client = bigquery.Client()
   query = 'SELECT COUNT(*) as count FROM \`YOUR_PROJECT.tmdb_analytics.movies\`'
   result = list(client.query(query).result())
   print(f'Movies loaded: {result[0].count}')
   "
   ```

10. **Stop services**
    ```bash
    docker-compose down
    ```

11. **Clean up (remove volumes)**
    ```bash
    docker-compose down --volumes --remove-orphans
    ```

### Option 2: Local Python Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd content-analytics-pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env and add your TMDB_API_KEY and GCP_PROJECT_ID
   ```

4. **Set up Google Cloud credentials**
   ```bash
   # Set path to your service account key file
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   ```

5. **Create BigQuery dataset and tables**
   ```bash
   # Run the Python setup script
   python scripts/setup_bigquery.py
   ```

6. **Initialize Airflow**
   ```bash
   export AIRFLOW_HOME=$(pwd)/airflow
   airflow db init
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com
   ```

7. **Copy DAGs to Airflow**
   ```bash
   ln -s $(pwd)/dags $AIRFLOW_HOME/dags
   ```

## Usage

### Initial Load

#### Docker Setup
Run the initial load script to fetch and store 1000 most popular movies:

```bash
docker-compose exec airflow-webserver bash /opt/airflow/scripts/run_initial_load.sh
```

Or manually:
```bash
docker-compose exec airflow-webserver python /opt/airflow/scripts/initial_load.py
```

#### Local Setup
```bash
python scripts/initial_load.py
```

This will:
- Fetch 1000 most popular movies using TMDB discover endpoint
- Load movie details into BigQuery `movies` table
- Create initial snapshots in `movie_daily_snapshots` table
- Compute initial metrics

### Managing Airflow

#### Docker Setup
```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler

# Stop services
docker-compose down

# Restart a specific service
docker-compose restart airflow-scheduler
```

Access the Airflow UI at `http://localhost:8080`

#### Local Setup
```bash
# Start the web server (in one terminal)
airflow webserver --port 8080

# Start the scheduler (in another terminal)
airflow scheduler
```

Access the Airflow UI at `http://localhost:8080`

### Airflow DAGs

#### Daily Ingestion DAG (`tmdb_daily_ingestion`)
- **Schedule**: Daily at 2 AM UTC
- **Tasks**:
  1. Get changed movies from TMDB
  2. Fetch movie details
  3. Load to BigQuery
  4. Compute metrics

#### Weekly Refresh DAG (`tmdb_weekly_refresh`)
- **Schedule**: Weekly on Sundays at 3 AM UTC
- **Tasks**:
  1. Refresh all tracked movies
  2. Compute metrics

### Query Analytics

Create BigQuery views for easy analysis:

```bash
# Create popularity momentum views
bq query --use_legacy_sql=false < queries/popularity_momentum_views.sql

# Create viral coefficient views
bq query --use_legacy_sql=false < queries/viral_coefficient_views.sql
```

### Example Queries

**Top 10 movies gaining traction:**
```sql
SELECT title, weekly_pct_change, current_popularity
FROM `project.dataset.v_top_gaining_movies`
LIMIT 10;
```

**Movies becoming viral:**
```sql
SELECT title, viral_coefficient, avg_daily_votes, virality_category
FROM `project.dataset.v_top_viral_movies`
WHERE viral_rank <= 20;
```

**Emerging viral trends:**
```sql
SELECT title, vote_acceleration_7d, viral_score_change_7d, trend_category
FROM `project.dataset.v_emerging_viral_trends`
LIMIT 10;
```

## Configuration

Edit [config.yaml](config.yaml) to customize:
- BigQuery settings (project, dataset, location)
- TMDB API rate limits
- Metric thresholds and windows
- Airflow schedules
- Alerting configuration

## Data Schema

### Tables

#### `movies`
Movie dimension table with metadata (title, genres, budget, revenue, etc.)

#### `movie_daily_snapshots`
Daily snapshots of movie metrics:
- `popularity`, `vote_count`, `vote_average`
- Day-over-day and week-over-week changes
- Derived metrics for momentum tracking

#### `movie_metrics_aggregated`
Pre-computed analytics:
- 7-day moving averages
- Week-over-week changes
- Vote velocity and acceleration
- Viral coefficients
- Trend categories

#### `ingestion_log`
Pipeline execution logs for monitoring

### Views

- `v_latest_popularity_metrics`: Latest popularity metrics with momentum status
- `v_popularity_7d_moving_average`: Smoothed popularity trends
- `v_top_gaining_movies`: Movies gaining traction
- `v_top_declining_movies`: Movies losing traction
- `v_vote_velocity_metrics`: Vote engagement metrics
- `v_viral_coefficient_scores`: Viral scores and categories
- `v_top_viral_movies`: Top trending movies
- `v_emerging_viral_trends`: Movies in early viral stages

## Monitoring

Monitor pipeline health through:

1. **Airflow UI**: Task execution status and logs
2. **BigQuery**: `ingestion_log` table for job history
3. **Metrics**: Data freshness and ingestion success rates

## Troubleshooting

### Common Issues

1. **TMDB API rate limiting**
   - The client implements exponential backoff and rate limiting
   - Adjust `rate_limit.requests_per_second` in config.yaml

2. **BigQuery quota exceeded**
   - Check your project quotas in GCP Console
   - Consider reducing `NUM_MOVIES` or refresh frequency

3. **Airflow task failures**
   - Check logs in Airflow UI
   - Verify environment variables are set correctly
   - Ensure GCP credentials are valid

## Future Enhancements

- [ ] Add sentiment analysis from movie reviews
- [ ] Integrate with other data sources (Box Office Mojo, IMDb)
- [ ] Real-time streaming ingestion
- [ ] Machine learning models for trend prediction
- [ ] Dashboard visualization (Looker, Tableau, etc.)
- [ ] Slack/email alerts for viral breakouts

## License

MIT License

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
