"""
Airflow DAG for daily incremental ingestion of TMDB movie data
Fetches changed movies and updates BigQuery tables with latest metrics
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys
import logging

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG configuration
dag = DAG(
    'tmdb_daily_ingestion',
    default_args=default_args,
    description='Daily ingestion of TMDB movie changes and metric updates',
    schedule_interval='0 16 * * *',  # Run daily at 4 PM UTC (10 AM CST)
    start_date=days_ago(1),
    catchup=False,
    tags=['tmdb', 'movies', 'analytics'],
)


def get_changed_movies(**context):
    """
    Task 1: Fetch list of movie IDs that changed in the last 24 hours
    """
    execution_date = context['execution_date']
    dag_run_id = context['dag_run'].run_id

    # Configuration
    TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
    if not TMDB_API_KEY:
        raise ValueError("TMDB_API_KEY environment variable is required")

    # Calculate date range (last 24 hours)
    end_date = execution_date.strftime("%Y-%m-%d")
    start_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info(f"Fetching changed movies from {start_date} to {end_date}")

    # Initialize TMDB client (import here to avoid top-level import timeout)
    from tmdb_client import TMDBClient
    tmdb_client = TMDBClient(api_key=TMDB_API_KEY)

    try:
        # Get changed movie IDs
        changed_movie_ids = tmdb_client.get_changed_movie_ids(
            start_date=start_date,
            end_date=end_date
        )

        logger.info(f"Found {len(changed_movie_ids)} changed movies")

        # Store movie IDs in XCom for next task
        context['task_instance'].xcom_push(key='changed_movie_ids', value=changed_movie_ids)
        context['task_instance'].xcom_push(key='start_date', value=start_date)
        context['task_instance'].xcom_push(key='end_date', value=end_date)

        return len(changed_movie_ids)

    except Exception as e:
        logger.error(f"Error fetching changed movies: {e}", exc_info=True)
        raise


def fetch_movie_details(**context):
    """
    Task 2: Fetch detailed information for changed movies
    """
    task_instance = context['task_instance']
    changed_movie_ids = task_instance.xcom_pull(task_ids='get_changed_movies', key='changed_movie_ids')

    if not changed_movie_ids:
        logger.info("No changed movies to process")
        return 0

    # Configuration
    TMDB_API_KEY = os.environ.get("TMDB_API_KEY")

    logger.info(f"Fetching details for {len(changed_movie_ids)} movies")

    # Initialize TMDB client (import here to avoid top-level import timeout)
    from tmdb_client import TMDBClient
    tmdb_client = TMDBClient(api_key=TMDB_API_KEY)

    try:
        # Fetch details for all changed movies
        movie_details = tmdb_client.get_movies_details_batch(changed_movie_ids)

        logger.info(f"Successfully fetched details for {len(movie_details)} movies")

        # Store movie details in XCom for next task
        task_instance.xcom_push(key='movie_details', value=movie_details)

        return len(movie_details)

    except Exception as e:
        logger.error(f"Error fetching movie details: {e}", exc_info=True)
        raise


def load_to_bigquery(**context):
    """
    Task 3: Load movie data and snapshots to BigQuery
    """
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    dag_run_id = context['dag_run'].run_id

    movie_details = task_instance.xcom_pull(task_ids='fetch_movie_details', key='movie_details')

    if not movie_details:
        logger.info("No movie details to load")
        return 0

    # Configuration
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "tmdb_analytics")

    if not GCP_PROJECT_ID:
        raise ValueError("GCP_PROJECT_ID environment variable is required")

    logger.info(f"Loading {len(movie_details)} movies to BigQuery")

    # Initialize BigQuery loader (import here to avoid top-level import timeout)
    from bigquery_loader import BigQueryLoader
    bq_loader = BigQueryLoader(
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID
    )

    try:
        # Load movies to dimension table
        movies_loaded = bq_loader.load_movies(movie_details)
        logger.info(f"Loaded {movies_loaded} movies to movies table")

        # Load daily snapshots
        snapshot_date = execution_date.date()
        snapshots_loaded = bq_loader.load_daily_snapshots(movie_details, snapshot_date=snapshot_date)
        logger.info(f"Loaded {snapshots_loaded} snapshots to daily snapshots table")

        return snapshots_loaded

    except Exception as e:
        logger.error(f"Error loading to BigQuery: {e}", exc_info=True)
        raise


def compute_metrics(**context):
    """
    Task 4: Compute derived and aggregated metrics
    """
    execution_date = context['execution_date']

    # Configuration
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "tmdb_analytics")

    logger.info("Computing metrics for all movies")

    # Initialize BigQuery loader (import here to avoid top-level import timeout)
    from bigquery_loader import BigQueryLoader
    bq_loader = BigQueryLoader(
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID
    )

    try:
        # Compute derived metrics (day-over-day changes)
        logger.info("Computing derived metrics...")
        bq_loader.compute_derived_metrics()

        # Compute aggregated metrics (7-day MA, viral coefficient, etc.)
        logger.info("Computing aggregated metrics...")
        calculation_date = execution_date.date()
        bq_loader.compute_aggregated_metrics(calculation_date=calculation_date)

        logger.info("Metrics computation completed successfully")
        return True

    except Exception as e:
        logger.error(f"Error computing metrics: {e}", exc_info=True)
        raise


def refresh_tracked_movies(**context):
    """
    Task 5 (Weekly): Refresh snapshots for all tracked movies (not just changed ones)
    This ensures we capture gradual popularity/vote changes even if movie metadata doesn't change
    """
    execution_date = context['execution_date']
    dag_run_id = context['dag_run'].run_id

    # Configuration
    TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "tmdb_analytics")

    logger.info("Refreshing all tracked movies")

    # Get list of all tracked movie IDs from BigQuery (import here to avoid top-level import timeout)
    from bigquery_loader import BigQueryLoader
    bq_loader = BigQueryLoader(
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID
    )

    try:
        # Query to get all movie IDs
        query = f"""
        SELECT DISTINCT movie_id
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.movies`
        ORDER BY movie_id
        """
        query_job = bq_loader.client.query(query)
        results = query_job.result()
        movie_ids = [row.movie_id for row in results]

        logger.info(f"Found {len(movie_ids)} movies to refresh")

        # Fetch latest details for all movies
        from tmdb_client import TMDBClient
        tmdb_client = TMDBClient(api_key=TMDB_API_KEY)
        movie_details = tmdb_client.get_movies_details_batch(movie_ids)

        logger.info(f"Fetched details for {len(movie_details)} movies")

        # Load snapshots (don't update movies table to preserve historical data)
        snapshot_date = execution_date.date()
        snapshots_loaded = bq_loader.load_daily_snapshots(movie_details, snapshot_date=snapshot_date)

        logger.info(f"Refreshed {snapshots_loaded} movie snapshots")

        return snapshots_loaded

    except Exception as e:
        logger.error(f"Error refreshing tracked movies: {e}", exc_info=True)
        raise


# Define tasks
task_get_changed_movies = PythonOperator(
    task_id='get_changed_movies',
    python_callable=get_changed_movies,
    dag=dag,
)

task_fetch_movie_details = PythonOperator(
    task_id='fetch_movie_details',
    python_callable=fetch_movie_details,
    dag=dag,
)

task_load_to_bigquery = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag,
)

task_compute_metrics = PythonOperator(
    task_id='compute_metrics',
    python_callable=compute_metrics,
    dag=dag,
)

# Define task dependencies
task_get_changed_movies >> task_fetch_movie_details >> task_load_to_bigquery >> task_compute_metrics


# Separate DAG for weekly full refresh
dag_weekly_refresh = DAG(
    'tmdb_weekly_refresh',
    default_args=default_args,
    description='Weekly refresh of all tracked movies to capture gradual metric changes',
    schedule_interval='0 3 * * 0',  # Run weekly on Sundays at 3 AM UTC
    start_date=days_ago(1),
    catchup=False,
    tags=['tmdb', 'movies', 'analytics', 'refresh'],
)

task_refresh_tracked_movies = PythonOperator(
    task_id='refresh_tracked_movies',
    python_callable=refresh_tracked_movies,
    dag=dag_weekly_refresh,
)

task_compute_metrics_weekly = PythonOperator(
    task_id='compute_metrics',
    python_callable=compute_metrics,
    dag=dag_weekly_refresh,
)

# Define task dependencies for weekly refresh
task_refresh_tracked_movies >> task_compute_metrics_weekly
