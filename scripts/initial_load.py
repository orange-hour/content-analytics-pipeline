"""
Initial load script to fetch and store top 1000 movies from TMDB
"""
import os
import logging
from datetime import date
from tmdb_client import TMDBClient
from bigquery_loader import BigQueryLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main function to perform initial load of 1000 movies"""

    # Configuration
    TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "tmdb_analytics")
    NUM_MOVIES = int(os.environ.get("NUM_MOVIES", "1000"))

    if not TMDB_API_KEY:
        raise ValueError("TMDB_API_KEY environment variable is required")
    if not GCP_PROJECT_ID:
        raise ValueError("GCP_PROJECT_ID environment variable is required")

    logger.info(f"Starting initial load of {NUM_MOVIES} movies")

    # Initialize clients
    tmdb_client = TMDBClient(api_key=TMDB_API_KEY)
    bq_loader = BigQueryLoader(
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID
    )

    try:
        # Step 0: Ensure dataset and tables exist
        logger.info("Ensuring BigQuery dataset exists...")
        bq_loader._ensure_dataset_exists()

        logger.info("Checking if tables exist...")
        logger.info("If tables don't exist, please run: python /opt/airflow/scripts/setup_bigquery.py")

        # Step 1: Fetch top N movies using discover endpoint
        logger.info(f"Fetching top {NUM_MOVIES} popular movies from TMDB...")
        movies = tmdb_client.get_top_movies(limit=NUM_MOVIES)
        logger.info(f"Successfully fetched {len(movies)} movies")

        if not movies:
            logger.error("No movies fetched. Exiting.")
            return

        # Step 2: Get detailed information for each movie
        logger.info("Fetching detailed information for each movie...")
        movie_ids = [movie["id"] for movie in movies]
        detailed_movies = tmdb_client.get_movies_details_batch(movie_ids)
        logger.info(f"Successfully fetched details for {len(detailed_movies)} movies")

        # Step 3: Load movies into BigQuery movies table
        logger.info("Loading movies into BigQuery movies table...")
        movies_loaded = bq_loader.load_movies(detailed_movies)
        logger.info(f"Loaded {movies_loaded} movies to movies table")

        # Step 4: Load initial snapshots into daily snapshots table
        logger.info("Loading initial snapshots into BigQuery...")
        today = date.today()
        snapshots_loaded = bq_loader.load_daily_snapshots(detailed_movies, snapshot_date=today)
        logger.info(f"Loaded {snapshots_loaded} snapshots to daily snapshots table")

        # Step 5: Compute derived metrics
        logger.info("Computing derived metrics...")
        bq_loader.compute_derived_metrics()
        logger.info("Derived metrics computed successfully")

        # Step 6: Compute aggregated metrics
        logger.info("Computing aggregated metrics...")
        bq_loader.compute_aggregated_metrics(calculation_date=today)
        logger.info("Aggregated metrics computed successfully")

        logger.info("Initial load completed successfully!")
        logger.info(f"Summary:")
        logger.info(f"  - Movies fetched: {len(movies)}")
        logger.info(f"  - Movies with details: {len(detailed_movies)}")
        logger.info(f"  - Movies loaded to BQ: {movies_loaded}")
        logger.info(f"  - Snapshots loaded to BQ: {snapshots_loaded}")

    except Exception as e:
        logger.error(f"Error during initial load: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
