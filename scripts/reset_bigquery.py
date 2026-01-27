"""
Reset BigQuery tables - Delete and recreate all tables
WARNING: This will delete all existing data!
"""
import os
import logging
from google.cloud import bigquery
from setup_bigquery import (
    create_dataset,
    create_movies_table,
    create_snapshots_table,
    create_aggregated_metrics_table
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Delete all tables and recreate them"""

    # Configuration
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "tmdb_analytics")
    BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

    if not GCP_PROJECT_ID:
        raise ValueError("GCP_PROJECT_ID environment variable is required")

    logger.warning("=" * 70)
    logger.warning("WARNING: This will DELETE ALL DATA in BigQuery tables!")
    logger.warning(f"Project: {GCP_PROJECT_ID}")
    logger.warning(f"Dataset: {BQ_DATASET_ID}")
    logger.warning("=" * 70)

    # Initialize BigQuery client
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dataset_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}"

    tables_to_delete = [
        'movies',
        'movie_daily_snapshots',
        'movie_metrics_aggregated'
    ]

    try:
        # Step 1: Delete existing tables
        logger.info("Step 1: Deleting existing tables...")
        for table_name in tables_to_delete:
            table_id = f"{dataset_ref}.{table_name}"
            try:
                client.delete_table(table_id)
                logger.info(f"✓ Deleted table: {table_name}")
            except Exception as e:
                logger.info(f"✗ Could not delete {table_name} (may not exist): {e}")

        # Step 2: Ensure dataset exists
        logger.info("\nStep 2: Ensuring dataset exists...")
        create_dataset(client, GCP_PROJECT_ID, BQ_DATASET_ID, BQ_LOCATION)

        # Step 3: Recreate tables with correct schema
        logger.info("\nStep 3: Recreating tables...")
        create_movies_table(client, dataset_ref)
        create_snapshots_table(client, dataset_ref)
        create_aggregated_metrics_table(client, dataset_ref)

        logger.info("\n" + "=" * 70)
        logger.info("✓ BigQuery reset completed successfully!")
        logger.info("=" * 70)
        logger.info(f"Dataset: {dataset_ref}")
        logger.info("Tables recreated:")
        for table_name in tables_to_delete:
            logger.info(f"  ✓ {table_name}")
        logger.info("=" * 70)
        logger.info("\nYou can now run the initial load:")
        logger.info("  python /opt/airflow/scripts/initial_load.py")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Error during BigQuery reset: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
