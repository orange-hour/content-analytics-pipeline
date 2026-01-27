"""
Validate data integrity in BigQuery tables
Checks for NULL values in required fields
"""
import os
import logging
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def validate_movies_table(client: bigquery.Client, dataset_ref: str):
    """Validate movies table for NULL values in required fields"""

    logger.info("Validating movies table...")

    # Check for NULL last_updated
    query = f"""
    SELECT COUNT(*) as null_count
    FROM `{dataset_ref}.movies`
    WHERE last_updated IS NULL
    """
    result = list(client.query(query).result())
    null_count = result[0].null_count

    if null_count > 0:
        logger.error(f"❌ Found {null_count} rows with NULL last_updated")
        return False
    else:
        logger.info("✓ No NULL values in last_updated")

    # Check for NULL movie_id
    query = f"""
    SELECT COUNT(*) as null_count
    FROM `{dataset_ref}.movies`
    WHERE movie_id IS NULL
    """
    result = list(client.query(query).result())
    null_count = result[0].null_count

    if null_count > 0:
        logger.error(f"❌ Found {null_count} rows with NULL movie_id")
        return False
    else:
        logger.info("✓ No NULL values in movie_id")

    # Check for NULL title
    query = f"""
    SELECT COUNT(*) as null_count
    FROM `{dataset_ref}.movies`
    WHERE title IS NULL
    """
    result = list(client.query(query).result())
    null_count = result[0].null_count

    if null_count > 0:
        logger.error(f"❌ Found {null_count} rows with NULL title")
        return False
    else:
        logger.info("✓ No NULL values in title")

    # Get total count
    query = f"""
    SELECT COUNT(*) as total_count
    FROM `{dataset_ref}.movies`
    """
    result = list(client.query(query).result())
    total_count = result[0].total_count
    logger.info(f"✓ Total movies: {total_count}")

    return True


def validate_snapshots_table(client: bigquery.Client, dataset_ref: str):
    """Validate snapshots table for NULL values in required fields"""

    logger.info("\nValidating movie_daily_snapshots table...")

    # Check for NULL ingestion_timestamp
    query = f"""
    SELECT COUNT(*) as null_count
    FROM `{dataset_ref}.movie_daily_snapshots`
    WHERE ingestion_timestamp IS NULL
    """
    result = list(client.query(query).result())
    null_count = result[0].null_count

    if null_count > 0:
        logger.error(f"❌ Found {null_count} rows with NULL ingestion_timestamp")
        return False
    else:
        logger.info("✓ No NULL values in ingestion_timestamp")

    # Check for NULL movie_id
    query = f"""
    SELECT COUNT(*) as null_count
    FROM `{dataset_ref}.movie_daily_snapshots`
    WHERE movie_id IS NULL
    """
    result = list(client.query(query).result())
    null_count = result[0].null_count

    if null_count > 0:
        logger.error(f"❌ Found {null_count} rows with NULL movie_id")
        return False
    else:
        logger.info("✓ No NULL values in movie_id")

    # Check for NULL snapshot_date
    query = f"""
    SELECT COUNT(*) as null_count
    FROM `{dataset_ref}.movie_daily_snapshots`
    WHERE snapshot_date IS NULL
    """
    result = list(client.query(query).result())
    null_count = result[0].null_count

    if null_count > 0:
        logger.error(f"❌ Found {null_count} rows with NULL snapshot_date")
        return False
    else:
        logger.info("✓ No NULL values in snapshot_date")

    # Get total count
    query = f"""
    SELECT COUNT(*) as total_count
    FROM `{dataset_ref}.movie_daily_snapshots`
    """
    result = list(client.query(query).result())
    total_count = result[0].total_count
    logger.info(f"✓ Total snapshots: {total_count}")

    return True


def main():
    """Main validation function"""

    # Configuration
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "tmdb_analytics")

    if not GCP_PROJECT_ID:
        raise ValueError("GCP_PROJECT_ID environment variable is required")

    logger.info("=" * 70)
    logger.info("TMDB Content Analytics - Data Validation")
    logger.info("=" * 70)
    logger.info(f"Project: {GCP_PROJECT_ID}")
    logger.info(f"Dataset: {BQ_DATASET_ID}")
    logger.info("=" * 70)

    # Initialize BigQuery client
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dataset_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}"

    try:
        # Validate movies table
        movies_valid = validate_movies_table(client, dataset_ref)

        # Validate snapshots table
        snapshots_valid = validate_snapshots_table(client, dataset_ref)

        # Summary
        logger.info("\n" + "=" * 70)
        if movies_valid and snapshots_valid:
            logger.info("✓ ALL VALIDATIONS PASSED")
            logger.info("=" * 70)
            logger.info("Data integrity checks:")
            logger.info("  ✓ No NULL values in required fields")
            logger.info("  ✓ All timestamps are populated")
            logger.info("  ✓ All IDs are present")
            logger.info("=" * 70)
            return True
        else:
            logger.error("❌ VALIDATION FAILED")
            logger.error("=" * 70)
            if not movies_valid:
                logger.error("  ❌ Movies table has issues")
            if not snapshots_valid:
                logger.error("  ❌ Snapshots table has issues")
            logger.error("=" * 70)
            return False

    except Exception as e:
        logger.error(f"Error during validation: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
