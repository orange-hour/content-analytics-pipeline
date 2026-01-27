"""
Setup BigQuery dataset and tables for TMDB Content Analytics Pipeline
"""
import os
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_dataset(client: bigquery.Client, project_id: str, dataset_id: str, location: str = "US"):
    """Create BigQuery dataset if it doesn't exist"""
    dataset_ref = f"{project_id}.{dataset_id}"

    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_ref} already exists")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset = client.create_dataset(dataset)
        logger.info(f"Created dataset {dataset_ref}")


def create_movies_table(client: bigquery.Client, dataset_ref: str):
    """Create movies dimension table"""
    table_id = f"{dataset_ref}.movies"

    schema = [
        bigquery.SchemaField("movie_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("original_title", "STRING"),
        bigquery.SchemaField("original_language", "STRING"),
        bigquery.SchemaField("overview", "STRING"),
        bigquery.SchemaField("release_date", "DATE"),
        bigquery.SchemaField("runtime", "INTEGER"),
        bigquery.SchemaField("budget", "INTEGER"),
        bigquery.SchemaField("revenue", "INTEGER"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("tagline", "STRING"),
        bigquery.SchemaField("homepage", "STRING"),
        bigquery.SchemaField("imdb_id", "STRING"),
        bigquery.SchemaField("adult", "BOOLEAN"),
        bigquery.SchemaField("video", "BOOLEAN"),
        bigquery.SchemaField("genres", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
        ]),
        bigquery.SchemaField("production_companies", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("logo_path", "STRING"),
            bigquery.SchemaField("origin_country", "STRING"),
        ]),
        bigquery.SchemaField("production_countries", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("iso_3166_1", "STRING"),
            bigquery.SchemaField("name", "STRING"),
        ]),
        bigquery.SchemaField("spoken_languages", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("iso_639_1", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("english_name", "STRING"),
        ]),
        bigquery.SchemaField("poster_path", "STRING"),
        bigquery.SchemaField("backdrop_path", "STRING"),
        bigquery.SchemaField("last_updated", "TIMESTAMP"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="last_updated"
    )

    try:
        client.get_table(table_id)
        logger.info(f"Table {table_id} already exists")
    except NotFound:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")


def create_snapshots_table(client: bigquery.Client, dataset_ref: str):
    """Create movie daily snapshots table"""
    table_id = f"{dataset_ref}.movie_daily_snapshots"

    schema = [
        bigquery.SchemaField("movie_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("popularity", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("vote_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("vote_average", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("popularity_change_1d", "FLOAT"),
        bigquery.SchemaField("popularity_change_7d", "FLOAT"),
        bigquery.SchemaField("popularity_pct_change_1d", "FLOAT"),
        bigquery.SchemaField("popularity_pct_change_7d", "FLOAT"),
        bigquery.SchemaField("vote_count_change_1d", "INTEGER"),
        bigquery.SchemaField("vote_count_change_7d", "INTEGER"),
        bigquery.SchemaField("vote_count_pct_change_1d", "FLOAT"),
        bigquery.SchemaField("vote_count_pct_change_7d", "FLOAT"),
        bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="snapshot_date"
    )
    table.clustering_fields = ["movie_id"]

    try:
        client.get_table(table_id)
        logger.info(f"Table {table_id} already exists")
    except NotFound:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")


def create_aggregated_metrics_table(client: bigquery.Client, dataset_ref: str):
    """Create aggregated metrics table"""
    table_id = f"{dataset_ref}.movie_metrics_aggregated"

    schema = [
        bigquery.SchemaField("movie_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("calculation_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("popularity_7d_ma", "FLOAT"),
        bigquery.SchemaField("popularity_wow_change", "FLOAT"),
        bigquery.SchemaField("popularity_wow_pct_change", "FLOAT"),
        bigquery.SchemaField("vote_velocity_7d", "FLOAT"),
        bigquery.SchemaField("vote_acceleration_7d", "FLOAT"),
        bigquery.SchemaField("viral_coefficient", "FLOAT"),
        bigquery.SchemaField("trend_category", "STRING"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="calculation_date"
    )
    table.clustering_fields = ["movie_id", "trend_category"]

    try:
        client.get_table(table_id)
        logger.info(f"Table {table_id} already exists")
    except NotFound:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")


def main():
    """Main function to setup BigQuery"""

    # Configuration
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "tmdb_analytics")
    BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

    if not GCP_PROJECT_ID:
        raise ValueError("GCP_PROJECT_ID environment variable is required")

    logger.info(f"Setting up BigQuery for project: {GCP_PROJECT_ID}")
    logger.info(f"Dataset: {BQ_DATASET_ID}")
    logger.info(f"Location: {BQ_LOCATION}")

    # Initialize BigQuery client
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dataset_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}"

    try:
        # Step 1: Create dataset
        logger.info("Step 1: Creating dataset...")
        create_dataset(client, GCP_PROJECT_ID, BQ_DATASET_ID, BQ_LOCATION)

        # Step 2: Create movies table
        logger.info("Step 2: Creating movies table...")
        create_movies_table(client, dataset_ref)

        # Step 3: Create snapshots table
        logger.info("Step 3: Creating snapshots table...")
        create_snapshots_table(client, dataset_ref)

        # Step 4: Create aggregated metrics table
        logger.info("Step 4: Creating aggregated metrics table...")
        create_aggregated_metrics_table(client, dataset_ref)

        logger.info("=" * 50)
        logger.info("BigQuery setup completed successfully!")
        logger.info("=" * 50)
        logger.info(f"Dataset: {dataset_ref}")
        logger.info("Tables created:")
        logger.info(f"  - {dataset_ref}.movies")
        logger.info(f"  - {dataset_ref}.movie_daily_snapshots")
        logger.info(f"  - {dataset_ref}.movie_metrics_aggregated")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error during BigQuery setup: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
