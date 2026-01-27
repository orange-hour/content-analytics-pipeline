"""
BigQuery loader for TMDB movie data
"""
import logging
from typing import List, Dict, Optional
from datetime import datetime, timezone, date
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Handles loading TMDB data into BigQuery"""

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        location: str = "US"
    ):
        """
        Initialize BigQuery loader

        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            location: BigQuery dataset location
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = f"{project_id}.{dataset_id}"

    def _ensure_dataset_exists(self):
        """Create dataset if it doesn't exist"""
        try:
            self.client.get_dataset(self.dataset_ref)
            logger.info(f"Dataset {self.dataset_ref} already exists")
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = self.location
            dataset = self.client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_ref}")

    def _transform_movie_for_movies_table(self, movie: Dict) -> Dict:
        """
        Transform TMDB movie data for movies table schema

        Args:
            movie: Raw movie data from TMDB API

        Returns:
            Transformed movie data matching BigQuery schema
        """
        # Ensure required fields are present
        if not movie.get("id"):
            raise ValueError(f"Movie missing required 'id' field: {movie}")
        if not movie.get("title"):
            raise ValueError(f"Movie {movie.get('id')} missing required 'title' field")

        # Generate timestamp
        current_timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        return {
            "movie_id": movie.get("id"),
            "title": movie.get("title"),
            "original_title": movie.get("original_title"),
            "original_language": movie.get("original_language"),
            "overview": movie.get("overview"),
            "release_date": movie.get("release_date"),
            "runtime": movie.get("runtime"),
            "budget": movie.get("budget"),
            "revenue": movie.get("revenue"),
            "status": movie.get("status"),
            "tagline": movie.get("tagline"),
            "homepage": movie.get("homepage"),
            "imdb_id": movie.get("imdb_id"),
            "adult": movie.get("adult", False),
            "video": movie.get("video", False),
            "genres": movie.get("genres", []),
            "production_companies": movie.get("production_companies", []),
            "production_countries": movie.get("production_countries", []),
            "spoken_languages": movie.get("spoken_languages", []),
            "poster_path": movie.get("poster_path"),
            "backdrop_path": movie.get("backdrop_path"),
            "last_updated": current_timestamp 
        }

    def _transform_movie_for_snapshot_table(
        self,
        movie: Dict,
        snapshot_date: Optional[date] = None
    ) -> Dict:
        """
        Transform TMDB movie data for daily snapshots table

        Args:
            movie: Raw movie data from TMDB API
            snapshot_date: Date of snapshot (defaults to today)

        Returns:
            Transformed snapshot data matching BigQuery schema
        """
        # Ensure required fields are present
        if not movie.get("id"):
            raise ValueError(f"Movie missing required 'id' field: {movie}")
        if movie.get("popularity") is None:
            raise ValueError(f"Movie {movie.get('id')} missing required 'popularity' field")
        if movie.get("vote_count") is None:
            raise ValueError(f"Movie {movie.get('id')} missing required 'vote_count' field")
        if movie.get("vote_average") is None:
            raise ValueError(f"Movie {movie.get('id')} missing required 'vote_average' field")

        if snapshot_date is None:
            snapshot_date = date.today()

        # Generate timestamp - this ensures ingestion_timestamp is NEVER NULL
        current_timestamp = datetime.utcnow().isoformat()

        return {
            "movie_id": movie.get("id"),
            "snapshot_date": snapshot_date.isoformat(),
            "popularity": movie.get("popularity"),
            "vote_count": movie.get("vote_count"),
            "vote_average": movie.get("vote_average"),
            "ingestion_timestamp": current_timestamp  # ALWAYS set, never NULL
        }

    def load_movies(self, movies: List[Dict]) -> int:
        """
        Load movies into movies dimension table (upsert based on movie_id)
        """
        if not movies:
            logger.warning("No movies to load")
            return 0

        # Deduplicate movies based on ID, keeping the last one
        movies_dict = {m["id"]: m for m in movies if "id" in m}
        movies = list(movies_dict.values())
        
        table_id = f"{self.dataset_ref}.movies"
        transformed_movies = [self._transform_movie_for_movies_table(m) for m in movies]

        # Validate timestamps
        for i, movie in enumerate(transformed_movies):
            if not movie.get("last_updated"):
                raise ValueError(f"Record {i} missing last_updated timestamp")

        try:
            # 1. Fetch the existing table to get its schema
            try:
                table = self.client.get_table(table_id)
                table_exists = True
                # Use the ACTUAL schema from the table to prevent mismatches
                current_schema = table.schema 
            except NotFound:
                table_exists = False
                logger.error(f"Table {table_id} not found. Run setup_bigquery.py first.")
                raise RuntimeError(f"Table {table_id} does not exist.")

            # 2. Handle Manual Upsert (Delete existing)
            if table_exists:
                movie_ids = [m["movie_id"] for m in transformed_movies]
                delete_query = f"DELETE FROM `{table_id}` WHERE movie_id IN UNNEST(@movie_ids)"
                
                job_config_query = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter("movie_ids", "INT64", movie_ids)
                    ]
                )
                delete_job = self.client.query(delete_query, job_config=job_config_query)
                delete_job.result()
                logger.info(f"Deleted {delete_job.num_dml_affected_rows} existing records")

            # 3. Insert new records with EXPLICIT SCHEMA
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=current_schema, # <--- THIS IS THE KEY FIX
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )

            load_job = self.client.load_table_from_json(
                transformed_movies,
                table_id,
                job_config=job_config
            )
            load_job.result()

            logger.info(f"Loaded {len(transformed_movies)} movies to {table_id}")
            return len(transformed_movies)

        except Exception as e:
            logger.error(f"Error loading movies: {e}")
            raise

    def load_daily_snapshots(
        self,
        movies: List[Dict],
        snapshot_date: Optional[date] = None
    ) -> int:
        """
        Load daily snapshots of movie metrics
        
        Args:
            movies: List of movie data from TMDB API
            snapshot_date: Date of snapshot (defaults to today)

        Returns:
            Number of rows loaded
        """
        if not movies:
            logger.warning("No snapshots to load")
            return 0

        # Deduplicate movies based on ID, keeping the last one
        movies_dict = {m["id"]: m for m in movies if "id" in m}
        movies = list(movies_dict.values())

        if snapshot_date is None:
            snapshot_date = date.today()

        table_id = f"{self.dataset_ref}.movie_daily_snapshots"
        transformed_snapshots = [
            self._transform_movie_for_snapshot_table(m, snapshot_date)
            for m in movies
        ]

        try:
            # 1. Fetch table metadata to get the current schema
            try:
                table = self.client.get_table(table_id)
                table_exists = True
                current_schema = table.schema  # <--- Essential to fix the 400 error
            except NotFound:
                table_exists = False
                logger.error(f"Table {table_id} does not exist. Run setup_bigquery.py.")
                raise RuntimeError(f"Table {table_id} must be created before loading.")

            # 2. Delete existing snapshots for the same date/IDs (Idempotency)
            if table_exists:
                movie_ids = [s["movie_id"] for s in transformed_snapshots]
                delete_query = f"""
                DELETE FROM `{table_id}`
                WHERE snapshot_date = @snapshot_date
                AND movie_id IN UNNEST(@movie_ids)
                """
                job_config_query = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("snapshot_date", "DATE", snapshot_date),
                        bigquery.ArrayQueryParameter("movie_ids", "INT64", movie_ids)
                    ]
                )
                delete_job = self.client.query(delete_query, job_config=job_config_query)
                delete_job.result()
                logger.info(f"Deleted {delete_job.num_dml_affected_rows} existing snapshots for {snapshot_date}")

            # 3. Load data using the table's actual schema
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=current_schema, 
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ],
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )

            load_job = self.client.load_table_from_json(
                transformed_snapshots,
                table_id,
                job_config=job_config
            )
            load_job.result()

            logger.info(f"Loaded {len(transformed_snapshots)} snapshots to {table_id}")
            return len(transformed_snapshots)

        except Exception as e:
            logger.error(f"Error loading snapshots: {e}")
            raise

    def compute_derived_metrics(self) -> None:
        """
        Compute derived metrics (day-over-day changes, moving averages, etc.)
        Updates movie_daily_snapshots with calculated fields
        """
        query = f"""
        -- Update derived metrics in snapshots table
        MERGE `{self.dataset_ref}.movie_daily_snapshots` T
        USING (
          SELECT
            movie_id,
            snapshot_date,
            popularity,
            vote_count,
            -- 1-day changes
            popularity - LAG(popularity, 1) OVER (PARTITION BY movie_id ORDER BY snapshot_date) AS popularity_change_1d,
            vote_count - LAG(vote_count, 1) OVER (PARTITION BY movie_id ORDER BY snapshot_date) AS vote_count_change_1d,
            SAFE_DIVIDE(
              popularity - LAG(popularity, 1) OVER (PARTITION BY movie_id ORDER BY snapshot_date),
              LAG(popularity, 1) OVER (PARTITION BY movie_id ORDER BY snapshot_date)
            ) * 100 AS popularity_pct_change_1d,
            SAFE_DIVIDE(
              vote_count - LAG(vote_count, 1) OVER (PARTITION BY movie_id ORDER BY snapshot_date),
              LAG(vote_count, 1) OVER (PARTITION BY movie_id ORDER BY snapshot_date)
            ) * 100 AS vote_count_pct_change_1d,
            -- 7-day changes
            popularity - LAG(popularity, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date) AS popularity_change_7d,
            vote_count - LAG(vote_count, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date) AS vote_count_change_7d,
            SAFE_DIVIDE(
              popularity - LAG(popularity, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date),
              LAG(popularity, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date)
            ) * 100 AS popularity_pct_change_7d,
            SAFE_DIVIDE(
              vote_count - LAG(vote_count, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date),
              LAG(vote_count, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date)
            ) * 100 AS vote_count_pct_change_7d
          FROM `{self.dataset_ref}.movie_daily_snapshots`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY movie_id, snapshot_date) = 1
        ) S
        ON T.movie_id = S.movie_id AND T.snapshot_date = S.snapshot_date
        WHEN MATCHED THEN UPDATE SET
          T.popularity_change_1d = S.popularity_change_1d,
          T.popularity_change_7d = S.popularity_change_7d,
          T.popularity_pct_change_1d = S.popularity_pct_change_1d,
          T.popularity_pct_change_7d = S.popularity_pct_change_7d,
          T.vote_count_change_1d = S.vote_count_change_1d,
          T.vote_count_change_7d = S.vote_count_change_7d,
          T.vote_count_pct_change_1d = S.vote_count_pct_change_1d,
          T.vote_count_pct_change_7d = S.vote_count_pct_change_7d
        """

        try:
            query_job = self.client.query(query)
            query_job.result()
            logger.info(f"Updated derived metrics, {query_job.num_dml_affected_rows} rows affected")
        except Exception as e:
            logger.error(f"Error computing derived metrics: {e}")
            raise

    def compute_aggregated_metrics(self, calculation_date: Optional[date] = None) -> None:
        """
        Compute aggregated metrics and populate movie_metrics_aggregated table

        Args:
            calculation_date: Date for which to calculate metrics (defaults to today)
        """
        if calculation_date is None:
            calculation_date = date.today()

        query = f"""
        INSERT INTO `{self.dataset_ref}.movie_metrics_aggregated`
        (movie_id, calculation_date, popularity_7d_ma, popularity_wow_change,
         popularity_wow_pct_change, vote_velocity_7d, vote_acceleration_7d,
         viral_coefficient, trend_category)
        SELECT
          movie_id,
          @calculation_date AS calculation_date,
          -- 7-day moving average of popularity
          AVG(popularity) OVER (
            PARTITION BY movie_id
            ORDER BY snapshot_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          ) AS popularity_7d_ma,
          -- Week-over-week change (comparing to 7 days ago)
          popularity - LAG(popularity, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date) AS popularity_wow_change,
          SAFE_DIVIDE(
            popularity - LAG(popularity, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date),
            LAG(popularity, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date)
          ) * 100 AS popularity_wow_pct_change,
          -- Vote velocity: average daily vote increase over 7 days
          SAFE_DIVIDE(
            vote_count - LAG(vote_count, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date),
            7
          ) AS vote_velocity_7d,
          -- Vote acceleration: change in vote velocity
          SAFE_DIVIDE(
            vote_count - LAG(vote_count, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date),
            7
          ) - SAFE_DIVIDE(
            LAG(vote_count, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date) -
            LAG(vote_count, 14) OVER (PARTITION BY movie_id ORDER BY snapshot_date),
            7
          ) AS vote_acceleration_7d,
          -- Viral coefficient: normalized score based on vote velocity and popularity change
          LEAST(100, GREATEST(0,
            (SAFE_DIVIDE(
              vote_count - LAG(vote_count, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date),
              GREATEST(LAG(vote_count, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date), 1)
            ) * 100) * 0.6 +
            (SAFE_DIVIDE(
              popularity - LAG(popularity, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date),
              GREATEST(LAG(popularity, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date), 1)
            ) * 100) * 0.4
          )) AS viral_coefficient,
          -- Trend classification
          CASE
            WHEN SAFE_DIVIDE(vote_count - LAG(vote_count, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date), 7) > 50
              AND (popularity - LAG(popularity, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date)) > 10
              THEN 'Viral'
            WHEN (popularity - LAG(popularity, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date)) > 5
              THEN 'Rising'
            WHEN (popularity - LAG(popularity, 7) OVER (PARTITION BY movie_id ORDER BY snapshot_date)) < -5
              THEN 'Declining'
            ELSE 'Stable'
          END AS trend_category
        FROM `{self.dataset_ref}.movie_daily_snapshots`
        WHERE snapshot_date = @calculation_date
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("calculation_date", "DATE", calculation_date)
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()
            logger.info(f"Computed aggregated metrics for {calculation_date}, {query_job.num_dml_affected_rows} rows inserted")
        except Exception as e:
            logger.error(f"Error computing aggregated metrics: {e}")
            raise

