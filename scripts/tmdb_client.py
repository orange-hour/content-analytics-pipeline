"""
TMDB API Client for fetching movie data
"""
import os
import time
import requests
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TMDBClient:
    """Client for interacting with TMDB API"""

    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize TMDB client

        Args:
            api_key: TMDB API key (defaults to TMDB_API_KEY env var)
        """
        self.api_key = api_key or os.environ.get("TMDB_API_KEY")
        if not self.api_key:
            raise ValueError("TMDB API key is required. Set TMDB_API_KEY environment variable.")

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json;charset=utf-8"
        })

    def _make_request(self, endpoint: str, params: Optional[Dict] = None, retries: int = 3) -> Dict:
        """
        Make HTTP request to TMDB API with retry logic

        Args:
            endpoint: API endpoint path
            params: Query parameters
            retries: Number of retry attempts

        Returns:
            JSON response as dictionary
        """
        url = f"{self.BASE_URL}/{endpoint}"

        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise

    def discover_movies(
        self,
        page: int = 1,
        sort_by: str = "popularity.desc",
        include_adult: bool = False,
        include_video: bool = False
    ) -> Dict:
        """
        Discover movies using TMDB discover endpoint

        Args:
            page: Page number (1-indexed)
            sort_by: Sort criteria (e.g., 'popularity.desc', 'vote_average.desc')
            include_adult: Include adult content
            include_video: Include video content

        Returns:
            Response containing movies list and pagination info
        """
        params = {
            "page": page,
            "sort_by": sort_by,
            "include_adult": include_adult,
            "include_video": include_video
        }

        logger.info(f"Fetching discover movies page {page}")
        return self._make_request("discover/movie", params=params)

    def get_movie_changes(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page: int = 1
    ) -> Dict:
        """
        Get list of movie IDs that have been changed/updated

        Args:
            start_date: Start date in YYYY-MM-DD format (defaults to yesterday)
            end_date: End date in YYYY-MM-DD format (defaults to today)
            page: Page number

        Returns:
            Response containing list of changed movie IDs
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "page": page
        }

        logger.info(f"Fetching movie changes from {start_date} to {end_date}, page {page}")
        return self._make_request("movie/changes", params=params)

    def get_movie_details(self, movie_id: int) -> Dict:
        """
        Get detailed information about a specific movie

        Args:
            movie_id: TMDB movie ID

        Returns:
            Movie details including popularity, votes, revenue, etc.
        """
        logger.debug(f"Fetching details for movie ID {movie_id}")
        return self._make_request(f"movie/{movie_id}")

    def get_top_movies(self, limit: int = 1000) -> List[Dict]:
        """
        Fetch top N most popular movies using discover endpoint

        Args:
            limit: Number of movies to fetch (max practical limit depends on API)

        Returns:
            List of movie data dictionaries
        """
        movies = []
        page = 1
        movies_per_page = 20  # TMDB returns 20 results per page
        max_pages = (limit + movies_per_page - 1) // movies_per_page  # Ceiling division

        logger.info(f"Fetching top {limit} movies (approximately {max_pages} pages)")

        while len(movies) < limit:
            try:
                response = self.discover_movies(page=page, sort_by="popularity.desc")
                results = response.get("results", [])

                if not results:
                    logger.info("No more results available")
                    break

                movies.extend(results)
                logger.info(f"Fetched page {page}/{max_pages}, total movies: {len(movies)}")

                # Check if we've reached the last page
                if page >= response.get("total_pages", 0):
                    break

                page += 1
                time.sleep(0.25)  # Rate limiting: 4 requests per second max

            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break

        return movies[:limit]

    def get_changed_movie_ids(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[int]:
        """
        Get all movie IDs that changed in the specified date range

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            List of movie IDs
        """
        movie_ids = []
        page = 1

        while True:
            try:
                response = self.get_movie_changes(start_date, end_date, page)
                results = response.get("results", [])

                if not results:
                    break

                movie_ids.extend([item["id"] for item in results if "id" in item])
                logger.info(f"Fetched changes page {page}, total movie IDs: {len(movie_ids)}")

                # Check if we've reached the last page
                if page >= response.get("total_pages", 1):
                    break

                page += 1
                time.sleep(0.25)  # Rate limiting

            except Exception as e:
                logger.error(f"Error fetching changes page {page}: {e}")
                break

        return movie_ids

    def get_movies_details_batch(self, movie_ids: List[int]) -> List[Dict]:
        """
        Fetch details for multiple movies with rate limiting

        Args:
            movie_ids: List of movie IDs to fetch

        Returns:
            List of movie details dictionaries
        """
        movies = []
        total = len(movie_ids)

        logger.info(f"Fetching details for {total} movies")

        for idx, movie_id in enumerate(movie_ids, 1):
            try:
                movie_data = self.get_movie_details(movie_id)
                movies.append(movie_data)

                if idx % 100 == 0:
                    logger.info(f"Progress: {idx}/{total} movies fetched")

                time.sleep(0.25)  # Rate limiting: 4 requests per second

            except Exception as e:
                logger.error(f"Error fetching movie {movie_id}: {e}")
                continue

        logger.info(f"Successfully fetched {len(movies)}/{total} movies")
        return movies
