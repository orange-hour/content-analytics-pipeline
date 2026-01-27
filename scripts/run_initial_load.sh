#!/bin/bash
# Script to run initial load from Docker container

set -e

echo "=========================================="
echo "TMDB Content Analytics - Initial Load"
echo "=========================================="
echo ""

# Check required environment variables
if [ -z "$TMDB_API_KEY" ]; then
    echo "ERROR: TMDB_API_KEY environment variable is not set"
    exit 1
fi

if [ -z "$GCP_PROJECT_ID" ]; then
    echo "ERROR: GCP_PROJECT_ID environment variable is not set"
    exit 1
fi

echo "Configuration:"
echo "  TMDB API Key: ${TMDB_API_KEY:0:10}..."
echo "  GCP Project: $GCP_PROJECT_ID"
echo "  BigQuery Dataset: ${BQ_DATASET_ID:-tmdb_analytics}"
echo "  Number of Movies: ${NUM_MOVIES:-1000}"
echo ""

# Run the initial load script
echo "Starting initial load..."
python /opt/airflow/scripts/initial_load.py

echo ""
echo "=========================================="
echo "Initial load completed successfully!"
echo "=========================================="
