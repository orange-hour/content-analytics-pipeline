# Data Validation Guide

## Ensuring No NULL Values in Required Fields

### Overview

The pipeline has been hardened to ensure that required timestamp fields (`last_updated`, `ingestion_timestamp`) are **always populated** and never NULL.

## What Was Fixed

### 1. **Movies Table - `last_updated` Field**

**Location**: [scripts/bigquery_loader.py:64-65](scripts/bigquery_loader.py)

```python
# Generate timestamp - this ensures last_updated is NEVER NULL
current_timestamp = datetime.utcnow().isoformat()
```

**Guarantees**:
- ✅ `last_updated` is set to `datetime.utcnow().isoformat()` for every record
- ✅ Generated **before** the dictionary is created, not as a default parameter
- ✅ Cannot be NULL or missing
- ✅ Validation check added to verify all records have this field

### 2. **Snapshots Table - `ingestion_timestamp` Field**

**Location**: [scripts/bigquery_loader.py:120-121](scripts/bigquery_loader.py)

```python
# Generate timestamp - this ensures ingestion_timestamp is NEVER NULL
current_timestamp = datetime.utcnow().isoformat()
```

**Guarantees**:
- ✅ `ingestion_timestamp` is set for every snapshot
- ✅ Always contains a valid UTC timestamp
- ✅ Validation checks added for all required fields

### 3. **Pre-Load Validation**

**Location**: [scripts/bigquery_loader.py:149-154](scripts/bigquery_loader.py)

```python
# Validate that last_updated is set for all records
for i, movie in enumerate(transformed_movies):
    if not movie.get("last_updated"):
        raise ValueError(f"Record {i} missing last_updated timestamp: {movie}")
```

**Guarantees**:
- ✅ Every record is validated before loading to BigQuery
- ✅ Pipeline will fail fast if any record is missing required fields
- ✅ Clear error messages indicating which record has issues

### 4. **Field Validation in Transform Functions**

**Added Checks**:
- Movie ID must be present
- Title must be present
- Popularity, vote_count, vote_average must be present

**Location**: [scripts/bigquery_loader.py:58-62](scripts/bigquery_loader.py)

## Validation Tools

### Automated Validation Script

Run the validation script to check data integrity:

```bash
# Using Docker
docker-compose exec airflow-webserver python /opt/airflow/scripts/validate_data.py

# Expected output:
# ✓ No NULL values in last_updated
# ✓ No NULL values in movie_id
# ✓ No NULL values in title
# ✓ Total movies: 1000
# ✓ No NULL values in ingestion_timestamp
# ✓ No NULL values in movie_id
# ✓ No NULL values in snapshot_date
# ✓ Total snapshots: 1000
# ✓ ALL VALIDATIONS PASSED
```

### Manual Validation Queries

#### Check for NULL `last_updated` in movies table:

```sql
SELECT COUNT(*) as null_count
FROM `movie-analytics-483821.tmdb_analytics.movies`
WHERE last_updated IS NULL;

-- Should return: 0
```

#### Check for NULL `ingestion_timestamp` in snapshots:

```sql
SELECT COUNT(*) as null_count
FROM `movie-analytics-483821.tmdb_analytics.movie_daily_snapshots`
WHERE ingestion_timestamp IS NULL;

-- Should return: 0
```

#### View sample records to verify timestamps:

```sql
SELECT
  movie_id,
  title,
  last_updated,
  DATE(last_updated) as update_date,
  TIME(last_updated) as update_time
FROM `movie-analytics-483821.tmdb_analytics.movies`
ORDER BY last_updated DESC
LIMIT 10;
```

## How to Use

### Step 1: Reset BigQuery (if needed)

If you have existing tables with schema issues:

```bash
docker-compose exec airflow-webserver python /opt/airflow/scripts/reset_bigquery.py
```

### Step 2: Load Data

```bash
docker-compose exec airflow-webserver python /opt/airflow/scripts/initial_load.py
```

You should see this log message during the load:
```
INFO:bigquery_loader:Validated 1000 movies - all have last_updated timestamp
```

### Step 3: Validate Data

```bash
docker-compose exec airflow-webserver python /opt/airflow/scripts/validate_data.py
```

### Step 4: Verify in BigQuery Console

1. Go to: https://console.cloud.google.com/bigquery?project=movie-analytics-483821
2. Navigate to `tmdb_analytics.movies`
3. Click "Preview" tab
4. Verify `last_updated` column has values for all rows

## Error Handling

### If Validation Fails

If the validation script reports NULL values:

```bash
# 1. Check the logs to identify which records failed
docker-compose logs airflow-webserver

# 2. Re-run with fresh data
docker-compose exec airflow-webserver python /opt/airflow/scripts/reset_bigquery.py
docker-compose exec airflow-webserver python /opt/airflow/scripts/initial_load.py

# 3. Validate again
docker-compose exec airflow-webserver python /opt/airflow/scripts/validate_data.py
```

### Expected Error Messages

If a record is missing required fields, you'll see:

```
ValueError: Movie missing required 'id' field: {...}
ValueError: Movie 123 missing required 'title' field
ValueError: Record 42 missing last_updated timestamp: {...}
```

These errors are **intentional** and prevent bad data from being loaded.

## Guarantees

With the updated code, the following is **guaranteed**:

1. ✅ **last_updated** will NEVER be NULL in the `movies` table
2. ✅ **ingestion_timestamp** will NEVER be NULL in the `movie_daily_snapshots` table
3. ✅ All records are validated before loading to BigQuery
4. ✅ Pipeline will fail fast with clear error messages if data is invalid
5. ✅ Timestamps use UTC timezone (`datetime.utcnow()`)
6. ✅ Timestamps are in ISO 8601 format (e.g., `2025-01-11T14:30:45.123456`)

## Files Modified

- ✅ [scripts/bigquery_loader.py](scripts/bigquery_loader.py) - Added validation and guaranteed timestamp generation
- ✅ [scripts/validate_data.py](scripts/validate_data.py) - New validation script
- ✅ [DATA_VALIDATION.md](DATA_VALIDATION.md) - This documentation

## Quick Reference

```bash
# Complete workflow with validation
docker-compose exec airflow-webserver python /opt/airflow/scripts/reset_bigquery.py
docker-compose exec airflow-webserver python /opt/airflow/scripts/initial_load.py
docker-compose exec airflow-webserver python /opt/airflow/scripts/validate_data.py

# Expected result: ✓ ALL VALIDATIONS PASSED
```
