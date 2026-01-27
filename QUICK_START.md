# Quick Start Guide

## 🚀 Get Running in 5 Minutes

**✨ Bonus:** Bind mounts enabled! Edit code locally, see changes instantly. No rebuild needed. ([Learn more](BIND_MOUNTS.md))

### Prerequisites
- Docker & Docker Compose installed
- TMDB API key ([get here](https://www.themoviedb.org/settings/api))
- GCP service account with BigQuery permissions

### Setup Steps

```bash
# 1. Clone and configure
git clone <repository-url>
cd content-analytics-pipeline
cp .env.example .env
# Edit .env: Add TMDB_API_KEY and GCP_PROJECT_ID

# 2. Add GCP credentials
cp ~/Downloads/your-service-account.json ./gcp-credentials.json

# 3. Create directories
mkdir -p ./logs ./dags ./plugins ./config

# 4. Start services
docker-compose up airflow-init
docker-compose up -d

# 5. Create BigQuery tables (IMPORTANT!)
docker-compose exec airflow-webserver python /opt/airflow/scripts/setup_bigquery.py

# 6. Load initial data (1000 movies)
docker-compose exec airflow-webserver python /opt/airflow/scripts/initial_load.py

# 7. Access Airflow
# Open http://localhost:8080
# Login: airflow / airflow
```

### Verify Everything Works

```bash
# Check data loaded
docker-compose exec airflow-webserver python -c "
from google.cloud import bigquery
client = bigquery.Client()
datasets = list(client.list_datasets())
print('Datasets:', [d.dataset_id for d in datasets])

query = 'SELECT COUNT(*) as count FROM \`YOUR_PROJECT.tmdb_analytics.movies\`'
result = list(client.query(query).result())
print(f'Movies loaded: {result[0].count}')
"
```

## ⚠️ Common Issues

### Issue: Tables not found
**Solution**: Run `docker-compose exec airflow-webserver python /opt/airflow/scripts/setup_bigquery.py`

### Issue: 403 Forbidden / Access Denied
**Solution**: Check GCP service account has BigQuery Data Editor and Job User roles

### Issue: Could not determine credentials
**Solution**:
1. Verify `gcp-credentials.json` exists in project root
2. Restart: `docker-compose down && docker-compose up -d`

### Issue: Environment variables not updating
**Solution**: After editing `.env`, restart containers:
```bash
docker-compose down
docker-compose up -d
```

## 📚 Next Steps

1. **View data in BigQuery**: Go to [BigQuery Console](https://console.cloud.google.com/bigquery)
2. **Enable DAGs**: In Airflow UI, toggle DAGs to "On"
3. **Create views**: Run queries from `queries/` folder in BigQuery
4. **Schedule runs**: DAGs run automatically (daily at 2 AM, weekly on Sundays)

## 🛠️ Useful Commands

```bash
# View logs
docker-compose logs -f airflow-scheduler

# Restart service
docker-compose restart airflow-webserver

# Stop everything
docker-compose down

# Complete reset (deletes all data)
docker-compose down --volumes --remove-orphans
```

## 📖 Full Documentation

- [README.md](README.md) - Complete documentation
- [DOCKER_SETUP.md](DOCKER_SETUP.md) - Detailed Docker guide
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Comprehensive troubleshooting

## 🆘 Getting Help

If stuck, check [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed solutions.
