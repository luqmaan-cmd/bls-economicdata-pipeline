# BLS Economic Data Pipeline

Automated pipeline for ingesting U.S. Bureau of Labor Statistics (BLS) bulk data into a PostgreSQL database, running on Google Cloud Run.

## Architecture

- **Pipeline**: `bls_unified_pipeline.py` — single Python script handling download, transformation, and load
- **Execution**: Cloud Run Jobs (serverless, scheduled via Cloud Scheduler)
- **Storage**: Raw files in GCS (`gs://bls-econ-data`), processed data in Cloud SQL PostgreSQL
- **Proxy**: Oxylabs residential proxy required to bypass BLS/Akamai bot protection (`download.bls.gov` returns 403 without it)

## Datasets

| Key | Dataset | Table | Schema |
|-----|---------|-------|--------|
| cpi | Consumer Price Index | cu_data | cpi_schema.sql |
| ppi | Producer Price Index | wp_data | ppi_schema.sql |
| employment | Current Employment Statistics (National) | ce_data | ce_schema.sql |
| unemployment | Local Area Unemployment Statistics | la_data | la_schema.sql |
| jolts | Job Openings and Labor Turnover | jt_data | jt_schema.sql |
| state_employment | Current Employment Statistics (State) | sa_data | sa_schema.sql |
| occupational | Occupational Employment & Wages | oe_data | oe_schema.sql |
| compensation | Employment Cost Index | ci_data | ci_schema.sql |
| productivity | Major Sector Productivity | mp_data | mp_schema.sql |
| state_metro | Current Employment Statistics (State/Metro) | sm_data | sm_schema.sql |

All datasets use denormalized (wide) tables with full refresh (truncate + reload) on each run.

## Cloud Run Jobs

| Job Name | Dataset | Timeout | Memory | Schedule |
|----------|---------|---------|--------|----------|
| bls-pipeline-cpi | cpi | 20m | 512Mi | Daily 06:00 UTC |
| bls-pipeline-ppi | ppi | 20m | 512Mi | Daily 06:15 UTC |
| bls-pipeline-employment | employment | 20m | 512Mi | Daily 06:30 UTC |
| bls-pipeline-unemployment | unemployment | 20m | 512Mi | Daily 06:45 UTC |
| bls-pipeline-jolts | jolts | 20m | 512Mi | Daily 07:00 UTC |
| bls-pipeline-state-employment | state_employment | 20m | 512Mi | Daily 07:15 UTC |
| bls-pipeline-occupational | occupational | 20m | 512Mi | Daily 07:30 UTC |
| bls-pipeline-compensation | compensation | 20m | 512Mi | Daily 07:45 UTC |
| bls-pipeline-productivity | productivity | 20m | 512Mi | Daily 08:00 UTC |
| bls-pipeline-state-metro | state_metro | 45m | 3.5Gi | Daily 08:15 UTC |
| bls-pipeline-all | All 10 datasets | 60m | 512Mi | Manual only |

## Data Integrity Safeguards

### 1. ETag Retry Logic
`get_etag_and_last_modified()` retries 3 times with 2-second backoff when the proxy drops HEAD requests. Previously, a single failure returned `None` and triggered unnecessary full reloads.

### 2. Smart `should_reload()`
When the ETag is unavailable (proxy failure), the pipeline checks the load log instead of forcing a reload. If the previous load succeeded, it skips the reload. Only forces a reload if there's no previous entry or the previous load failed.

### 3. Row Count Validation
`validate_row_count()` compares the new row count against the previous successful load. If the new count is less than 50% of the previous, the load is marked as `failed` (truncated download detected). A warning is logged if below 90%. Applied to all 10 denormalized dataset loaders.

### 4. Malformed Row Handling
Rows with fewer columns than expected are skipped during COPY transforms instead of crashing the pipeline.

## Infrastructure

| Component | Value |
|-----------|-------|
| GCP Project | testgke-412710 |
| Region | europe-west2 |
| Artifact Registry | europe-west2-docker.pkg.dev/testgke-412710/bls-pipeline/bls-pipeline |
| GCS Bucket | bls-econ-data |
| Database | PostgreSQL on Compute Engine (35.214.22.65:5432) |
| Service Account | 832081557693-compute@developer.gserviceaccount.com |
| VPC Connector | bls-connector |
| Secrets | DB_HOST, DB_NAME, DB_PASSWORD, DB_USER, PROXY_USER, PROXY_PASS |

## Local Development

```bash
# Set environment variables
export DB_HOST=...
export DB_USER=...
export DB_PASSWORD=...
export DB_NAME=asset_identification_scheme
export PROXY_USER=...
export PROXY_PASS=...

# Run a single dataset
python bls_unified_pipeline.py --datasets=cpi

# Force re-download
python bls_unified_pipeline.py --datasets=cpi --force

# Run all datasets
python bls_unified_pipeline.py --datasets=all
```

## Source

- BLS Bulk Download: https://download.bls.gov/pub/time.series/
- GitHub: https://github.com/luqmaan-cmd/bls-economicdata-pipeline
