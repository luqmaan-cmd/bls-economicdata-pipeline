# BLS Data Pipeline - Idempotent Load Plan

## Overview

Automated, scheduled pipeline to load BLS economic data with idempotency using ETag-based change detection.

---

## Data Sources

### QCEW (Quarterly Census of Employment and Wages)
- **URL Pattern**: `https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip`
- **Granularity**: One ZIP file per year (1990-present)
- **Table**: `bls_qcew_raw`
- **Partition Key**: `year` column

### Other BLS Tables (Employment, Unemployment, CPI, JOLTS, etc.)
- **Source**: Single bulk download file
- **Tables**: `bls_employment`, `bls_unemployment`, `bls_cpi`, `bls_jolts`, `bls_compensation`, `bls_ppi`, `bls_state_employment`, `bls_occupational`, `bls_import_export`, `bls_mass_layoff`, `bls_state_metro`
- **Granularity**: One `ETag` for entire bulk file

---

## Database Schema

### Load Tracking Table

```sql
CREATE TABLE bls_load_log (
    dataset VARCHAR(50) NOT NULL,
    data_key VARCHAR(100) NOT NULL,
    etag VARCHAR(100),
    last_modified TIMESTAMP,
    source_url VARCHAR(500),
    rows_loaded INT,
    load_timestamp TIMESTAMP DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'success',
    error_message TEXT,
    PRIMARY KEY (dataset, data_key)
);
```

### Unique Constraints (for UPSERT support)

```sql
-- QCEW
ALTER TABLE bls_qcew_raw ADD CONSTRAINT bls_qcew_raw_uniq 
    UNIQUE (year, quarter, area_fips, industry_code, ownership_code);

-- Time series tables (employment, unemployment, cpi, etc.)
ALTER TABLE bls_employment ADD CONSTRAINT bls_employment_uniq 
    UNIQUE (series_id, year, period);
ALTER TABLE bls_unemployment ADD CONSTRAINT bls_unemployment_uniq 
    UNIQUE (series_id, year, period);
ALTER TABLE bls_cpi ADD CONSTRAINT bls_cpi_uniq 
    UNIQUE (series_id, year, period);
ALTER TABLE bls_jolts ADD CONSTRAINT bls_jolts_uniq 
    UNIQUE (series_id, year, period);
ALTER TABLE bls_compensation ADD CONSTRAINT bls_compensation_uniq 
    UNIQUE (series_id, year, period);
ALTER TABLE bls_ppi ADD CONSTRAINT bls_ppi_uniq 
    UNIQUE (series_id, year, period);
ALTER TABLE bls_state_employment ADD CONSTRAINT bls_state_employment_uniq 
    UNIQUE (series_id, year, period);
ALTER TABLE bls_import_export ADD CONSTRAINT bls_import_export_uniq 
    UNIQUE (series_id, year, period);
ALTER TABLE bls_mass_layoff ADD CONSTRAINT bls_mass_layoff_uniq 
    UNIQUE (series_id, year, period);
ALTER TABLE bls_state_metro ADD CONSTRAINT bls_state_metro_uniq 
    UNIQUE (series_id, year, period);
```

---

## Pipeline Flow

### QCEW Pipeline

```
For each year (1990 to current_year - 1):
    1. HEAD request to BLS URL → get ETag, Last-Modified
    2. Query bls_load_log for (dataset='qcew', data_key=year)
    3. Compare ETags:
       - If match → SKIP (no change)
       - If no match or no entry → PROCEED
    4. Download ZIP from BLS
    5. Upload to GCS (backup)
    6. Extract CSV
    7. DELETE FROM bls_qcew_raw WHERE year = {year}
    8. Load new data with INSERT
    9. INSERT/UPDATE bls_load_log with new ETag
```

### Bulk BLS Pipeline (Employment, CPI, etc.)

```
1. HEAD request to bulk download URL → get ETag, Last-Modified
2. Query bls_load_log for (dataset='bls_bulk', data_key='all')
3. Compare ETags:
   - If match → SKIP (no change)
   - If no match or no entry → PROCEED
4. Download bulk file
5. Upload to GCS (backup)
6. For each table:
   a. TRUNCATE table (or use UPSERT)
   b. Load new data
7. INSERT/UPDATE bls_load_log with new ETag
```

---

## ETag Change Detection

### Why ETag?
- Content-based hash - detects actual file changes
- More reliable than `Last-Modified` alone
- No need to download file to check for changes

### Example ETag Values
```
QCEW 2024: "122b489e-63dcfcdf704e5"
QCEW 2023: "118a3f2c-52cbede85f3d2"
Bulk file: "9f8e7d6c-5b4a3c2d1e0f"
```

---

## Scheduling

### Recommended Schedule
- **Frequency**: Every 2 weeks
- **Reason**: BLS typically updates data monthly/quarterly
- **Tool**: Cloud Scheduler → Cloud Run Jobs

### Cron Expression
```
0 6 1,15 * *  # Run at 6 AM on 1st and 15th of each month
```

---

## Error Handling

1. **Network errors**: Retry with exponential backoff (3 attempts)
2. **Parse errors**: Log and continue to next year/dataset
3. **DB errors**: Rollback transaction, log error in `bls_load_log.status = 'failed'`
4. **Partial failures**: Each year/dataset is independent - one failure doesn't block others

---

## Monitoring & Alerting

1. **Cloud Logging**: Automatic capture of all stdout/stderr
2. **Alert on**:
   - `status = 'failed'` in `bls_load_log`
   - No successful load in X days
   - Row count deviation > 10% from expected
3. **Notifications**: Slack/Email via Pub/Sub

---

## GCP Infrastructure

### Components
- **Cloud Run Jobs**: Execute pipeline
- **Cloud Scheduler**: Trigger on schedule
- **Secret Manager**: Store DB credentials, GCS service account key
- **Cloud Storage**: Backup raw files (`bls-econ-data` bucket)
- **Cloud Logging**: Centralized logs

### Docker
- Base image: `python:3.11-slim`
- Dependencies: `psycopg2-binary`, `google-cloud-storage`, `requests`

---

## Files to Create

1. `bls_pipeline_base.py` - Shared utilities (DB, GCS, ETag checking)
2. `qcew_pipeline.py` - QCEW-specific pipeline
3. `bls_bulk_pipeline.py` - Bulk BLS tables pipeline
4. `Dockerfile` - Container definition
5. `cloudbuild.yaml` - CI/CD pipeline
6. `deploy.sh` - Deployment script

---

## Next Steps

1. Create `bls_load_log` table in database
2. Add unique constraints to all BLS tables
3. Refactor existing scripts to use ETag-based change detection
4. Dockerize and deploy to Cloud Run
5. Set up Cloud Scheduler
6. Configure monitoring/alerting
