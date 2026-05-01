# BLS Data Pipeline Architecture

## Overview

This document describes the architecture for ingesting U.S. Bureau of Labor Statistics (BLS) economic data into a production PostgreSQL database. The pipeline runs as a serverless Cloud Run Job on Google Cloud Platform, downloading bulk data files via an Oxylabs residential proxy, storing them in Google Cloud Storage (GCS), and loading them into PostgreSQL.

---

## Architecture Diagram

```
                                    GOOGLE CLOUD PLATFORM
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │                                                                              │
 │  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────────┐   │
 │  │   Cloud      │    │    GCS       │    │     Cloud Run Job            │   │
 │  │   Scheduler  │───>│              │    │                              │   │
 │  │              │    │  bls-econ-   │    │  bls_unified_pipeline.py     │   │
  │  │  BLS-aligned │    │  data/       │    │                              │   │
 │  │  per dataset │    │              │    │  1. HEAD check (ETag)        │   │
 │  └──────────────┘    │  raw/        │    │  2. Download via proxy       │   │
 │                      │  └──cpi/     │    │  3. Upload to GCS            │   │
 │                      │  └──ppi/     │    │  4. Stream into PostgreSQL   │   │
 │                      │  └──...      │    │  5. Validate row count       │   │
 │                      │              │    │  6. Update load log         │   │
 │                      └──────┬───────┘    └──────────┬───────────────────┘   │
 │                             │                       │                       │
 │                             │                       │                       │
 │                             │    ┌──────────────────┘                       │
 │                             │    │                                          │
 │                             ▼    ▼                                          │
 │                      ┌──────────────┐                                      │
 │                      │  PostgreSQL   │                                      │
 │                      │  (Compute VM) │                                      │
 │                      │              │                                       │
 │                      │  cu_data     │                                       │
 │                      │  wp_data     │                                       │
 │                      │  ce_data     │                                       │
 │                      │  la_data     │                                       │
 │                      │  jt_data     │                                       │
 │                      │  sa_data     │                                       │
 │                      │  oe_data     │                                       │
 │                      │  ci_data     │                                       │
 │                      │  mp_data     │                                       │
 │                      │  sm_data     │                                       │
 │                      └──────────────┘                                      │
 │                                                                             │
 │              ┌──────────────────────────────────────┐                       │
 │              │  VPC Connector: bls-connector        │                       │
 │              │  (private-ranges-only egress)        │                       │
 │              └──────────────────────────────────────┘                       │
 └─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        │ HTTPS via Oxylabs Proxy
                                        │ (unblock.oxylabs.io:60000)
                                        ▼
                              ┌───────────────────┐
                              │   BLS Data Source  │
                              │                   │
                              │ download.bls.gov  │
                              │ (Akamai-protected │
                              │  — 403 without    │
                              │  proxy)           │
                              └───────────────────┘
```

---

## Components

### 1. Cloud Run Jobs

**Purpose**: Serverless execution of the data pipeline

Each dataset has its own Cloud Run Job, allowing independent scheduling, timeout configuration, and resource allocation.

| Job Name | Dataset | Timeout | Memory | CPU |
|----------|---------|---------|--------|-----|
| bls-pipeline-cpi | cpi | 20m | 512Mi | 1000m |
| bls-pipeline-ppi | ppi | 20m | 512Mi | 1000m |
| bls-pipeline-employment | employment | 20m | 512Mi | 1000m |
| bls-pipeline-unemployment | unemployment | 20m | 512Mi | 1000m |
| bls-pipeline-jolts | jolts | 20m | 512Mi | 1000m |
| bls-pipeline-state-employment | state_employment | 20m | 512Mi | 1000m |
| bls-pipeline-occupational | occupational | 20m | 512Mi | 1000m |
| bls-pipeline-compensation | compensation | 20m | 512Mi | 1000m |
| bls-pipeline-productivity | productivity | 20m | 512Mi | 1000m |
| bls-pipeline-state-metro | state_metro | 45m | 3.5Gi | 1000m |
| bls-pipeline-all | All 10 | 60m | 512Mi | 1000m |

**Why state_metro has 3.5Gi / 45m**: The SM dataset contains ~9.7M rows — the largest dataset by far. It requires more memory for the denormalization join and more time for the full download + insert cycle.

### 2. Cloud Scheduler

**Purpose**: Trigger Cloud Run Jobs aligned with BLS release schedules

Each job is triggered by a Cloud Scheduler HTTP target. Schedules are aligned with BLS publication dates (not daily) to avoid unnecessary runs when no new data is available. All times are in Europe/London timezone (UTC+0 in winter, UTC+1 in BST):

| Dataset | Cron Expression | Description |
|---------|----------------|-------------|
| cpi | `0 14 11,13 * *` | 11th and 13th of each month |
| ppi | `0 14 12-16 * *` | 12th–16th of each month |
| employment | `0 14 1-7 * 5` | 1st–7th of the month on Fridays |
| unemployment | `0 15 15-31/3 * *` | Every 3rd day from 15th to 31st |
| jolts | `0 15 1-15/3 * *` | Every 3rd day from 1st to 15th |
| state_employment | `0 15 22-28 * *` | 22nd–28th of each month |
| occupational | `0 15 */3 4,5 *` | Every 3rd day in April & May |
| compensation | `0 14 */3 1,4,7,10 *` | Every 3rd day in Jan, Apr, Jul, Oct |
| productivity | `0 14 */3 8 *` | Every 3rd day in August |
| state_metro | `0 15 18-27/3 * *` | Every 3rd day from 18th to 27th |

### 3. Google Cloud Storage (GCS)

**Purpose**: Permanent storage for raw data files

**Bucket**: `bls-econ-data`

**Structure**:
```
gs://bls-econ-data/
├── raw/
│   ├── cpi/
│   │   ├── cu.data.0.Current
│   │   ├── cu.area
│   │   ├── cu.item
│   │   └── ...
│   ├── ppi/
│   ├── employment/
│   └── ...
```

**Responsibilities**:
- Store all raw data files in original TSV format
- Serve as source of truth for re-processing and disaster recovery
- Enable re-download avoidance (files checked against GCS before downloading)

### 4. PostgreSQL Database

**Purpose**: Queryable data store for applications

**Host**: Compute Engine VM (35.214.22.65:5432)
**Database**: `asset_identification_scheme`

**Schema Design**:
- One denormalized (wide) table per dataset (e.g., `cu_data`, `ce_data`, `sm_data`)
- Schema defined in `*_schema.sql` files
- Indexes on frequently queried columns (series_id, date, area_code)
- Full refresh (truncate + reload) on each run

**Load Log Table** (`bls_load_log`):
Tracks every load attempt with dataset, ETag, row count, status, and timestamp. Used by `should_reload()` and `validate_row_count()` to make intelligent decisions about when to reload.

### 5. Oxylabs Proxy

**Purpose**: Bypass Akamai bot protection on `download.bls.gov`

BLS blocks direct access from cloud providers (returns 403). The Oxylabs residential proxy (`unblock.oxylabs.io:60000`) routes requests through residential IPs. This is mandatory — the pipeline cannot reach BLS without it.

**Trade-off**: Residential proxies are inherently less reliable than direct connections. The pipeline includes multiple safeguards to handle proxy flakiness (see Data Integrity section).

### 6. Secret Manager

**Purpose**: Store sensitive configuration

| Secret | Purpose |
|--------|---------|
| db-host | PostgreSQL host address |
| db-name | Database name |
| db-password | Database password |
| db-user | Database user |
| bls-proxy-user | Oxylabs proxy username |
| bls-proxy-pass | Oxylabs proxy password |

Mounted as environment variables in Cloud Run Job containers.

---

## Data Flow

### Incremental Update (Daily Run)

```
1. Cloud Scheduler triggers Cloud Run Job
2. Pipeline checks ETag via HEAD request (3 retries with 2s backoff)
3. should_reload() decides whether to download:
   a. No previous load → must load
   b. Previous load failed → must retry
   c. ETag unavailable (proxy failure) → trust previous successful load, skip
   d. ETag changed → must reload
   e. ETag unchanged → skip
4. If reload needed:
   a. Download data file via proxy (streaming, 600s timeout)
   b. Upload to GCS for persistence
   c. Stream into PostgreSQL via COPY (denormalized join)
   d. Validate row count against previous successful load
   e. If row count < 50% of previous → mark as failed (truncated download)
   f. If valid → mark as success in bls_load_log
5. Pipeline exits
```

---

## Data Integrity Safeguards

### Problem: Proxy Flakiness

The Oxylabs residential proxy can drop connections at any point — during HEAD requests, during data downloads, or mid-stream. This caused three distinct failure modes:

| Failure Mode | Symptom | Root Cause |
|---|---|---|
| ETag check fails | Returns `None` | Proxy drops HEAD request |
| Unnecessary full reload | Pipeline re-downloads unchanged data | Old code treated `None` ETag as "data changed" |
| Truncated download | Partial data loaded as "success" | Proxy drops GET mid-stream, no completeness check |

### Fix 1: ETag Retry Logic

`get_etag_and_last_modified()` retries 3 times with 2-second backoff. If all 3 fail, returns `None` gracefully instead of crashing.

### Fix 2: Smart `should_reload()`

When ETag is `None`, checks the load log instead of forcing a reload:
- Previous load succeeded → skip reload (trust existing data)
- Previous load failed → force reload
- No previous entry → force reload

### Fix 3: Row Count Validation

`validate_row_count()` compares new row count against previous successful load:
- **< 50%** → mark as `failed` (truncated download detected)
- **50-90%** → log warning but mark as `success`
- **> 90%** → mark as `success`

Applied to all 10 denormalized dataset loaders.

### Fix 4: Malformed Row Handling

Rows with fewer columns than expected are skipped during COPY transforms instead of crashing the pipeline.

---

## Idempotency & Data Refresh Strategy

### Why BLS Data Changes

BLS frequently revises historical data after initial publication:

| Revision Type | Description | Impact |
|---|---|---|
| Seasonal adjustments | Recalculated annually | Affects all prior months in year |
| Benchmark revisions | Annual alignment to QCEW | Can revise up to 5 years of data |
| Late respondent data | Companies report late | Monthly revisions for 2-3 months |
| Methodology changes | Survey improvements | Historical data re-published |
| Error corrections | Data quality fixes | Specific series corrected |

### Strategy: Full Refresh (Truncate + Reload)

All datasets use full refresh rather than upsert:

| Approach | Pros | Cons |
|---|---|---|
| **Full Refresh** | Simple, guaranteed consistency, handles deletions | Re-processes all data |
| **Upsert** | Only processes changes | Complex keys, stale data risk |

**Why full refresh is preferred**:
1. PostgreSQL COPY loads millions of rows in minutes
2. Simpler logic — no unique key management, no conflict resolution
3. Handles deletions — if BLS removes erroneous data, full refresh catches it
4. Guaranteed consistency — database always matches source file exactly

---

## Deployment

### Building & Pushing the Docker Image

```bash
# Build for Cloud Run (linux/amd64)
docker buildx build --platform linux/amd64 --load \
  -t europe-west2-docker.pkg.dev/testgke-412710/bls-pipeline/bls-pipeline:latest .

# Push to Artifact Registry
docker push europe-west2-docker.pkg.dev/testgke-412710/bls-pipeline/bls-pipeline:latest
```

### Updating Cloud Run Jobs

After pushing a new image, update each job to pick up the new digest:

```bash
# Update a single job
gcloud run jobs update bls-pipeline-cpi \
  --region=europe-west2 \
  --image=europe-west2-docker.pkg.dev/testgke-412710/bls-pipeline/bls-pipeline:latest

# Update all jobs
for dataset in cpi ppi employment unemployment jolts state_employment occupational compensation productivity state_metro; do
  gcloud run jobs update "bls-pipeline-${dataset//_/-}" \
    --region=europe-west2 \
    --image=europe-west2-docker.pkg.dev/testgke-412710/bls-pipeline/bls-pipeline:latest
done
```

### Running a Job Manually

```bash
# Run a specific dataset
gcloud run jobs execute bls-pipeline-cpi --region=europe-west2 --wait

# Force re-download (requires --force flag in job args)
# Update job args temporarily:
gcloud run jobs update bls-pipeline-cpi \
  --region=europe-west2 \
  --args="--datasets=cpi,--force"
```

---

## Monitoring

### Checking Job Status

```bash
# List recent executions
gcloud run jobs executions list --job=bls-pipeline-cpi --region=europe-west2

# View logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=bls-pipeline-cpi" \
  --limit=50 --project=testgke-412710
```

### Checking Load Log

```sql
SELECT dataset, data_key, rows_loaded, status, error_message, load_timestamp
FROM bls_load_log
ORDER BY load_timestamp DESC;
```

### Checking Table Row Counts

```sql
SELECT 'cu_data' AS table, COUNT(*) FROM cu_data
UNION ALL SELECT 'wp_data', COUNT(*) FROM wp_data
UNION ALL SELECT 'ce_data', COUNT(*) FROM ce_data
UNION ALL SELECT 'la_data', COUNT(*) FROM la_data
UNION ALL SELECT 'jt_data', COUNT(*) FROM jt_data
UNION ALL SELECT 'sa_data', COUNT(*) FROM sa_data
UNION ALL SELECT 'oe_data', COUNT(*) FROM oe_data
UNION ALL SELECT 'ci_data', COUNT(*) FROM ci_data
UNION ALL SELECT 'mp_data', COUNT(*) FROM mp_data
UNION ALL SELECT 'sm_data', COUNT(*) FROM sm_data;
```

---

## Cost Optimization

- Cloud Run Jobs only bill for execution time (no idle costs)
- GCS Standard storage for raw files (minimal cost — files are small)
- Staggered schedules avoid concurrent database load
- Proxy costs are the primary ongoing expense
