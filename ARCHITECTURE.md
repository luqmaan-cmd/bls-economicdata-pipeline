# BLS Data Pipeline Architecture

## Overview

This document describes the architecture for ingesting U.S. Bureau of Labor Statistics (BLS) economic data into a production environment. The pipeline downloads bulk data files, stores them in Google Cloud Storage (GCS), and loads them into a PostgreSQL database running on a Google Cloud VM.

---

## Data Sources

### BLS Bulk Download Endpoints

All data is sourced from official BLS bulk download endpoints:

| Dataset | Code | Endpoint Pattern |
|---------|------|------------------|
| Current Employment Statistics (National) | ce | `https://download.bls.gov/pub/time.series/ce/` |
| Current Employment Statistics (State/Area) | ces | `https://download.bls.gov/pub/time.series/sm/` |
| Consumer Price Index | cu | `https://download.bls.gov/pub/time.series/cu/` |
| Producer Price Index | pr | `https://download.bls.gov/pub/time.series/pr/` |
| Import/Export Price Indexes | is | `https://download.bls.gov/pub/time.series/is/` |
| Job Openings (JOLTS) | jt | `https://download.bls.gov/pub/time.series/jt/` |
| Employment Cost Index | ci | `https://download.bls.gov/pub/time.series/ci/` |
| Local Area Unemployment Statistics | la | `https://download.bls.gov/pub/time.series/la/` |
| State/Area Employment | sm | `https://download.bls.gov/pub/time.series/sm/` |
| Occupational Employment & Wages | oe | `https://download.bls.gov/pub/time.series/oe/` |
| Major Sector Productivity | mp | `https://download.bls.gov/pub/time.series/mp/` |
| Quarterly Census of Employment & Wages | qcew | `https://data.bls.gov/cew/data/files/{YEAR}/csv/{YEAR}_qtrly_singlefile.zip` |

### Data Format

- **Format**: CSV (comma-separated values)
- **Compression**: ZIP (QCEW only)
- **Encoding**: UTF-8
- **Update Frequency**: Monthly, quarterly, or annual depending on dataset

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GOOGLE CLOUD PLATFORM                           │
│                                                                              │
│  ┌─────────────────┐         ┌─────────────────────────────────────────┐   │
│  │   GCS Bucket    │         │              Google Cloud VM             │   │
│  │                 │         │                                         │   │
│  │  ┌───────────┐  │  gsutil │  ┌───────────┐    ┌─────────────────┐   │   │
│  │  │  Raw CSV  │  │ ──────> │  │  Temp     │    │   PostgreSQL    │   │   │
│  │  │  Files    │  │         │  │  Storage  │───>│   Database      │   │   │
│  │  │  (zipped) │  │  <───── │  │  (unzip)  │    │                 │   │   │
│  │  └───────────┘  │  upload │  └───────────┘    └─────────────────┘   │   │
│  │                 │         │        │                               │   │
│  │  - Permanent    │         │        ▼                               │   │
│  │    storage      │         │  ┌───────────┐                         │   │
│  │  - Source of    │         │  │  Cleanup  │                         │   │
│  │    truth        │         │  │  (delete) │                         │   │
│  │  - Backup       │         │  └───────────┘                         │   │
│  └─────────────────┘         └─────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                        ▲
                                        │
                                        │ HTTPS
                                        │
                              ┌─────────┴─────────┐
                              │   BLS Data Source │
                              │                   │
                              │  - download.bls.gov
                              │  - data.bls.gov   │
                              └───────────────────┘
```

---

## Components

### 1. Google Cloud Storage (GCS) Bucket

**Purpose**: Permanent storage for raw data files

**Structure**:
```
gs://{bucket-name}/
├── bls/
│   ├── csv/
│   │   ├── ce_data.csv
│   │   ├── cu_data.csv
│   │   ├── pr_data.csv
│   │   └── ...
│   └── metadata/
│       └── last_modified.json
└── qcew/
    ├── 1990_qtrly_singlefile.zip
    ├── 1991_qtrly_singlefile.zip
    ├── ...
    └── 2025_qtrly_singlefile.zip
```

**Responsibilities**:
- Store all raw data files in original format (compressed when applicable)
- Maintain metadata about last download timestamps and ETags
- Serve as source of truth for re-processing and disaster recovery
- Enable versioning for audit trail (optional)

### 2. Google Cloud VM

**Purpose**: Compute resource for data processing

**Specifications**:
- Sufficient CPU/RAM for decompression and data transformation
- Ephemeral or persistent disk for temporary file storage
- Network access to GCS and BLS endpoints
- PostgreSQL installed and configured

**Responsibilities**:
- Download files from BLS endpoints
- Decompress ZIP files to temporary storage
- Transform data if needed (encoding, formatting)
- Load data into PostgreSQL
- Clean up temporary files after processing

### 3. PostgreSQL Database

**Purpose**: Queryable data store for applications

**Schema Design**:
- One table per dataset (e.g., `cpi_data`, `employment_data`, `qcew_data`)
- Appropriate column types for each field
- Indexes on frequently queried columns (date, series_id, geography)
- Partitioning for large tables (recommended for QCEW)

**Responsibilities**:
- Store processed, queryable data
- Support application queries with low latency
- Maintain data integrity with constraints

---

## Data Flow

### Initial Load (First Run)

```
1. Check GCS bucket for existing files
2. If empty, download all datasets from BLS endpoints
3. Store raw files in GCS bucket
4. For each file:
   a. Download from GCS to VM temp storage
   b. If ZIP, decompress to extract CSV
   c. Load CSV into PostgreSQL using COPY command
   d. Delete temporary files from VM
5. Record metadata (ETag, Last-Modified, download timestamp)
```

### Incremental Update (Subsequent Runs)

```
1. For each dataset endpoint:
   a. Fetch HTTP headers (HEAD request)
   b. Compare ETag/Last-Modified with stored metadata
   c. If changed:
      - Download updated file
      - Upload to GCS (overwrite or version)
      - Truncate table and reload full dataset
      - Update metadata
   d. If unchanged, skip
2. Clean up temporary files
```

---

## Update Detection Strategy

### HTTP Headers for Change Detection

| Header | Purpose | Usage |
|--------|---------|-------|
| `Last-Modified` | Timestamp of last file update | Compare with stored timestamp |
| `ETag` | Unique identifier for file version | Compare with stored ETag |
| `Content-Length` | File size in bytes | Quick check for size changes |

### Implementation

```bash
# Check if file has been updated
curl -sI "https://data.bls.gov/cew/data/files/2025/csv/2025_qtrly_singlefile.zip" \
  | grep -E "Last-Modified|ETag|Content-Length"
```

### Metadata Storage

Store metadata in GCS as JSON:

```json
{
  "datasets": {
    "qcew_2025": {
      "url": "https://data.bls.gov/cew/data/files/2025/csv/2025_qtrly_singlefile.zip",
      "etag": "\"122b489e-63dcfcdf704e5\"",
      "last_modified": "Mon, 05 Jan 2026 12:46:38 GMT",
      "content_length": 157730191,
      "downloaded_at": "2026-02-18T10:00:00Z",
      "gcs_path": "gs://bucket/qcew/2025_qtrly_singlefile.zip"
    }
  }
}
```

---

## Data Loading Strategy

### PostgreSQL COPY Command

For optimal performance, use PostgreSQL's `COPY` command for bulk inserts:

```sql
-- From file on VM
COPY table_name FROM '/path/to/file.csv' WITH (FORMAT csv, HEADER true);

-- From stdin (streaming)
\copy table_name FROM PROGRAM 'unzip -p file.zip' WITH (FORMAT csv, HEADER true);
```

### Handling Large Files

For datasets with large file sizes (QCEW, CE, OES):

1. **Stream processing**: Decompress and load in one pipeline
2. **Chunked loading**: Process in batches to manage memory
3. **Disable indexes**: Drop indexes before load, recreate after
4. **Partitioning**: Use table partitioning for time-series data

### Example: Streaming Load

```bash
# Download, decompress, and load in one pipeline
gsutil cp gs://bucket/qcew/2025_qtrly_singlefile.zip - | \
  unzip -p - | \
  psql -c "COPY qcew_data FROM STDIN WITH (FORMAT csv, HEADER true);"
```

---

## Idempotency & Data Refresh Strategy

### Why BLS Data Changes

BLS frequently revises historical data after initial publication:

| Revision Type | Description | Impact |
|---------------|-------------|--------|
| **Seasonal adjustments** | Recalculated annually | Affects all prior months in year |
| **Benchmark revisions** | Annual alignment to QCEW | Can revise up to 5 years of data |
| **Late respondent data** | Companies report late | Monthly revisions for 2-3 months |
| **Methodology changes** | Survey improvements | Historical data re-published |
| **Error corrections** | Data quality fixes | Specific series corrected |

**Example of revision cascade:**
```
Jan 2025: Employment reported as 150,000
Feb 2025: Revised to 152,000 (late reports)
Mar 2025: Revised to 148,000 (benchmark adjustment)
```

### Strategy: Full Refresh

All datasets use **full refresh** (truncate + reload) rather than upsert:

| Approach | Pros | Cons |
|----------|------|------|
| **Full Refresh** | Simple, guaranteed consistency, handles deletions, no key management | Re-processes all data |
| **Upsert** | Only processes changes | Complex keys, doesn't handle deletions, stale data risk |

**Why full refresh is preferred:**

1. **COPY is fast** - PostgreSQL loads millions of rows in minutes
2. **Simpler logic** - No unique key management, no conflict resolution
3. **Handles deletions** - If BLS removes erroneous data, full refresh catches it
4. **Guaranteed consistency** - Database always matches source file exactly
5. **No stale data** - Eliminates risk of outdated rows persisting

### Implementation

```sql
-- Start transaction
BEGIN;

-- Truncate existing data
TRUNCATE TABLE cpi_data;

-- Load fresh data from CSV
COPY cpi_data FROM '/path/to/file.csv' WITH (FORMAT csv, HEADER true);

-- Commit transaction
COMMIT;
```

### Dataset-Specific Approach

| Dataset | Strategy | Reason |
|---------|----------|--------|
| CPI, PPI, Import/Export | Full Refresh | Monthly data with revisions |
| Employment (CE, CES, SM) | Full Refresh | Monthly with frequent benchmark revisions |
| JOLTS | Full Refresh | Monthly with 3-month revision window |
| Local Unemployment (LA) | Full Refresh | Monthly with annual benchmarking |
| Employment Cost Index (CI) | Full Refresh | Quarterly with revisions |
| Productivity (MP) | Full Refresh | Quarterly with revisions |
| QCEW | Full Refresh (by year) | Each year file is complete independent snapshot |
| OES | Full Refresh | Annual, complete replacement |

### QCEW Special Handling

QCEW is processed **one year at a time** due to file size:

```sql
-- Process each year independently
TRUNCATE TABLE qcew_data WHERE year = 2024;
COPY qcew_data FROM '/path/to/2024.csv' WITH (FORMAT csv, HEADER true);

TRUNCATE TABLE qcew_data WHERE year = 2025;
COPY qcew_data FROM '/path/to/2025.csv' WITH (FORMAT csv, HEADER true);
```

This allows:
- Parallel processing of multiple years
- Incremental updates for new years only
- Smaller transaction sizes

---

## Error Handling

### Download Failures

- Retry with exponential backoff
- Log failed downloads
- Alert on persistent failures
- Resume from last successful file

### Data Loading Failures

- Validate CSV structure before loading
- Use transactions for atomic operations
- Log rejected rows
- Maintain staging tables for validation

### Recovery

- All raw files preserved in GCS
- Metadata tracks last successful load
- Can re-process from GCS without re-downloading

---

## Security Considerations

### Network Security

- Use VPC for VM and database communication
- Enable private Google Access for GCS
- Restrict egress to necessary BLS endpoints

### Access Control

- Use IAM roles for GCS access
- Database credentials stored in Secret Manager
- Service accounts with minimal permissions

### Data Integrity

- Verify file checksums after download
- Validate row counts after load
- Monitor for data anomalies

---

## Monitoring & Alerting

### Metrics to Track

- Download success/failure rate
- Time since last update per dataset
- Database load duration
- Storage utilization (GCS and VM disk)
- Data freshness (latest date in each table)

### Alerts

- Failed downloads
- Stale data (no updates beyond expected schedule)
- Load failures
- Storage capacity warnings

---

## Cost Optimization

### Storage

- Keep files compressed in GCS (8-10x smaller)
- Use Standard storage class for frequently accessed data
- Consider Nearline/Coldline for historical archives

### Compute

- Schedule updates during off-peak hours
- Use preemptible VMs for batch processing (with checkpointing)
- Right-size VM based on workload

### Network

- Minimize cross-region transfers
- Use same region for GCS, VM, and database

---

## Release Schedule Reference

| Dataset | Frequency | Typical Release Day | Lag |
|---------|-----------|---------------------|-----|
| Employment Situation | Monthly | First Friday | 1 week |
| CPI | Monthly | ~13th of month | 2 weeks |
| PPI | Monthly | ~15th of month | 2 weeks |
| JOLTS | Monthly | First Tuesday | 5 weeks |
| QCEW | Quarterly | ~6 months after quarter | 6 months |
| Employment Cost Index | Quarterly | Last Friday of quarter | 3 weeks |
| Import/Export Prices | Monthly | ~10th of month | 2 weeks |
| Local Unemployment | Monthly | First Friday | 4 weeks |
| OES | Annual | May (following year) | 6 months |

---

## Future Considerations

- **Data versioning**: Enable GCS versioning for audit trail
- **API integration**: Use BLS API for real-time updates between bulk downloads
- **Data quality**: Add validation and cleansing pipeline
- **Documentation**: Auto-generate data dictionary from BLS metadata
- **Parallel loading**: Process multiple datasets concurrently for faster refresh
