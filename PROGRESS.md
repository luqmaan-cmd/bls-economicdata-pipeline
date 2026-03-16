# BLS Data Pipeline Progress

## Completed
- [x] CPI denormalized table schema (`cpi_schema.sql`)
- [x] CPI ETag checking in `load_cpi_denormalized()`
- [x] Docker container setup (`Dockerfile`, `docker-compose.yml`)
- [x] Local Docker testing for CPI - ETag skip verified
- [x] PPI denormalized table schema (`ppi_schema.sql`)
- [x] PPI ETag checking in `load_ppi_denormalized()`
- [x] Local Docker testing for PPI - ETag skip verified (76,178 rows)
- [x] Employment (CE) denormalized table schema (`ce_schema.sql`)
- [x] Employment ETag checking in `load_employment_denormalized()`
- [x] Employment data loaded (8,205,648 rows)
- [x] Docker image updated with all schemas
- [x] Docker ETag skip verified for all 3 datasets

## In Progress
- [ ] Next dataset

## Pending - Bulk Datasets
All bulk datasets have ETag logic in `process_bulk_dataset()` but need table verification.

| Dataset | Table | Schema Created | Tested |
|---------|-------|----------------|--------|
| employment | `bls_employment` | [ ] | [ ] |
| unemployment | `bls_unemployment` | [ ] | [ ] |
| state_employment | `bls_state_employment` | [ ] | [ ] |
| occupational | `bls_occupational` | [ ] | [ ] |
| import_export | `bls_import_export` | [ ] | [ ] |
| compensation | `bls_compensation` | [ ] | [ ] |
| jolts | `bls_jolts` | [ ] | [ ] |
| ppi | `ppi_data` | [x] | [x] |
| mass_layoff | `bls_mass_layoff` | [ ] | [ ] |
| state_metro | `bls_state_metro` | [ ] | [ ] |

## Pending - QCEW
- [ ] Verify `bls_qcew_raw` table exists
- [ ] Test QCEW load in Docker

## Pending - Cloud Deployment
- [ ] Push Docker image to registry
- [ ] Deploy to GCP (Cloud Run / Cloud Scheduler)
- [ ] Set up scheduled runs
