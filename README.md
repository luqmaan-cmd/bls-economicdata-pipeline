# BLS Data Downloads

Bulk downloaded from U.S. Bureau of Labor Statistics using the [BLSloadR](https://github.com/schmidtDETR/BLSloadR) R package.

## Datasets

| Code | Dataset | Records | File |
|------|---------|---------|------|
| ce | Current Employment Statistics (National) | 7,881,789 | bls_ce_data.csv |
| cu | Consumer Price Index | 1,549,316 | bls_cu_data.csv |
| jt | Job Openings (JOLTS) | 321,300 | bls_jt_data.csv |
| pr | Producer Price Index | 37,521 | bls_pr_data.csv |
| is | Import/Export Price Indexes | 5,691,796 | bls_is_data.csv |
| ci | Employment Cost Index | 96,140 | bls_ci_data.csv |
| sm | State and Area Employment | 9,450,308 | bls_sm_data.csv |
| sa | State and Area Employment | 2,786,305 | bls_sa_data.csv |
| la | Local Area Unemployment Statistics | 244,902 | bls_la_data.csv |
| oe | Occupational Employment Statistics | 6,036,958 | bls_oe_data.csv |
| mp | Major Sector Productivity | 176,059 | bls_mp_data.csv |
| qcew | Quarterly Census of Employment and Wages | 365,960 | bls_qcew_data.csv |

## Source

- BLS Bulk Download: https://download.bls.gov/pub/time.series/
- BLSloadR Package: https://github.com/schmidtDETR/BLSloadR

## Download Script

See `bls_bulk_download.R` for the R script used to download these datasets.
