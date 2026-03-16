# BLS Datasets Documentation

## Locked In Datasets (12 Total)

| # | Code | Name | Data File | Table Name | Dimensions |
|---|------|------|-----------|------------|------------|
| 1 | CE | Current Employment Statistics | `ce.data.0.AllCESSeries` | `bls_employment` | series, industry, supersector, datatype, seasonal, period |
| 2 | CU | Consumer Price Index | `cu.data.1.AllItems` | `bls_cpi` | series, area, item, period, periodicity |
| 3 | PR | Major Sector Productivity (Quarterly) | `pr.data.1.AllData` | `bls_productivity_quarterly` | series, sector, measure, duration, class |
| 4 | MP | Major Sector Productivity (Annual) | `mp.data.1.AllData` | `bls_productivity_annual` | series, sector, measure, duration |
| 5 | WP | Producer Price Index | `wp.data.0.Current` | `bls_ppi` | series, item, group |
| 6 | IS | Import/Export Price Indexes | `is.data.1.AllData` | `bls_import_export` | series, area, industry, supersector, data_type |
| 7 | LA | Local Area Unemployment (State) | `la.data.2.AllStatesU` + `la.data.3.AllStatesS` | `bls_unemployment` | series, area, area_type, measure |
| 8 | SA | State and Area Employment | `sa.data.0.Current` | `bls_state_employment` | series, area, state, industry, data_type |
| 9 | JT | JOLTS | `jt.data.1.AllItems` | `bls_jolts` | series, state, area, industry, dataelement |
| 10 | CI | Compensation | `ci.data.1.AllData` | `bls_compensation` | series, area, industry, occupation, estimate |
| 11 | SM | State/Metro Employment | `sm.data.1.AllData` | `bls_state_metro` | series, state, area, industry, supersector |
| 12 | OE | Occupational Employment | `oe.data.1.AllData` | `bls_occupational` | series, area, industry, occupation, datatype |

**Skipped:** QCEW (different URL structure - uses ZIP files per year, already downloaded separately)

---

## Dataset Details

### CE - Current Employment Statistics
- **URL:** `https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries`
- **Table:** `bls_employment`
- **Dimensions:** `ce.series`, `ce.industry`, `ce.supersector`, `ce.datatype`, `ce.seasonal`, `ce.period`

### CU - Consumer Price Index
- **URL:** `https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems`
- **Table:** `bls_cpi`
- **Dimensions:** `cu.series`, `cu.area`, `cu.item`, `cu.period`, `cu.periodicity`

### PR - Major Sector Productivity (Quarterly)
- **URL:** `https://download.bls.gov/pub/time.series/pr/pr.data.1.AllData`
- **Table:** `bls_productivity_quarterly`
- **Dimensions:** `pr.series`, `pr.sector`, `pr.measure`, `pr.duration`, `pr.class`

### MP - Major Sector Productivity (Annual)
- **URL:** `https://download.bls.gov/pub/time.series/mp/mp.data.1.AllData`
- **Table:** `bls_productivity_annual`
- **Dimensions:** `mp.series`, `mp.sector`, `mp.measure`, `mp.duration`

### WP - Producer Price Index
- **URL:** `https://download.bls.gov/pub/time.series/wp/wp.data.0.Current`
- **Table:** `bls_ppi`
- **Dimensions:** `wp.series`, `wp.item`, `wp.group`

### IS - Import/Export Price Indexes
- **URL:** `https://download.bls.gov/pub/time.series/is/is.data.1.AllData`
- **Table:** `bls_import_export`
- **Dimensions:** `is.series`, `is.area`, `is.industry`, `is.supersector`, `is.data_type`

### LA - Local Area Unemployment (State-level)
- **URLs:**
  - `https://download.bls.gov/pub/time.series/la/la.data.2.AllStatesU` (Unadjusted)
  - `https://download.bls.gov/pub/time.series/la/la.data.3.AllStatesS` (Seasonally Adjusted)
- **Table:** `bls_unemployment`
- **Dimensions:** `la.series`, `la.area`, `la.area_type`, `la.measure`

### SA - State and Area Employment
- **URL:** `https://download.bls.gov/pub/time.series/sa/sa.data.0.Current`
- **Table:** `bls_state_employment`
- **Dimensions:** `sa.series`, `sa.area`, `sa.state`, `sa.industry`, `sa.data_type`

### JT - JOLTS
- **URL:** `https://download.bls.gov/pub/time.series/jt/jt.data.1.AllItems`
- **Table:** `bls_jolts`
- **Dimensions:** `jt.series`, `jt.state`, `jt.area`, `jt.industry`, `jt.dataelement`

### CI - Compensation
- **URL:** `https://download.bls.gov/pub/time.series/ci/ci.data.1.AllData`
- **Table:** `bls_compensation`
- **Dimensions:** `ci.series`, `ci.area`, `ci.industry`, `ci.occupation`, `ci.estimate`

### SM - State/Metro Employment
- **URL:** `https://download.bls.gov/pub/time.series/sm/sm.data.1.AllData`
- **Table:** `bls_state_metro`
- **Dimensions:** `sm.series`, `sm.state`, `sm.area`, `sm.industry`, `sm.supersector`

### OE - Occupational Employment
- **URL:** `https://download.bls.gov/pub/time.series/oe/oe.data.1.AllData`
- **Table:** `bls_occupational`
- **Dimensions:** `oe.series`, `oe.area`, `oe.industry`, `oe.occupation`, `oe.datatype`

---

## Usage

### Download a single dataset:
```bash
python3 -m bls_pipeline.download -d cu
```

### Download with dimension files:
```bash
python3 -m bls_pipeline.download -d cu --dimensions
```

### Download all datasets:
```bash
python3 -m bls_pipeline.download --all --dimensions
```

### Force re-download:
```bash
python3 -m bls_pipeline.download -d cu --force
```

---

## URL Pattern

All BLS time series data follows this pattern:
```
https://download.bls.gov/pub/time.series/{code}/{code}.{file}
```

Example:
```
https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems
https://download.bls.gov/pub/time.series/cu/cu.series
```
