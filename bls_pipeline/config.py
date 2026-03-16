import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "35.214.22.65"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "hSg8vrqSb9SYT1Eg"),
    "database": os.getenv("DB_NAME", "asset_identification_scheme")
}

GCS_CONFIG = {
    "bucket": "bls-econ-data",
    "credentials_path": os.path.join(os.path.dirname(__file__), "..", "testgke-412710-fded8bee5457.json")
}

BLS_DATASETS = {
    "cu": {
        "name": "Consumer Price Index (CPI)",
        "url": "https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems",
        "file_prefix": "bls_cu",
        "table": "bls_cpi",
        "dimensions": ["cu.series", "cu.area", "cu.item", "cu.period", "cu.periodicity"]
    },
    "ce": {
        "name": "Current Employment Statistics",
        "url": "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries",
        "file_prefix": "bls_ce",
        "table": "bls_employment",
        "dimensions": ["ce.series", "ce.industry", "ce.supersector", "ce.datatype", "ce.seasonal", "ce.period"]
    },
    "qcew": {
        "name": "Quarterly Census of Employment and Wages",
        "url": "https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip",
        "file_prefix": "bls_qcew",
        "table": "bls_qcew",
        "year_range": range(1990, 2025)
    },
    "la": {
        "name": "Local Area Unemployment Statistics (State-level)",
        "urls": [
            "https://download.bls.gov/pub/time.series/la/la.data.2.AllStatesU",
            "https://download.bls.gov/pub/time.series/la/la.data.3.AllStatesS"
        ],
        "file_prefix": "bls_la",
        "table": "bls_unemployment",
        "dimensions": ["la.series", "la.area", "la.area_type", "la.measure"]
    },
    "sa": {
        "name": "State and Area Employment",
        "url": "https://download.bls.gov/pub/time.series/sa/sa.data.0.Current",
        "file_prefix": "bls_sa",
        "table": "bls_state_employment",
        "dimensions": ["sa.series", "sa.area", "sa.state", "sa.industry", "sa.data_type"]
    },
    "oe": {
        "name": "Occupational Employment and Wage Statistics",
        "url": "https://download.bls.gov/pub/time.series/oe/oe.data.1.AllData",
        "file_prefix": "bls_oe",
        "table": "bls_occupational",
        "dimensions": ["oe.series", "oe.area", "oe.industry", "oe.occupation", "oe.datatype"]
    },
    "is": {
        "name": "Import/Export Price Indexes",
        "url": "https://download.bls.gov/pub/time.series/is/is.data.1.AllData",
        "file_prefix": "bls_is",
        "table": "bls_import_export",
        "dimensions": ["is.series", "is.area", "is.industry", "is.supersector", "is.data_type"]
    },
    "ci": {
        "name": "Compensation and Working Conditions",
        "url": "https://download.bls.gov/pub/time.series/ci/ci.data.1.AllData",
        "file_prefix": "bls_ci",
        "table": "bls_compensation",
        "dimensions": ["ci.series", "ci.area", "ci.industry", "ci.occupation", "ci.estimate"]
    },
    "jt": {
        "name": "Job Openings and Labor Turnover Survey",
        "url": "https://download.bls.gov/pub/time.series/jt/jt.data.1.AllItems",
        "file_prefix": "bls_jt",
        "table": "bls_jolts",
        "dimensions": ["jt.series", "jt.state", "jt.area", "jt.industry", "jt.dataelement"]
    },
    "pr": {
        "name": "Major Sector Productivity (Quarterly)",
        "url": "https://download.bls.gov/pub/time.series/pr/pr.data.1.AllData",
        "file_prefix": "bls_pr",
        "table": "bls_productivity_quarterly",
        "dimensions": ["pr.series", "pr.sector", "pr.measure", "pr.duration", "pr.class"]
    },
    "mp": {
        "name": "Major Sector Productivity (Annual)",
        "url": "https://download.bls.gov/pub/time.series/mp/mp.data.1.AllData",
        "file_prefix": "bls_mp",
        "table": "bls_productivity_annual",
        "dimensions": ["mp.series", "mp.sector", "mp.measure", "mp.duration"]
    },
    "sm": {
        "name": "State and Metropolitan Area Employment",
        "url": "https://download.bls.gov/pub/time.series/sm/sm.data.1.AllData",
        "file_prefix": "bls_sm",
        "table": "bls_state_metro",
        "dimensions": ["sm.series", "sm.state", "sm.area", "sm.industry", "sm.supersector"]
    },
    "wp": {
        "name": "Producer Price Index",
        "url": "https://download.bls.gov/pub/time.series/wp/wp.data.0.Current",
        "file_prefix": "bls_wp",
        "table": "bls_ppi",
        "dimensions": ["wp.series", "wp.item", "wp.group"]
    }
}

TABLE_SCHEMAS = {
    "bls_cpi": """
        CREATE TABLE IF NOT EXISTS bls_cpi (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            seasonal VARCHAR(1),
            base_period VARCHAR(50),
            series_title TEXT,
            begin_year INTEGER,
            begin_period VARCHAR(10),
            end_year INTEGER,
            end_period VARCHAR(10),
            aspect_type VARCHAR(10),
            aspect_value VARCHAR(100),
            area_name VARCHAR(200),
            base_name VARCHAR(100),
            item_name VARCHAR(500),
            period_abbr VARCHAR(10),
            period_name VARCHAR(50),
            periodicity_name VARCHAR(50),
            date DATE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_employment": """
        CREATE TABLE IF NOT EXISTS bls_employment (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            footnote_codes VARCHAR(50),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_qcew": """
        CREATE TABLE IF NOT EXISTS bls_qcew (
            id SERIAL PRIMARY KEY,
            area_fips VARCHAR(10),
            industry_code VARCHAR(10),
            own_code INTEGER,
            agglvl_code INTEGER,
            size_code INTEGER,
            year INTEGER,
            qtr INTEGER,
            disclosure_code VARCHAR(5),
            qtrly_estabs INTEGER,
            month1_emplvl INTEGER,
            month2_emplvl INTEGER,
            month3_emplvl INTEGER,
            total_qtrly_wages NUMERIC,
            taxable_qtrly_wages NUMERIC,
            qtrly_contributions NUMERIC,
            avg_wkly_wage NUMERIC,
            lq_disclosure_code VARCHAR(5),
            lq_qtrly_estabs NUMERIC,
            lq_month1_emplvl NUMERIC,
            lq_month2_emplvl NUMERIC,
            lq_month3_emplvl NUMERIC,
            lq_total_qtrly_wages NUMERIC,
            lq_taxable_qtrly_wages NUMERIC,
            lq_qtrly_contributions NUMERIC,
            lq_avg_wkly_wage NUMERIC,
            oty_disclosure_code VARCHAR(5),
            oty_qtrly_estabs_chg INTEGER,
            oty_qtrly_estabs_pct_chg NUMERIC,
            oty_month1_emplvl_chg INTEGER,
            oty_month1_emplvl_pct_chg NUMERIC,
            oty_month2_emplvl_chg INTEGER,
            oty_month2_emplvl_pct_chg NUMERIC,
            oty_month3_emplvl_chg INTEGER,
            oty_month3_emplvl_pct_chg NUMERIC,
            oty_total_qtrly_wages_chg NUMERIC,
            oty_total_qtrly_wages_pct_chg NUMERIC,
            oty_taxable_qtrly_wages_chg NUMERIC,
            oty_taxable_qtrly_wages_pct_chg NUMERIC,
            oty_qtrly_contributions_chg NUMERIC,
            oty_qtrly_contributions_pct_chg NUMERIC,
            oty_avg_wkly_wage_chg NUMERIC,
            oty_avg_wkly_wage_pct_chg NUMERIC,
            date DATE,
            industry_title VARCHAR(500),
            ind_level VARCHAR(50),
            naics_2d VARCHAR(10),
            sector VARCHAR(10),
            vintage_start INTEGER,
            vintage_end INTEGER,
            area_title VARCHAR(500),
            area_type VARCHAR(100),
            stfips VARCHAR(10),
            specified_region VARCHAR(50),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_unemployment": """
        CREATE TABLE IF NOT EXISTS bls_unemployment (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            footnote_codes VARCHAR(50),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_state_employment": """
        CREATE TABLE IF NOT EXISTS bls_state_employment (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            footnote_codes VARCHAR(50),
            series_title TEXT,
            area_code VARCHAR(20),
            area_type_code VARCHAR(10),
            supersector_code VARCHAR(10),
            industry_code VARCHAR(20),
            data_type_code VARCHAR(10),
            seasonal VARCHAR(1),
            area_name VARCHAR(500),
            supersector_name VARCHAR(200),
            industry_name VARCHAR(500),
            data_type_name VARCHAR(200),
            date DATE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_occupational": """
        CREATE TABLE IF NOT EXISTS bls_occupational (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            footnote_codes VARCHAR(50),
            series_title TEXT,
            area_code VARCHAR(20),
            area_type_code VARCHAR(10),
            industry_code VARCHAR(20),
            occupation_code VARCHAR(20),
            datatype_code VARCHAR(10),
            area_name VARCHAR(500),
            industry_name VARCHAR(500),
            occupation_name VARCHAR(500),
            datatype_name VARCHAR(200),
            date DATE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_import_export": """
        CREATE TABLE IF NOT EXISTS bls_import_export (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            footnote_codes VARCHAR(50),
            series_title TEXT,
            area_code VARCHAR(20),
            area_name VARCHAR(500),
            item_code VARCHAR(50),
            item_name VARCHAR(500),
            date DATE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_compensation": """
        CREATE TABLE IF NOT EXISTS bls_compensation (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            footnote_codes VARCHAR(50),
            series_title TEXT,
            area_code VARCHAR(20),
            area_name VARCHAR(500),
            industry_code VARCHAR(20),
            industry_name VARCHAR(500),
            occupation_code VARCHAR(20),
            occupation_name VARCHAR(500),
            datatype_code VARCHAR(10),
            datatype_name VARCHAR(200),
            date DATE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_jolts": """
        CREATE TABLE IF NOT EXISTS bls_jolts (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            footnote_codes VARCHAR(50),
            series_title TEXT,
            area_code VARCHAR(20),
            area_name VARCHAR(500),
            industry_code VARCHAR(20),
            industry_name VARCHAR(500),
            dataelement_code VARCHAR(10),
            dataelement_name VARCHAR(200),
            ratelevel_code VARCHAR(10),
            seasonal VARCHAR(1),
            date DATE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_productivity_quarterly": """
        CREATE TABLE IF NOT EXISTS bls_productivity_quarterly (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            seasonal VARCHAR(1),
            base_year VARCHAR(20),
            begin_year INTEGER,
            begin_period VARCHAR(10),
            end_year INTEGER,
            end_period VARCHAR(10),
            class_text VARCHAR(500),
            duration_text VARCHAR(100),
            measure_text VARCHAR(200),
            period_abbr VARCHAR(10),
            period_name VARCHAR(50),
            sector_name VARCHAR(200),
            date DATE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_productivity_annual": """
        CREATE TABLE IF NOT EXISTS bls_productivity_annual (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            base_year VARCHAR(20),
            series_title TEXT,
            begin_year INTEGER,
            begin_period VARCHAR(10),
            end_year INTEGER,
            end_period VARCHAR(10),
            duration_text VARCHAR(100),
            measure_text VARCHAR(200),
            sector_name VARCHAR(200),
            date DATE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_state_metro": """
        CREATE TABLE IF NOT EXISTS bls_state_metro (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            footnote_codes VARCHAR(50),
            series_title TEXT,
            area_code VARCHAR(20),
            area_name VARCHAR(500),
            supersector_code VARCHAR(10),
            supersector_name VARCHAR(200),
            industry_code VARCHAR(20),
            industry_name VARCHAR(500),
            data_type_code VARCHAR(10),
            data_type_name VARCHAR(200),
            seasonal VARCHAR(1),
            date DATE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_ppi": """
        CREATE TABLE IF NOT EXISTS bls_ppi (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(50),
            year INTEGER,
            period VARCHAR(10),
            value NUMERIC,
            seasonal VARCHAR(1),
            series_title TEXT,
            begin_year INTEGER,
            begin_period VARCHAR(10),
            end_year INTEGER,
            end_period VARCHAR(10),
            item_code VARCHAR(50),
            item_name VARCHAR(500),
            group_code VARCHAR(10),
            group_name VARCHAR(200),
            date DATE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "bls_load_log": """
        CREATE TABLE IF NOT EXISTS bls_load_log (
            id SERIAL PRIMARY KEY,
            dataset VARCHAR(50) NOT NULL,
            data_key VARCHAR(100) NOT NULL,
            etag VARCHAR(200),
            last_modified TIMESTAMP,
            source_url TEXT,
            rows_loaded INTEGER DEFAULT 0,
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) DEFAULT 'pending',
            error_message TEXT,
            UNIQUE(dataset, data_key)
        )
    """
}
