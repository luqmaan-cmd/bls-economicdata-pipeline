-- JT (Job Openings and Labor Turnover Survey) Denormalized Table Schema

CREATE TABLE IF NOT EXISTS jt_data (
    series_id VARCHAR(22) NOT NULL,
    year INTEGER NOT NULL,
    period VARCHAR(3) NOT NULL,
    period_name VARCHAR(50),
    value NUMERIC(14,2),
    industry_code VARCHAR(10),
    industry_name VARCHAR(200),
    state_code VARCHAR(10),
    state_name VARCHAR(100),
    area_code VARCHAR(10),
    area_name VARCHAR(200),
    sizeclass_code VARCHAR(10),
    sizeclass_name VARCHAR(100),
    dataelement_code VARCHAR(10),
    dataelement_name VARCHAR(100),
    ratelevel_code VARCHAR(10),
    ratelevel_name VARCHAR(50),
    seasonal_code VARCHAR(10),
    seasonal_name VARCHAR(30),
    footnote_codes VARCHAR(10),
    PRIMARY KEY (series_id, year, period)
);

CREATE INDEX IF NOT EXISTS idx_jt_year ON jt_data(year);
CREATE INDEX IF NOT EXISTS idx_jt_industry ON jt_data(industry_code);
CREATE INDEX IF NOT EXISTS idx_jt_state ON jt_data(state_code);
CREATE INDEX IF NOT EXISTS idx_jt_dataelement ON jt_data(dataelement_code);

COMMENT ON TABLE jt_data IS 'Job Openings and Labor Turnover Survey - denormalized with all dimension lookups joined';
