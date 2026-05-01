-- SM (State and Metropolitan Area Employment) Denormalized Table Schema

CREATE TABLE IF NOT EXISTS sm_data (
    series_id VARCHAR(22) NOT NULL,
    year INTEGER NOT NULL,
    period VARCHAR(3) NOT NULL,
    period_name VARCHAR(50),
    value NUMERIC(14,2),
    state_code VARCHAR(2),
    state_name VARCHAR(100),
    area_code VARCHAR(10),
    area_name VARCHAR(200),
    supersector_code VARCHAR(2),
    supersector_name VARCHAR(100),
    industry_code VARCHAR(10),
    industry_name VARCHAR(200),
    data_type_code VARCHAR(2),
    data_type_name VARCHAR(100),
    seasonal_code VARCHAR(1),
    seasonal_name VARCHAR(30),
    footnote_codes VARCHAR(10),
    PRIMARY KEY (series_id, year, period)
);

CREATE INDEX IF NOT EXISTS idx_sm_year ON sm_data(year);
CREATE INDEX IF NOT EXISTS idx_sm_state ON sm_data(state_code);
CREATE INDEX IF NOT EXISTS idx_sm_industry ON sm_data(industry_code);
CREATE INDEX IF NOT EXISTS idx_sm_supersector ON sm_data(supersector_code);

COMMENT ON TABLE sm_data IS 'State and Metropolitan Area Employment - denormalized with all dimension lookups joined';
