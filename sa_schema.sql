-- SA (State and Area Employment) Denormalized Table Schema

CREATE TABLE IF NOT EXISTS sa_data (
    series_id VARCHAR(20) NOT NULL,
    year INTEGER NOT NULL,
    period VARCHAR(3) NOT NULL,
    value NUMERIC(14,2),
    state_code VARCHAR(2),
    state_name VARCHAR(100),
    area_code VARCHAR(10),
    area_name VARCHAR(200),
    industry_code VARCHAR(10),
    industry_name VARCHAR(200),
    detail_code VARCHAR(2),
    detail_name VARCHAR(100),
    data_type_code VARCHAR(2),
    data_type_name VARCHAR(100),
    seasonal_code VARCHAR(1),
    seasonal_name VARCHAR(30),
    footnote_codes VARCHAR(10),
    PRIMARY KEY (series_id, year, period)
);

CREATE INDEX IF NOT EXISTS idx_sa_year ON sa_data(year);
CREATE INDEX IF NOT EXISTS idx_sa_state ON sa_data(state_code);
CREATE INDEX IF NOT EXISTS idx_sa_industry ON sa_data(industry_code);
CREATE INDEX IF NOT EXISTS idx_sa_data_type ON sa_data(data_type_code);

COMMENT ON TABLE sa_data IS 'State and Area Employment - denormalized with all dimension lookups joined';
