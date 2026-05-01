-- CI (Compensation and Working Conditions) Denormalized Table Schema

CREATE TABLE IF NOT EXISTS ci_data (
    series_id VARCHAR(20) NOT NULL,
    year INTEGER NOT NULL,
    period VARCHAR(3) NOT NULL,
    period_name VARCHAR(50),
    value NUMERIC(14,2),
    owner_code VARCHAR(10),
    owner_name VARCHAR(100),
    industry_code VARCHAR(10),
    industry_name VARCHAR(200),
    occupation_code VARCHAR(10),
    occupation_name VARCHAR(200),
    area_code VARCHAR(10),
    area_name VARCHAR(200),
    estimate_code VARCHAR(10),
    estimate_name VARCHAR(200),
    periodicity_code VARCHAR(10),
    periodicity_name VARCHAR(50),
    seasonal_code VARCHAR(10),
    seasonal_name VARCHAR(30),
    footnote_codes VARCHAR(10),
    PRIMARY KEY (series_id, year, period)
);

CREATE INDEX IF NOT EXISTS idx_ci_year ON ci_data(year);
CREATE INDEX IF NOT EXISTS idx_ci_industry ON ci_data(industry_code);
CREATE INDEX IF NOT EXISTS idx_ci_occupation ON ci_data(occupation_code);
CREATE INDEX IF NOT EXISTS idx_ci_estimate ON ci_data(estimate_code);

COMMENT ON TABLE ci_data IS 'Compensation and Working Conditions - denormalized with all dimension lookups joined';
