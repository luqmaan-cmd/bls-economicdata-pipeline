-- CE (Current Employment Statistics) Denormalized Table Schema
-- Employment data from BLS

CREATE TABLE IF NOT EXISTS ce_data (
    series_id VARCHAR(20) NOT NULL,
    year INTEGER NOT NULL,
    period VARCHAR(3) NOT NULL,
    period_name VARCHAR(20),
    value NUMERIC(14,2),
    supersector_code VARCHAR(5),
    supersector_name VARCHAR(100),
    industry_code VARCHAR(10),
    industry_name VARCHAR(200),
    datatype_code VARCHAR(5),
    datatype_name VARCHAR(100),
    seasonal_code VARCHAR(1),
    seasonal_name VARCHAR(30),
    footnote_codes VARCHAR(10),
    PRIMARY KEY (series_id, year, period)
);

CREATE INDEX IF NOT EXISTS idx_ce_year ON ce_data(year);
CREATE INDEX IF NOT EXISTS idx_ce_supersector ON ce_data(supersector_code);
CREATE INDEX IF NOT EXISTS idx_ce_industry ON ce_data(industry_code);
CREATE INDEX IF NOT EXISTS idx_ce_datatype ON ce_data(datatype_code);

COMMENT ON TABLE ce_data IS 'Current Employment Statistics - denormalized with all dimension lookups joined';
COMMENT ON COLUMN ce_data.supersector_code IS 'Supersector code (e.g., 00=Total nonfarm)';
COMMENT ON COLUMN ce_data.industry_code IS 'NAICS-based industry code';
COMMENT ON COLUMN ce_data.datatype_code IS 'Data type code (e.g., 01=All Employees, 02=Average Weekly Hours)';
