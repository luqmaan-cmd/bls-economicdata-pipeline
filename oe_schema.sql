-- OE (Occupational Employment and Wage Statistics) Denormalized Table Schema

CREATE TABLE IF NOT EXISTS oe_data (
    series_id VARCHAR(35) NOT NULL,
    year INTEGER NOT NULL,
    period VARCHAR(10) NOT NULL,
    period_name VARCHAR(50),
    value NUMERIC(14,2),
    areatype_code VARCHAR(10),
    areatype_name VARCHAR(100),
    area_code VARCHAR(20),
    area_name VARCHAR(200),
    industry_code VARCHAR(20),
    industry_name VARCHAR(200),
    occupation_code VARCHAR(20),
    occupation_name VARCHAR(200),
    datatype_code VARCHAR(10),
    datatype_name VARCHAR(100),
    sector_code VARCHAR(20),
    sector_name VARCHAR(100),
    seasonal_code VARCHAR(10),
    seasonal_name VARCHAR(30),
    footnote_codes VARCHAR(250),
    PRIMARY KEY (series_id, year, period)
);

CREATE INDEX IF NOT EXISTS idx_oe_year ON oe_data(year);
CREATE INDEX IF NOT EXISTS idx_oe_area ON oe_data(area_code);
CREATE INDEX IF NOT EXISTS idx_oe_industry ON oe_data(industry_code);
CREATE INDEX IF NOT EXISTS idx_oe_occupation ON oe_data(occupation_code);

COMMENT ON TABLE oe_data IS 'Occupational Employment and Wage Statistics - denormalized with all dimension lookups joined';
