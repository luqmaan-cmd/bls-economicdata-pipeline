-- MP (Mass Layoff Statistics) Denormalized Table Schema

CREATE TABLE IF NOT EXISTS mp_data (
    series_id VARCHAR(20) NOT NULL,
    year INTEGER NOT NULL,
    period VARCHAR(3) NOT NULL,
    value NUMERIC(14,2),
    sector_code VARCHAR(10),
    sector_name VARCHAR(200),
    measure_code VARCHAR(10),
    measure_name VARCHAR(100),
    duration_code VARCHAR(10),
    duration_name VARCHAR(100),
    seasonal_code VARCHAR(10),
    seasonal_name VARCHAR(30),
    footnote_codes VARCHAR(10),
    PRIMARY KEY (series_id, year, period)
);

CREATE INDEX IF NOT EXISTS idx_mp_year ON mp_data(year);
CREATE INDEX IF NOT EXISTS idx_mp_sector ON mp_data(sector_code);
CREATE INDEX IF NOT EXISTS idx_mp_measure ON mp_data(measure_code);

COMMENT ON TABLE mp_data IS 'Mass Layoff Statistics - denormalized with all dimension lookups joined';
