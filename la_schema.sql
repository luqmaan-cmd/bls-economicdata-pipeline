-- LA (Local Area Unemployment Statistics) Denormalized Table Schema

CREATE TABLE IF NOT EXISTS la_data (
    series_id VARCHAR(20) NOT NULL,
    year INTEGER NOT NULL,
    period VARCHAR(3) NOT NULL,
    period_name VARCHAR(50),
    value NUMERIC(14,2),
    area_type_code VARCHAR(2),
    area_type_name VARCHAR(100),
    area_code VARCHAR(20),
    area_name VARCHAR(200),
    measure_code VARCHAR(2),
    measure_name VARCHAR(100),
    seasonal_code VARCHAR(1),
    seasonal_name VARCHAR(30),
    state_code VARCHAR(2),
    state_name VARCHAR(100),
    footnote_codes VARCHAR(10),
    PRIMARY KEY (series_id, year, period)
);

CREATE INDEX IF NOT EXISTS idx_la_year ON la_data(year);
CREATE INDEX IF NOT EXISTS idx_la_area ON la_data(area_code);
CREATE INDEX IF NOT EXISTS idx_la_measure ON la_data(measure_code);
CREATE INDEX IF NOT EXISTS idx_la_state ON la_data(state_code);

COMMENT ON TABLE la_data IS 'Local Area Unemployment Statistics - denormalized with all dimension lookups joined';
