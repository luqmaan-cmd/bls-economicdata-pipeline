-- PPI Denormalized Table Schema
-- Producer Price Index data from BLS

CREATE TABLE IF NOT EXISTS ppi_data (
    series_id VARCHAR(20) NOT NULL,
    year INTEGER NOT NULL,
    period VARCHAR(3) NOT NULL,
    period_name VARCHAR(20),
    value NUMERIC(12,4),
    measure_code VARCHAR(2),
    measure_name VARCHAR(100),
    sector_code VARCHAR(10),
    sector_name VARCHAR(200),
    class_code VARCHAR(10),
    class_name VARCHAR(200),
    duration_code VARCHAR(2),
    duration_name VARCHAR(50),
    seasonal_code VARCHAR(1),
    seasonal_name VARCHAR(20),
    footnote_codes VARCHAR(10),
    PRIMARY KEY (series_id, year, period)
);

CREATE INDEX IF NOT EXISTS idx_ppi_year ON ppi_data(year);
CREATE INDEX IF NOT EXISTS idx_ppi_measure ON ppi_data(measure_code);
CREATE INDEX IF NOT EXISTS idx_ppi_sector ON ppi_data(sector_code);
CREATE INDEX IF NOT EXISTS idx_ppi_class ON ppi_data(class_code);

COMMENT ON TABLE ppi_data IS 'Producer Price Index - denormalized with all dimension lookups joined';
COMMENT ON COLUMN ppi_data.series_id IS 'BLS series identifier';
COMMENT ON COLUMN ppi_data.measure_code IS 'Measure type code (e.g., 01=Finished Goods)';
COMMENT ON COLUMN ppi_data.sector_code IS 'Industry sector code';
COMMENT ON COLUMN ppi_data.class_code IS 'Product class code';
COMMENT ON COLUMN ppi_data.duration_code IS 'Duration code (e.g., monthly, annual)';
COMMENT ON COLUMN ppi_data.seasonal_code IS 'S=Seasonally Adjusted, U=Unadjusted';
