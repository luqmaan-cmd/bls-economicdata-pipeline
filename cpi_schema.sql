-- CPI Data Table (denormalized - single table with all information)

CREATE TABLE IF NOT EXISTS cpi_data (
    id SERIAL PRIMARY KEY,
    series_id VARCHAR(20),
    year INTEGER,
    period VARCHAR(5),
    period_name VARCHAR(50),
    value NUMERIC(15, 4),
    area_code VARCHAR(10),
    area_name VARCHAR(255),
    item_code VARCHAR(20),
    item_name VARCHAR(500),
    seasonal_code VARCHAR(1),
    seasonal_text VARCHAR(50)
);

CREATE INDEX idx_cpi_series ON cpi_data(series_id);
CREATE INDEX idx_cpi_year ON cpi_data(year);
CREATE INDEX idx_cpi_period ON cpi_data(period);
CREATE INDEX idx_cpi_area ON cpi_data(area_code);
CREATE INDEX idx_cpi_item ON cpi_data(item_code);
CREATE INDEX idx_cpi_area_year ON cpi_data(area_code, year);
