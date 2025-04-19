CREATE TABLE IF NOT EXISTS demo.aircrafts_data (
    aircraft_code STRING,
    model STRING,
    range INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (aircraft_code);