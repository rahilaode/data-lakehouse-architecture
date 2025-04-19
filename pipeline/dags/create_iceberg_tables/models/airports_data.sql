CREATE TABLE IF NOT EXISTS demo.airports_data (
    airport_code STRING,
    airport_name STRING,
    city STRING,
    coordinates STRING,
    timezone STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (airport_code);