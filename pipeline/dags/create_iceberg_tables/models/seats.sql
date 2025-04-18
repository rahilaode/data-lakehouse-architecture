CREATE TABLE IF NOT EXISTS demo.seats (
    aircraft_code STRING,
    seat_no STRING,
    fare_conditions STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (aircraft_code, seat_no);