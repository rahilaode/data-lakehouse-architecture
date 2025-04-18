CREATE TABLE IF NOT EXISTS demo.flights (
    flight_id INT,
    flight_no STRING,
    scheduled_departure TIMESTAMP,
    scheduled_arrival TIMESTAMP,
    departure_airport STRING,
    arrival_airport STRING,
    status STRING,
    aircraft_code STRING,
    actual_departure TIMESTAMP,
    actual_arrival TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (flight_id);