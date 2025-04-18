CREATE TABLE IF NOT EXISTS demo.aircrafts_data (
    aircraft_code STRING,
    model STRING,
    range INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (aircraft_code);

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

CREATE TABLE IF NOT EXISTS demo.boarding_passes (
    ticket_no STRING,
    flight_id INT,
    boarding_no INT,
    seat_no STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (ticket_no, flight_id);

CREATE TABLE IF NOT EXISTS demo.bookings (
    book_ref STRING,
    book_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (book_ref);

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

CREATE TABLE IF NOT EXISTS demo.seats (
    aircraft_code STRING,
    seat_no STRING,
    fare_conditions STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (aircraft_code, seat_no);

CREATE TABLE IF NOT EXISTS demo.ticket_flights (
    ticket_no STRING,
    flight_id INT,
    fare_conditions STRING,
    amount DECIMAL(10,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (ticket_no, flight_id);

CREATE TABLE IF NOT EXISTS demo.tickets (
    ticket_no STRING,
    book_ref STRING,
    passenger_id STRING,
    passenger_name STRING,
    contact_data STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (ticket_no);