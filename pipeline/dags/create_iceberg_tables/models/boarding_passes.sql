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