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