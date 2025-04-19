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