CREATE TABLE IF NOT EXISTS demo.bookings (
    book_ref STRING,
    book_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (book_ref);