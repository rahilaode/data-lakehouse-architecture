from pyspark.sql import SparkSession

AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "minio123"

# Initialize SparkSession with Iceberg + Hive Metastore + MinIO (S3A)
spark = SparkSession.builder \
    .appName("Create Iceberg Tables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "hive") \
    .config("spark.sql.catalog.demo.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://hive/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# -- Tabel aircrafts_data (dimensi) - no partition
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.aircrafts_data (
        aircraft_code STRING,
        model STRING,
        range INT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
""")

# -- Tabel airports_data (dimensi) - no partition
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.airports_data (
        airport_code STRING,
        airport_name STRING,
        city STRING,
        coordinates STRING,
        timezone STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
""")

# -- Tabel boarding_passes
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.boarding_passes (
        ticket_no STRING,
        flight_id INT,
        boarding_no INT,
        seat_no STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (month(created_at))
""")

# -- Tabel bookings
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.bookings (
        book_ref STRING,
        book_date TIMESTAMP,
        total_amount DECIMAL(10,2),
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (month(book_date))
""")

# -- Tabel flights
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.flights (
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
    PARTITIONED BY (month(scheduled_departure), arrival_airport)
""")

# -- Tabel seats
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.seats (
        aircraft_code STRING,
        seat_no STRING,
        fare_conditions STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (fare_conditions)
""")

# -- Tabel ticket_flights
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.ticket_flights (
        ticket_no STRING,
        flight_id INT,
        fare_conditions STRING,
        amount DECIMAL(10,2),
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (month(created_at), fare_conditions)
""")

# -- Tabel tickets
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.tickets (
        ticket_no STRING,
        book_ref STRING,
        passenger_id STRING,
        passenger_name STRING,
        contact_data STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (day(created_at))
""")

# -- dim_date
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.dim_date (
        date_id INT,
        date_actual DATE,
        day_suffix STRING,
        day_name STRING,
        day_of_year INT,
        week_of_month INT,
        week_of_year INT,
        week_of_year_iso STRING,
        month_actual INT,
        month_name STRING,
        month_name_abbreviated STRING,
        quarter_actual INT,
        quarter_name STRING,
        year_actual INT,
        first_day_of_week DATE,
        last_day_of_week DATE,
        first_day_of_month DATE,
        last_day_of_month DATE,
        first_day_of_quarter DATE,
        last_day_of_quarter DATE,
        first_day_of_year DATE,
        last_day_of_year DATE,
        mmyyyy STRING,
        mmddyyyy STRING,
        weekend_indr STRING
    )
    USING iceberg
""")

# -- dim_time
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.dim_time (
        time_id INT,
        time_actual STRING,
        hours_24 STRING,
        hours_12 STRING,
        hour_minutes STRING,
        day_minutes INT,
        day_time_name STRING,
        day_night STRING
    )
    USING iceberg
""")

# -- dim_passenger
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.dim_passenger (
        passenger_id STRING,
        passenger_nk STRING,
        passenger_name STRING,
        phone STRING,
        email STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (month(updated_at))
""")

# -- dim_aircraft
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.dim_aircraft (
        aircraft_id STRING,
        aircraft_nk STRING,
        model_en STRING,
        model_ru STRING,
        range INT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
""")

# -- dim_airport
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.dim_airport (
        airport_id STRING,
        airport_nk STRING,
        airport_name_en STRING,
        airport_name_ru STRING,
        city_en STRING,
        city_ru STRING,
        coordinates STRING,
        timezone STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
""")

# -- dim_seat
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.dim_seat (
        seat_id STRING,
        aircraft_id STRING,
        seat_no STRING,
        fare_conditions STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
""")

# -- fct_booking_ticket
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.fct_booking_ticket (
        booking_ticket_id STRING,
        book_nk STRING,
        ticket_no STRING,
        passenger_id STRING,
        flight_nk INT,
        flight_no STRING,
        book_date_local INT,
        book_date_utc INT,
        book_time_local INT,
        book_time_utc INT,
        scheduled_departure_date_local INT,
        scheduled_departure_date_utc INT,
        scheduled_departure_time_local INT,
        scheduled_departure_time_utc INT,
        scheduled_arrival_date_local INT,
        scheduled_arrival_date_utc INT,
        scheduled_arrival_time_local INT,
        scheduled_arrival_time_utc INT,
        departure_airport STRING,
        arrival_airport STRING,
        aircraft_code STRING,
        actual_departure_date_local INT,
        actual_departure_date_utc INT,
        actual_departure_time_local INT,
        actual_departure_time_utc INT,
        actual_arrival_date_local INT,
        actual_arrival_date_utc INT,
        actual_arrival_time_local INT,
        actual_arrival_time_utc INT,
        fare_conditions STRING,
        amount DECIMAL(10,2),
        total_amount DECIMAL(10,2),
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
""")

# -- fct_flight_activity
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.fct_flight_activity (
        flight_activity_id STRING,
        flight_nk STRING,
        flight_no STRING,
        scheduled_departure_date_local INT,
        scheduled_departure_date_utc INT,
        scheduled_departure_time_local INT,
        scheduled_departure_time_utc INT,
        scheduled_arrival_date_local INT,
        scheduled_arrival_date_utc INT,
        scheduled_arrival_time_local INT,
        scheduled_arrival_time_utc INT,
        departure_airport STRING,
        arrival_airport STRING,
        aircraft_code STRING,
        actual_departure_date_local INT,
        actual_departure_date_utc INT,
        actual_departure_time_local INT,
        actual_departure_time_utc INT,
        actual_arrival_date_local INT,
        actual_arrival_date_utc INT,
        actual_arrival_time_local INT,
        actual_arrival_time_utc INT,
        status STRING,
        delay_departure STRING,
        delay_arrival STRING,
        travel_time STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
""")

# -- fct_seat_occupied_daily
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.fct_seat_occupied_daily (
        seat_occupied_daily_id STRING,
        date_flight INT,
        flight_nk STRING,
        flight_no STRING,
        departure_airport STRING,
        arrival_airport STRING,
        aircraft_code STRING,
        status STRING,
        total_seat DECIMAL(10,2),
        seat_occupied DECIMAL(10,2),
        empty_seats DECIMAL(10,2),
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
""")

# -- fct_boarding_pass
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.fct_boarding_pass (
        boarding_pass_id STRING,
        ticket_no STRING,
        book_ref STRING,
        passenger_id STRING,
        flight_id INT,
        flight_no STRING,
        boarding_no INT,
        scheduled_departure_date_local INT,
        scheduled_departure_date_utc INT,
        scheduled_departure_time_local INT,
        scheduled_departure_time_utc INT,
        scheduled_arrival_date_local INT,
        scheduled_arrival_date_utc INT,
        scheduled_arrival_time_local INT,
        scheduled_arrival_time_utc INT,
        departure_airport STRING,
        arrival_airport STRING,
        aircraft_code STRING,
        status STRING,
        fare_conditions STRING,
        seat_no STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
""")
# Tutup SparkSession
spark.stop()