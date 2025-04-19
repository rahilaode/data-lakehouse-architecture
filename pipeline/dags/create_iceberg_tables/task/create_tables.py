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
    .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/staging/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# # Optional: Buat namespace (seperti schema) jika belum ada
# spark.sql("CREATE NAMESPACE IF NOT EXISTS demo")

# Buat semua tabel di catalog `demo` dengan penamaan penuh
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.aircrafts_data (
        aircraft_code STRING,
        model STRING,
        range INT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (aircraft_code)
""")

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
    PARTITIONED BY (airport_code)
""")

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
    PARTITIONED BY (ticket_no, flight_id)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.bookings (
        book_ref STRING,
        book_date TIMESTAMP,
        total_amount DECIMAL(10,2),
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (book_ref)
""")

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
    PARTITIONED BY (flight_id)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.default.seats (
        aircraft_code STRING,
        seat_no STRING,
        fare_conditions STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (aircraft_code, seat_no)
""")

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
    PARTITIONED BY (ticket_no, flight_id)
""")

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
    PARTITIONED BY (ticket_no)
""")

# Tutup SparkSession
spark.stop()
