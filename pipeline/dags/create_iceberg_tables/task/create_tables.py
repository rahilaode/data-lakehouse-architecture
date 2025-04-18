from pyspark.sql import SparkSession
import logging

BASE_PATH = "/opt/airflow/dags"
AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "minio123"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Insert to Iceberg - aircrafts_data") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.23.jar") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "hadoop") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/staging/") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()


# Create Iceberg table
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.aircrafts_data (
        aircraft_code STRING,
        model STRING,
        range INT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (aircraft_code);
""")

spark.sql("""
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
""")

spark.sql("""
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
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.bookings (
        book_ref STRING,
        book_date TIMESTAMP,
        total_amount DECIMAL(10,2),
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (book_ref);
""")

spark.sql("""
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
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.seats (
        aircraft_code STRING,
        seat_no STRING,
        fare_conditions STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (aircraft_code, seat_no);
""")

spark.sql("""
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
""")

spark.sql("""
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
""")

spark.stop()