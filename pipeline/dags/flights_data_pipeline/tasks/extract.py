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
    .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/iceberg-sample-data/") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Read from PostgreSQL
query = "(SELECT * FROM aircrafts_data) as data"
df = spark.read.jdbc(
    url="jdbc:postgresql://flights_db:5432/demo",
    table=query,
    properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
)

df.show()

# Create Iceberg table (if it doesn't exist)
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.aircrafts_data (
        aircraft_code STRING,
        model STRING,
        range INT
    )
    USING iceberg
""")

# Insert to Iceberg table (append mode)
df.writeTo("demo.aircrafts_data").append()