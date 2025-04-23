from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when
from pyspark.sql.types import MapType, StringType, StructType

BASE_PATH = "/opt/airflow/dags"
AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "minio123"

spark = SparkSession.builder \
    .appName("Transform DWH - Airlines Booking") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "hive") \
    .config("spark.sql.catalog.demo.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://hive/warehouse/") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# reduce noise
spark.sparkContext.setLogLevel("WARN")

tickets = spark.read.table("demo.default.tickets")

print(tickets.show())

# Skema untuk kolom contact_data (bertipe JSON string)
contact_schema = StructType() \
    .add("phone", StringType()) \
    .add("email", StringType())

# Transformasi DataFrame
tickets_transformed = tickets.withColumn(
    "contact_parsed", from_json(col("contact_data"), contact_schema)
).select(
    col("passenger_id").alias("passenger_id"),
    col("passenger_id").alias("passenger_nk"),
    col("passenger_name"),
    col("contact_parsed.phone").alias("phone"),
    col("contact_parsed.email").alias("email"),
    current_timestamp().alias("created_at"),
    current_timestamp().alias("updated_at")
)
print(tickets_transformed.show())
tickets_transformed.writeTo(f"demo.default.final_dim_passenger").overwritePartitions()

spark.stop()