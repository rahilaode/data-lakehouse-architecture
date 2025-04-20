from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when
from pyspark.sql.types import MapType, StringType

spark = SparkSession.builder \
    .appName("Transform DWH - Airlines Booking") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "hive") \
    .config("spark.sql.catalog.demo.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://hive/warehouse/") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# reduce noise
spark.sparkContext.setLogLevel("WARN")

tickets = spark.read.table("demo.default.tickets")

tickets_clean = tickets.withColumn(
    "contact_map",
    when(
        col("contact_data").isNotNull(),
        from_json("contact_data", MapType(StringType(), StringType()))
    )
)

dim_passenger = tickets_clean.select(
    col("passenger_id").alias("passenger_nk"),
    col("passenger_name"),
    col("contact_map")["phone"].alias("phone"),
    col("contact_map")["email"].alias("email")
).dropDuplicates(["passenger_nk"]) \
 .withColumn("passenger_id", col("passenger_nk")) \
 .withColumn("created_at", current_timestamp()) \
 .withColumn("updated_at", current_timestamp()) \
 .select("passenger_id", "passenger_nk", "passenger_name", "phone", "email", "created_at", "updated_at")

# Hindari OOM
dim_passenger.limit(5).show(truncate=False)

# Gunakan overwriteFiles jika tidak ada partition
dim_passenger.writeTo("demo.default.final_dim_passenger").overwriteFiles()

spark.stop()
