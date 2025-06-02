from pyspark.sql import SparkSession
import sys

AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "minio123"

def create_tables(minio_ip, hive_metastore_ip):
    MINIO_IP = minio_ip
    HIVE_METASTORE_IP = hive_metastore_ip
    # Initialize SparkSession with Iceberg + Hive Metastore + MinIO (S3A)
    spark = SparkSession.builder \
        .appName("Create Iceberg Tables") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "hive") \
        .config("spark.sql.catalog.demo.uri", f"thrift://{HIVE_METASTORE_IP}:9083") \
        .config("spark.sql.catalog.demo.warehouse", "s3a://hive/warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_IP}:9000") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


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

        # -- Tabel ticket_flights transformed
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.default.ticket_flights_transformed (
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


    # Tutup SparkSession
    spark.stop()

if __name__ == "__main__":
    """
    Main entry point for the script.
    """
    if len(sys.argv) != 3:
        sys.exit(-1)

    MINIO_IP = sys.argv[1]
    HIVE_METASTORE_IP = sys.argv[2]

    create_tables(MINIO_IP, HIVE_METASTORE_IP)