from pyspark.sql import SparkSession
from airflow.exceptions import AirflowException
import sys
import time

BASE_PATH = "/opt/airflow/dags"
AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "minio123"

def extract_load(table_name, incremental, date, minio_ip, hive_metastore_ip):
    try:
        MINIO_IP = minio_ip
        HIVE_METASTORE_IP = hive_metastore_ip
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName(f"Insert to Iceberg - {table_name}") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.2.23.jar") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.demo.type", "hive") \
            .config("spark.sql.catalog.demo.uri", f"thrift://{HIVE_METASTORE_IP}:9083") \
            .config("spark.sql.catalog.demo.warehouse", "s3a://hive/warehouse/") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_IP}:9000") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()

        if incremental:
            query = f"(SELECT * FROM bookings.{table_name} WHERE updated_at::DATE = '{date}'::DATE - INTERVAL '1 DAY')) as data"
        else:
            query = f"(SELECT * FROM bookings.{table_name}) as data"

        df = spark.read.jdbc(
            url="jdbc:postgresql://flights_db:5432/demo",
            table=query,
            properties={
                "user": "postgres",
                "password": "postgres",
                "driver": "org.postgresql.Driver"
            }
        )
        
        # Measure time spark write to iceberg tables
        start = time.time()
        df.writeTo(f"demo.default.{table_name}").overwritePartitions()
        end = time.time()
        print(f"Write durations for {table_name} table in landing zone: {end - start} seconds")

        spark.stop()

    except Exception as e:
        raise AirflowException(f"Error when extracting {table_name}: {str(e)}")

if __name__ == "__main__":
    """
    Main entry point for the script. Extracts data from Flights database based on command line arguments.
    """
    if len(sys.argv) != 6:
        sys.exit(-1)

    table_name = sys.argv[1]
    incremental = sys.argv[2].lower() == 'true'
    date = sys.argv[3]
    minio_ip = sys.argv[4]
    hive_metastore_ip = sys.argv[5]

    extract_load(table_name, incremental, date, minio_ip, hive_metastore_ip)