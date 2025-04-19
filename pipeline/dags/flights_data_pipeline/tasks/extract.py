from pyspark.sql import SparkSession
from airflow.exceptions import AirflowSkipException, AirflowException
import sys

BASE_PATH = "/opt/airflow/dags"
AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "minio123"

def extract_load(table_name, incremental, date):
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName(f"Insert to Iceberg - {table_name}") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.2.23.jar") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.demo.type", "hive") \
            .config("spark.sql.catalog.demo.uri", "thrift://34.227.73.6:9083") \
            .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/staging/") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()

        if incremental:
            query = f"(SELECT * FROM bookings.{table_name} WHERE updated_at::DATE = '{date}'::DATE - INTERVAL '1 DAY')) as data"
        else:
            query = f"(SELECT * FROM bookings.{table_name}) as data"

        df = spark.read.jdbc(
            url="jdbc:postgresql://34.227.73.6:5432/demo",
            table=query,
            properties={
                "user": "postgres",
                "password": "postgres",
                "driver": "org.postgresql.Driver"
            }
        )

        print("test")
        print(df.show())
        # Overwrite specific partitions based on i
        df.writeTo(f"demo.default.{table_name}").overwritePartitions()

        spark.stop()

    except Exception as e:
        raise AirflowException(f"Error when extracting {table_name}: {str(e)}")


if __name__ == "__main__":
    """
    Main entry point for the script. Extracts data from Flights database based on command line arguments.
    """
    if len(sys.argv) != 4:
        sys.exit(-1)

    table_name = sys.argv[1]    
    incremental = sys.argv[2].lower() == 'true'
    date = sys.argv[3]

    extract_load(table_name, incremental, date)