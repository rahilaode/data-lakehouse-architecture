from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from helper.minio import get_clients
from datetime import timedelta
from airflow.models import Variable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

import pandas as pd

BASE_PATH = "/opt/airflow/dags"

# Initialize Spark session
spark = SparkSession.builder \
    .appName(f"Extract table - aircrafts_data") \
    .getOrCreate()

# Define query and object name
query = f"(SELECT * FROM aircrafts_data) as data"
object_name = f'/flights-db/aircrafts_data'

# Read data from database
df = spark.read.jdbc(
    url="jdbc:postgresql://flights_db:5432/demo",
    table=query,
    properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
)

print(df.show())

# Stop Spark session
spark.stop()