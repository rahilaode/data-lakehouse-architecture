from airflow.decorators import task_group, dag
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models.variable import Variable

# Constants
DATE = '{{ ds }}'
MINIO_IP = Variable.get('MINIO_IP')
HIVE_METASTORE_IP = Variable.get('HIVE_METASTORE_IP')
LIMIT_DATA = Variable.get('LIMIT_DATA')


# Define the list of JAR files required for Spark
jar_list = [
    '/opt/spark/jars/hadoop-aws-3.3.1.jar',
    '/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar',
    '/opt/spark/jars/postgresql-42.2.23.jar',
    '/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar'
]

# Define Spark configuration
spark_conf = {
    'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3',
    'spark.hadoop.fs.s3a.access.key': 'minio',
    'spark.hadoop.fs.s3a.secret.key': 'minio123',
    'spark.hadoop.fs.s3a.endpoint': f'http://{MINIO_IP}:9000',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    # 'spark.dynamicAllocation.enabled': 'true',
    # 'spark.dynamicAllocation.maxExecutors': '3',
    # 'spark.dynamicAllocation.minExecutors': '1',
    # 'spark.dynamicAllocation.initialExecutors': '1',
    'spark.executor.memory': '8g',  
    'spark.executor.cores': '2',     
    'spark.driver.memory': '8g'    
    # 'spark.scheduler.mode': 'FAIR'
}

table_to_extract = [
    "ticket_flights"
]

# table_to_extract = [
#     "aircrafts_data",
#     "airports_data",
#     "bookings",
#     "flights",
#     "seats",
#     "tickets",
#     "ticket_flights",
#     "boarding_passes"
# ]

incremental = False
@dag(
    dag_id='flights_data_pipeline',
    schedule='@daily',
    start_date=days_ago(1),
    tags=['flights_data_pipeline'],
)
def flights_data_pipeline():
    # TASK GROUP TO EXTRACT DATA FROM FLIGHTS DB
    @task_group()
    def extract_load():
        previous_task = None

        for table_name in table_to_extract:
            current_task = SparkSubmitOperator(
                task_id=f"extract-load__{table_name}",
                conn_id="spark-conn",
                application="/opt/airflow/dags/flights_data_pipeline/tasks/extract.py",
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed',
                application_args=[
                    f"{table_name}",
                    f"{incremental}",
                    f"{DATE}",
                    f"{MINIO_IP}",
                    f"{HIVE_METASTORE_IP}",
                    f"{LIMIT_DATA}"
                ]
            )
            # Set task dependencies
            if previous_task:
                previous_task >> current_task

            previous_task = current_task

    # TASK GROUP TO TRANSFORM DATA
    @task_group()
    def transform():
        SparkSubmitOperator(
            task_id="transform",
            conn_id="spark-conn",
            application="/opt/airflow/dags/flights_data_pipeline/tasks/transform.py",
            conf=spark_conf,
            jars=','.join(jar_list),
            trigger_rule='none_failed',
            application_args=[
                f"{MINIO_IP}",
                f"{HIVE_METASTORE_IP}"
            ]
        )

    extract_load() >> transform()

flights_data_pipeline()