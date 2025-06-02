from airflow.decorators import task_group, dag
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# Define the list of JAR files required for Spark
jar_list = [
    '/opt/spark/jars/hadoop-aws-3.3.1.jar',
    '/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar',
    '/opt/spark/jars/postgresql-42.2.23.jar',
    '/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar'
]

# Import variables
MINIO_IP = Variable.get('MINIO_IP')
HIVE_METASTORE_IP = Variable.get('HIVE_METASTORE_IP')

# Define Spark configuration
spark_conf = {
    'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3',
    'spark.hadoop.fs.s3a.access.key': 'minio',
    'spark.hadoop.fs.s3a.secret.key': 'minio123',
    'spark.hadoop.fs.s3a.endpoint': f'http://{MINIO_IP}:9000',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
    # 'spark.dynamicAllocation.enabled': 'true',
    # 'spark.dynamicAllocation.maxExecutors': '3',
    # 'spark.dynamicAllocation.minExecutors': '1',
    # 'spark.dynamicAllocation.initialExecutors': '1',
    # 'spark.executor.memory': '4g',  # Define RAM per executor
    # 'spark.executor.cores': '2',  # Define cores per executor
    # 'spark.scheduler.mode': 'FAIR'
}

@dag(
    dag_id='create_iceberg_tables',
    schedule='@daily',
    start_date=days_ago(1),
    tags=['create_iceberg_tables'],
)
def create_iceberg_tables():
    create_raw_tables = SparkSubmitOperator(
        task_id='create_raw_tables',
        conn_id="spark-conn",
        application="/opt/airflow/dags/create_iceberg_tables/task/create_tables.py",
        conf=spark_conf,
        jars=','.join(jar_list),
        trigger_rule='none_failed',
        application_args=[
            f"{MINIO_IP}",
            f"{HIVE_METASTORE_IP}"
        ]
    )

    # create_warehouse_tables = SparkSubmitOperator(
    #     task_id='create_warehouse_tables',
    #     conn_id="spark-conn",
    #     application="/opt/airflow/dags/create_iceberg_tables/task/create_warehouse_tables.py",
    #     conf=spark_conf,
    #     jars=','.join(jar_list),
    #     trigger_rule='none_failed',
    # )
    
    create_raw_tables 
    
create_iceberg_tables()