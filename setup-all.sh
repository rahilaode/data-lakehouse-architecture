# Create networks
docker network create riset-lakehouse-networks

# Start data source
docker compose -f ./setups/data_sources/flights_db/docker-compose.yml down -v
docker compose -f ./setups/data_sources/flights_db/docker-compose.yml up --build --detach

# Start data storage (minio)
docker compose -f ./setups/minio/docker-compose.yml down -v
docker compose -f ./setups/minio/docker-compose.yml up --build --detach

# Start spark
docker compose -f ./setups/spark/docker-compose.yml down -v
docker compose -f ./setups/spark/docker-compose.yml up --build --detach

# Hive metastore
docker compose -f ./setups/hive_metastore/docker-compose.yml down -v
docker compose -f ./setups/hive_metastore/docker-compose.yml up --build --detach
    
# Start airflow
docker compose -f ./setups/airflow/docker-compose.yml down -v
docker compose -f ./setups/airflow/docker-compose.yml up --build --detach

# Insert connection & variables
docker exec -it airflow-webserver airflow connections import /init/variables_and_connections/airflow_connections_init.yaml
docker exec -it airflow-webserver airflow variables import /init/variables_and_connections/airflow_variables_init.json

# Start Trino
docker compose -f ./setups/trino/docker-compose.yml down -v
docker compose -f ./setups/trino/docker-compose.yml up --build --detach

# # Start Kafka
docker compose -f ./setups/kafka/docker-compose.yml down -v
docker compose -f ./setups/kafka/docker-compose.yml up --build --detach

# Start Flink
docker compose -f ./setups/flink/docker-compose.yml down -v
docker compose -f ./setups/flink/docker-compose.yml up --build --detach

# Start streaming data source
docker compose -f ./setups/data_sources/stream/docker-compose.yml down -v
docker compose -f ./setups/data_sources/stream/docker-compose.yml up --build --detach

sleep 10

# Submit flink job
# docker exec -it jobmanager bash -c "/opt/flink/bin/sql-client.sh -f /opt/flink/usr_jobs/kafka-to-iceberg.sql" 