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

# Insert variables
docker exec -it airflow-webserver airflow connections import /init/variables_and_connections/airflow_connections_init.yaml