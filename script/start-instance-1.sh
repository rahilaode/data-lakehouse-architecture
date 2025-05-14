# Create networks
docker network create riset-lakehouse-networks

# Start data source
docker compose -f ~/drskl-research/setups/data_sources/flights_db/docker-compose.yml down -v
docker compose -f ~/drskl-research/setups/data_sources/flights_db/docker-compose.yml up --build --detach

# Start spark
docker compose -f ~/drskl-research/setups/spark/docker-compose.yml down -v
docker compose -f ~/drskl-research/setups/spark/docker-compose.yml up --build --detach

# Start airflow
docker compose -f ~/drskl-research/setups/airflow/docker-compose.yml down -v
docker compose -f ~/drskl-research/setups/airflow/docker-compose.yml up --build --detach

# Insert variables
docker exec -it airflow-webserver airflow connections import /init/variables_and_connections/airflow_connections_init.yaml