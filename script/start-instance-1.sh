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

# Insert connection & variables
docker exec -it airflow-webserver airflow connections import /init/variables_and_connections/airflow_connections_init.yaml
docker exec -it airflow-webserver airflow variables import -a overwrite /init/variables_and_connections/airflow_variables_init.json

# Delete if monitoring data exists
sudo rm ~/drskl-research/docker_stats.csv

# Start monitoring resources usage
python3 ~/drskl-research/evaluations/scrape_docker_stats/1.py &