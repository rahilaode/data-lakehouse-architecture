# Create networks
docker network create riset-lakehouse-networks

# Start data storage (minio)
docker compose -f ~/drskl-research/setups/minio/docker-compose.yml down -v
docker compose -f ~/drskl-research/setups/minio/docker-compose.yml up --build --detach

# Hive metastore
docker compose -f ~/drskl-research/setups/hive_metastore/docker-compose.yml down -v
docker compose -f ~/drskl-research/setups/hive_metastore/docker-compose.yml up --build --detach

# Start Trino
docker compose -f ~/drskl-research/setups/trino/docker-compose.yml down -v
docker compose -f ~/drskl-research/setups/trino/docker-compose.yml up --build --detach

# Start Metabase
# docker compose -f ~/drskl-research/setups/metabase/docker-compose.yaml down -v
# docker compose -f ~/drskl-research/setups/metabase/docker-compose.yaml up --build --detach

# Delete if monitoring data exists
sudo rm ~/drskl-research/docker_stats.csv

# Start monitoring resources usage
python3 ~/drskl-research/evaluations/scrape_docker_stats/1.py &