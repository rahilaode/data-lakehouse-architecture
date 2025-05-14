# Create networks
docker network create riset-lakehouse-networks

# Start Kafka
docker compose -f ~/drskl-research/setups/kafka/docker-compose.yml down -v
docker compose -f ~/drskl-research/setups/kafka/docker-compose.yml up --build --detach

# Start Flink
docker compose -f ~/drskl-research/setups/flink/docker-compose.yml down -v
docker compose -f ~/drskl-research/setups/flink/docker-compose.yml up --build --detach

# Start data source
docker compose -f ~/drskl-research/setups/data_sources/stream/docker-compose.yml down -v
docker compose -f ~/drskl-research/setups/data_sources/stream/docker-compose.yml up --build --detach