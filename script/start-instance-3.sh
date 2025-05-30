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

# Delete if monitoring data exists
sudo rm ~/drskl-research/docker_stats.csv

# Start monitoring resources usage
python3 ~/drskl-research/evaluations/scrape_docker_stats/1.py &

sleep 10

# Submit flink job
docker exec -it jobmanager bash -c "/opt/flink/bin/sql-client.sh -f /opt/flink/usr_jobs/kafka-to-iceberg.sql" 