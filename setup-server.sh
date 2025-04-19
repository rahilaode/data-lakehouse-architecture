# Start data storage (minio)
docker compose -f ./setups/minio/docker-compose.yml down -v
docker compose -f ./setups/minio/docker-compose.yml up --build --detach

# Start spark
docker compose -f ./setups/spark/docker-compose.yml down -v
docker compose -f ./setups/spark/docker-compose.yml up --build --detach

# Hive metastore
docker compose -f ./setups/hive_metastore/docker-compose.yml down -v
docker compose -f ./setups/hive_metastore/docker-compose.yml up --build --detach