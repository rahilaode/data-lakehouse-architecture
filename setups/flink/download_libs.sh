#!/bin/bash

mkdir -p ./lib

curl -o ./lib/flink-connector-jdbc-3.1.2-1.18.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar
curl -o ./lib/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
curl -o ./lib/flink-sql-connector-kafka-3.1.0-1.18.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
# Download Iceberg Flink Runtime JAR for Flink 1.18
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.4.3/iceberg-flink-runtime-1.18-1.4.3.jar -P /opt/flink/opt/
echo "Libs downloaded successfully"