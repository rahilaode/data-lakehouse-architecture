FROM bitnami/spark:latest

USER root


# Install Java 17 (kalau belum tersedia di base image)
RUN apt-get update && apt-get install -y openjdk-17-jdk-headless curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Hadoop AWS + SDK
RUN curl -o /opt/bitnami/spark/jars/hadoop-aws-3.3.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.901.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar && \
    curl -o /opt/bitnami/spark/jars/hadoop-common-3.3.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.1/hadoop-common-3.3.1.jar

# PostgreSQL JDBC
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.2.23.jar \
    https://jdbc.postgresql.org/download/postgresql-42.2.23.jar

# Iceberg Spark Runtime for Spark 3.5
RUN curl -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

USER 1001