FROM apache/airflow:2.10.2

# Copy requirements
COPY requirements.txt /requirements.txt
COPY ./variables_and_connections /init/variables_and_connections
RUN pip install --no-cache-dir -r /requirements.txt

USER root

# Install Java (dibutuhkan untuk Spark & Hadoop)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download Hadoop AWS, AWS SDK, dan Iceberg Spark Runtime JARs
RUN mkdir -p /opt/spark/jars/ && \
    curl -o /opt/spark/jars/hadoop-aws-3.3.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar && \
    curl -o /opt/spark/jars/hadoop-common-3.3.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.1/hadoop-common-3.3.1.jar && \
    curl -o /opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.2/iceberg-spark-runtime-3.4_2.12-1.4.2.jar

# Install PostgreSQL JDBC driver (untuk Hive Metastore)
RUN curl -o /opt/spark/jars/postgresql-42.2.23.jar \
    https://jdbc.postgresql.org/download/postgresql-42.2.23.jar

# Set user ke airflow
USER airflow

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$JAVA_HOME/bin

# Konfigurasi Spark untuk menggunakan Hive Metastore dan Iceberg
ENV SPARK_CLASSPATH=/opt/spark/jars/*
ENV SPARK_PACKAGES=org.apache.hadoop:hadoop-aws:3.3.1,\
com.amazonaws:aws-java-sdk-bundle:1.11.900,\
org.apache.hadoop:hadoop-common:3.3.1,\
org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2

# Buat direktori Hadoop dengan permission yang benar
USER root
RUN mkdir -p $HADOOP_HOME/etc/hadoop && chown -R airflow:root $HADOOP_HOME && chmod -R 775 $HADOOP_HOME
USER airflow