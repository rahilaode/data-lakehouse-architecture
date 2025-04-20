FROM apache/airflow:2.10.2

# -------------------------
# 📦 COPY requirements
# -------------------------
COPY requirements.txt /requirements.txt
COPY ./variables_and_connections /init/variables_and_connections

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# prepare dbt virtual environment
COPY dbt-requirements.txt ./
RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate

# -------------------------
# ☕ INSTALL JAVA (required for Spark & Hadoop)
# -------------------------
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# -------------------------
# 🔗 DOWNLOAD JARs (Hadoop AWS, PostgreSQL JDBC, Iceberg Runtime)
# -------------------------
RUN mkdir -p /opt/spark/jars

# Hadoop AWS + SDK
RUN curl -o /opt/spark/jars/hadoop-aws-3.3.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar && \
    curl -o /opt/spark/jars/hadoop-common-3.3.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.1/hadoop-common-3.3.1.jar

# PostgreSQL JDBC
RUN curl -o /opt/spark/jars/postgresql-42.2.23.jar \
    https://jdbc.postgresql.org/download/postgresql-42.2.23.jar

# Iceberg Spark Runtime for Spark 3.5
RUN curl -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

# -------------------------
# 📁 PERMISSION untuk Hadoop Directory
# -------------------------
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

RUN mkdir -p $HADOOP_HOME/etc/hadoop && \
    chown -R airflow:root $HADOOP_HOME && \
    chmod -R 775 $HADOOP_HOME

# -------------------------
# 🔙 Switch back to Airflow user
# -------------------------
USER airflow

# Optional: Environment JAR path
ENV SPARK_CLASSPATH=/opt/spark/jars/*
