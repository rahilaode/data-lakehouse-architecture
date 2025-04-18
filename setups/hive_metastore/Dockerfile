FROM apache/hive:4.0.1

# Install curl untuk mengunduh JDBC driver
USER root
RUN apt-get update && apt-get install -y curl

# Install PostgreSQL JDBC Driver
RUN curl -o /opt/hive/lib/postgresql-42.2.23.jar \
    https://jdbc.postgresql.org/download/postgresql-42.2.23.jar

# Set environment variables
ENV HIVE_AUX_JARS_PATH=/opt/hive/lib/postgresql-42.2.23.jar

# Copy konfigurasi Hive
COPY ./conf/hive-site.xml /opt/hive/conf/hive-site.xml
COPY ./conf/hive-log4j2.properties /opt/hive/conf/hive-log4j2.properties

# Set permissions
RUN chown -R hive:hive /opt/hive/conf

# Inisialisasi Hive Metastore dengan PostgreSQL
CMD ["/bin/bash", "-c", "/opt/hive/bin/schematool -dbType postgres -initSchema && /opt/hive/bin/hive --service metastore"]