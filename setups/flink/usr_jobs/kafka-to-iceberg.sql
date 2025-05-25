CREATE TABLE t_kafka_weathers_api(
     `timestamp`        TIMESTAMP,
     `location`         STRING,
     weather_condition  STRING,
     details            ROW<
        temperature INT,
        humidity INT,
        wind_speed INT
    >
  ) WITH (
    'connector' = 'kafka',
    'topic' = 'weather-api-data',
    'properties.bootstrap.servers' = 'kafka1:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
  );


SET 'execution.checkpointing.interval' = '1sec';
SET 'pipeline.operator-chaining.enabled' = 'false';

CREATE TABLE weathers_data
  WITH (
  'connector' = 'iceberg',
  'catalog-type'='hive',
  'catalog-name'='dev',
  'warehouse' = 's3a://warehouse',
  'hive-conf-dir' = '/opt/flink/conf')
  AS 
  SELECT 
    `timestamp`,
    `location`,
    details[1] as temperature,
    details[2] as humidity,
    details[3] as wind_speed,
    weather_condition
  FROM t_kafka_weathers_api;