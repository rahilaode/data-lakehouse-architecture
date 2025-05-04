from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def main():
    # 1. Setup environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. Register Iceberg catalog using Hive Metastore
    print("Executing SQL to create Iceberg catalog...")
    table_env.execute_sql("""
        CREATE CATALOG iceberg_catalog WITH (
            'type'='iceberg',
            'catalog-type'='hive',
            'warehouse'='s3a://hive/warehouse',
            'hive-conf-dir' = './conf'
        )
    """)
    print("Success create Iceberg catalog...")

    # 3. Create Kafka source table in default_catalog
    print("Switch to default_catalog for Kafka source...")
    table_env.execute_sql("USE CATALOG `default_catalog`")

    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS flights_kafka (
            flight_date STRING,
            flight_status STRING,

            departure ROW<
                airport STRING,
                timezone STRING,
                iata STRING,
                icao STRING,
                terminal STRING,
                gate STRING,
                delay STRING,
                scheduled STRING,
                estimated STRING
            >,

            arrival ROW<
                airport STRING,
                timezone STRING,
                iata STRING,
                icao STRING,
                terminal STRING,
                gate STRING,
                baggage STRING,
                scheduled STRING
            >,

            airline ROW<
                name STRING,
                iata STRING,
                icao STRING
            >,

            flight ROW<
                number STRING,
                iata STRING,
                icao STRING,
                codeshared ROW<
                    airline_name STRING,
                    airline_iata STRING,
                    airline_icao STRING,
                    flight_number STRING,
                    flight_iata STRING,
                    flight_icao STRING
                >
            >,

            aircraft STRING,
            live STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'flights-data',
            'properties.bootstrap.servers' = 'kafka1:9092',
            'properties.group.id' = 'flights-consumer',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    # 4. Create Iceberg schema 'testing' and target table
    print("Switch to iceberg_catalog for Iceberg target...")
    table_env.execute_sql("USE CATALOG iceberg_catalog")
    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS `testing`")

    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `testing`.flights_api (
            flight_date STRING,
            flight_status STRING,

            dep_airport STRING,
            dep_timezone STRING,
            dep_iata STRING,
            dep_icao STRING,
            dep_terminal STRING,
            dep_gate STRING,
            dep_delay STRING,
            dep_scheduled TIMESTAMP_LTZ(3),
            dep_estimated TIMESTAMP_LTZ(3),

            arr_airport STRING,
            arr_timezone STRING,
            arr_iata STRING,
            arr_icao STRING,
            arr_terminal STRING,
            arr_gate STRING,
            arr_baggage STRING,
            arr_scheduled TIMESTAMP_LTZ(3),

            airline_name STRING,
            airline_iata STRING,
            airline_icao STRING,

            flight_number STRING,
            flight_iata STRING,
            flight_icao STRING,

            codeshare_airline_name STRING,
            codeshare_iata STRING,
            codeshare_icao STRING,
            codeshare_flight_number STRING,
            codeshare_flight_iata STRING,
            codeshare_flight_icao STRING
        )
    """)
    print(table_env.list_catalogs())
    print(table_env.list_databases())
    print(table_env.list_tables())


    # 5. Insert transformed data into Iceberg
    print("Insert data into testing.flights_api...")
    table_env.execute_sql("""
        INSERT INTO `testing`.flights_api
        SELECT
            flight_date,
            flight_status,

            departure.airport,
            departure.timezone,
            departure.iata,
            departure.icao,
            departure.terminal,
            departure.gate,
            departure.delay,
            CAST(departure.scheduled AS TIMESTAMP),
            CAST(departure.estimated AS TIMESTAMP),

            arrival.airport,
            arrival.timezone,
            arrival.iata,
            arrival.icao,
            arrival.terminal,
            arrival.gate,
            arrival.baggage,
            CAST(arrival.scheduled AS TIMESTAMP),

            airline.name,
            airline.iata,
            airline.icao,

            flight.number,
            flight.iata,
            flight.icao,

            flight.codeshared.airline_name,
            flight.codeshared.airline_iata,
            flight.codeshared.airline_icao,
            flight.codeshared.flight_number,
            flight.codeshared.flight_iata,
            flight.codeshared.flight_icao
        FROM `default_catalog`.`default_database`.`flights_kafka`
    """)


    print("Insert complete.")


if __name__ == '__main__':
    main()
