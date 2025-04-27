from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Kafka Consumer Config
    kafka_props = {
        'bootstrap.servers': 'kafka1:9092,kafka2:9093,kafka3:9094',
        'group.id': 'flink-flight-consumer',
        'auto.offset.reset': 'earliest'
    }

    # Define Kafka Source
    kafka_consumer = FlinkKafkaConsumer(
        topics='flights_data',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    # Create stream
    stream = env.add_source(kafka_consumer).name("Kafka Source")

    # Example: parse json and print certain fields
    parsed_stream = stream \
        .map(lambda x: json.loads(x), output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .map(lambda x: f"Flight: {x.get('flight', {}).get('iata')}, Status: {x.get('flight_status')}", output_type=Types.STRING())

    parsed_stream.print()

    # Execute
    env.execute("Flink Kafka Flight Data Consumer")

if __name__ == "__main__":
    main()