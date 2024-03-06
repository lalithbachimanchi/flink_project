from pyflink.common import Row
from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream import StreamExecutionEnvironment,
# from pyflink.common.typeinfo import STRING, INT
from pyflink.table import DataTypes
from pyflink.common import Configuration, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream import
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.jdbc import JdbcSink


# class MyTransformation(StreamTransformation[str, Row]):
#
#     def transform(self, data_stream: 'StreamTransformation[str, str]') -> 'StreamTransformation[str, Row]':
#         # Split the data into fields based on a delimiter (e.g., comma)
#         split_stream = data_stream.flat_map(lambda x: x.split(","))
#
#         # Select specific fields and convert to integers if needed
#         return split_stream.select((0, 2)).cast(type_info=[DataTypes.STRING(), DataTypes.INT()])


def main():
    # Flink environment setup
    # env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:///Users/lalith/GenAIPoc/flink_project/flink-sql-connector-kafka-3.1.0-1.18.jar")

    # Kafka consumer configuration
    kafka_topic = "your_kafka_topic"
    kafka_bootstrap_servers = "localhost:9092"
    kafka_group_id = "flink_consumer_group"

    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("input-topic") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # Define Kafka source
    # kafka_source = FlinkKafkaConsumer(
    #     topics=[kafka_topic],
    #     deserialization_schema=SimpleStringSchema(),
    #     properties={"bootstrap.servers": kafka_bootstrap_servers, "group.id": kafka_group_id}
    # )

    import pdb
    pdb.set_trace()

    # Data transformation
    # transformed_stream = kafka_source.transform(MyTransformation())

    # MySQL connection configuration
    jdbc_url = "jdbc:mysql://localhost:3306/your_database"
    username = "your_username"
    password = "your_password"
    table_name = "your_table"

    # Define JDBC sink
    jdbc_sink = JdbcSink.builder() \
        .set_driver_name("com.mysql.cj.jdbc.Driver") \
        .set_connection_url(jdbc_url) \
        .set_username(username) \
        .set_password(password) \
        .set_query("INSERT INTO " + table_name + " (field1, field2) VALUES (?, ?)") \
        .set_parameter_types([DataTypes.STRING, DataTypes.INT]) \
        .build()

    # Connect transformed data to MySQL sink
    ds.sink_to(jdbc_sink)

    # Execute the Flink application
    env.execute("Flink Kafka-to-MySQL Example")


if __name__ == "__main__":
    main()
