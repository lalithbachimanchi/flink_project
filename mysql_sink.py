from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types
from pyflink.datastream.connectors.jdbc import JdbcSink
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions, JdbcExecutionOptions


env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars("file:///Users/lalith/GenAIPoc/sage_stack/database_connectors/mysql-connector-j-8.1.0.jar")
# env.add_classpaths("file:///Users/lalith/GenAIPoc/sage_stack/database_connectors/mysql-connector-j-8.1.0.jar")
type_info = Types.ROW([Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])
ds = env.from_collection(
    [(101, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
     (102, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
     (103, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
     (104, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
     ], type_info=type_info)


jdbc_sink = JdbcSink.sink(
        "insert into books (id, title, authors, year) values (?, ?, ?, ?)",
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url('jdbc:mysql://dbhost:5432/postgresdb')
            .with_driver_name('org.postgresql.Driver')
            .with_user_name('someUser')
            .with_password('somePassword')
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_interval_ms(1000)
            .with_batch_size(200)
            .with_max_retries(5)
            .build()
    )

ds.add_sink(jdbc_sink)

    # .add_sink(
    # JdbcSink.sink(
    #     "insert into books (id, title, authors, year) values (?, ?, ?, ?)",
    #     type_info,
    #     JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
    #         .with_url('jdbc:mysql://dbhost:5432/postgresdb')
    #         .with_driver_name('org.postgresql.Driver')
    #         .with_user_name('someUser')
    #         .with_password('somePassword')
    #         .build(),
    #     JdbcExecutionOptions.builder()
    #         .with_batch_interval_ms(1000)
    #         .with_batch_size(200)
    #         .with_max_retries(5)
    #         .build()
    # ))

env.execute()