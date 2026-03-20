from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_green_trips_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime  VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID          INTEGER,
            DOLocationID          INTEGER,
            passenger_count       DOUBLE,
            trip_distance         DOUBLE,
            tip_amount            DOUBLE,
            total_amount          DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def create_session_results_sink_postgres(t_env):
    table_name = "session_results"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start  TIMESTAMP(3),
            window_end    TIMESTAMP(3),
            PULocationID  INTEGER,
            num_trips     BIGINT,
            PRIMARY KEY (window_start, window_end, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_session_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Disable checkpointing — bounded source, no need for recovery
    env.set_parallelism(1)  # single partition topic - must be 1

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Advance watermark when source goes idle after bounded read completes
    t_env.get_config().set("table.exec.source.idle-timeout", "10s")
    # Disable mini-batch to avoid buffering issues with session window merges
    t_env.get_config().set("table.exec.mini-batch.enabled", "false")

    try:
        source_table = create_green_trips_source_kafka(t_env)
        sink_table = create_session_results_sink_postgres(t_env)

        # Filter out rows with NULL PULocationID or unparseable timestamps
        # to prevent NPE in session window grouping
        t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(TABLE {source_table} PARTITION BY PULocationID, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        WHERE PULocationID IS NOT NULL
        GROUP BY window_start, window_end, PULocationID
        """).wait()

    except Exception as e:
        print("Session job failed:", str(e))


if __name__ == '__main__':
    log_session_aggregation()


