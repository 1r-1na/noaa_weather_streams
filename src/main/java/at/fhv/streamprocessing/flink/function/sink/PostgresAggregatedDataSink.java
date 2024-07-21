package at.fhv.streamprocessing.flink.function.sink;

import at.fhv.streamprocessing.flink.record.AggregatedDataRecord;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class PostgresAggregatedDataSink {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresAggregatedDataSink.class);

    public static SinkFunction<AggregatedDataRecord> createSink() {
        String insertStatement = "INSERT INTO public.aggregated_data (country, measurement_type, aggregation_type, value, start_ts, duration_days) VALUES (?, ?, ?, ?, ?, ?);";
        return JdbcSink.sink(insertStatement, (ps, record) -> {
                    ps.setString(1, record.country());
                    ps.setString(2, record.measurementType());
                    ps.setString(3, record.aggregationType());
                    ps.setDouble(4, record.value());
                    ps.setTimestamp(5, Timestamp.from(record.startTs()));
                    ps.setInt(6, record.durationDays());
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(100)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("org.postgresql.Driver")
                        .withUrl("jdbc:postgresql://postgres:5432/default")
                        .withUsername("dev_user")
                        .withPassword("dev_pw")
                        .build());
    }

}
