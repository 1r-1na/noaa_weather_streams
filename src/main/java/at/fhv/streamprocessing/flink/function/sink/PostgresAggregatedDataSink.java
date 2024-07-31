package at.fhv.streamprocessing.flink.function.sink;

import at.fhv.streamprocessing.flink.util.Constants;
import at.fhv.streamprocessing.flink.record.AggregatedDataRecord;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class PostgresAggregatedDataSink {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresAggregatedDataSink.class);

    private static SinkFunction<AggregatedDataRecord> singleton = null;

    public static SinkFunction<AggregatedDataRecord> getSink() {
        if (singleton == null) {
            singleton = createSink();
        }
        return singleton;
    }
    private static SinkFunction<AggregatedDataRecord> createSink() {
        String insertStatement = "INSERT INTO public.aggregated_data (country, measurement_type, aggregation_type, value, start_ts, duration_days) VALUES (?, ?, ?, ?, ?, ?);";
        return JdbcSink.sink(insertStatement, (ps, record) -> {
                    ps.setString(1, record.country());
                    ps.setString(2, record.measurementType());
                    ps.setString(3, record.aggregationType());
                    ps.setDouble(4, record.value());
                    ps.setTimestamp(5, Timestamp.from(record.startTs()));
                    ps.setInt(6, record.durationDays());
                },
                Constants.JDBC_EXECUTION_OPTIONS,
                Constants.JDBC_CONNECTION_OPTIONS);
    }

}
