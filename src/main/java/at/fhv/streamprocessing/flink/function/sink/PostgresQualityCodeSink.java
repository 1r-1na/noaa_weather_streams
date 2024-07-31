package at.fhv.streamprocessing.flink.function.sink;

import at.fhv.streamprocessing.flink.record.AggregatedDataRecord;
import at.fhv.streamprocessing.flink.util.Constants;
import at.fhv.streamprocessing.flink.record.QualityCodeRecord;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class PostgresQualityCodeSink {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresQualityCodeSink.class);

    private static SinkFunction<QualityCodeRecord> singleton = null;

    public static SinkFunction<QualityCodeRecord> getSink() {
        if (singleton == null) {
            singleton = createSink();
        }
        return singleton;
    }

    private static SinkFunction<QualityCodeRecord> createSink() {
        String insertStatement = "INSERT INTO public.quality_codes (wban, measurement_type, code, amount, start_ts, duration_days) VALUES (?, ?, ?, ?, ?, ?);";
        return JdbcSink.sink(insertStatement, (ps, record) -> {
                    ps.setString(1, record.wban());
                    ps.setString(2, record.measurementType());
                    ps.setString(3, record.code().toString());
                    ps.setLong(4, record.amount());
                    ps.setTimestamp(5, Timestamp.from(record.startTs()));
                    ps.setInt(6, record.durationDays());
                },
                Constants.JDBC_EXECUTION_OPTIONS,
                Constants.JDBC_CONNECTION_OPTIONS);
    }

}
