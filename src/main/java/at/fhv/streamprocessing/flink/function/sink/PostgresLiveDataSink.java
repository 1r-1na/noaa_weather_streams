package at.fhv.streamprocessing.flink.function.sink;

import at.fhv.streamprocessing.flink.Constants;
import at.fhv.streamprocessing.flink.record.LiveDataRecord;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class PostgresLiveDataSink {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresLiveDataSink.class);

    public static SinkFunction<LiveDataRecord> createSink() {

        StringBuilder statement = new StringBuilder()
                .append("INSERT INTO public.live_values (wban, measurement_type, code, value, timestamp, lat, lon, country) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ")
                .append("ON CONFLICT (wban, measurement_type) DO ")
                .append("UPDATE SET code = EXCLUDED.code, value = EXCLUDED.value, timestamp = EXCLUDED.timestamp, lat = EXCLUDED.lat, lon = EXCLUDED.lon, country = EXCLUDED.country;");

        return JdbcSink.sink(statement.toString(), (ps, record) -> {
                    ps.setString(1, record.wban());
                    ps.setString(2, record.measurementType());
                    ps.setString(3, record.code().toString());
                    ps.setDouble(4, record.value());
                    ps.setTimestamp(5, Timestamp.from(record.timestamp()));
                    ps.setString(6, record.lat());
                    ps.setString(7, record.lon());
                    ps.setString(8, record.country());
                },
                Constants.JDBC_EXECUTION_OPTIONS,
                Constants.JDBC_CONNECTION_OPTIONS);
    }

}
