package at.fhv.streamprocessing.flink.job;

import at.fhv.streamprocessing.flink.record.MasterLocationIdentifierDatabasePojo;
import at.fhv.streamprocessing.flink.record.NoaaRecord;
import at.fhv.streamprocessing.flink.process.NoaaRecordParseProcessFunction;
import at.fhv.streamprocessing.flink.source.FtpDataSource;
import at.fhv.streamprocessing.flink.sink.MlidLoggingSink;
import at.fhv.streamprocessing.flink.sink.NoaaLoggingSink;
import at.fhv.streamprocessing.flink.source.MlidDataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DemoWeatherDataJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // NOAA Stream
        DataStream<String> weatherDataStream = env
                .addSource(new FtpDataSource())
                .name("weather-data");

        DataStream<NoaaRecord> noaaRecords = weatherDataStream
                .keyBy(a -> a)
                .process(new NoaaRecordParseProcessFunction())
                .name("noah-record-parser");

        noaaRecords
                .addSink(new NoaaLoggingSink())
                .name("noah-logging-sink");

        // MLID Stream
        String csvFilePath = "/opt/flink/resources/master-location-identifier-database-202401_standard.csv";
        DataStream<MasterLocationIdentifierDatabasePojo> mlidDataStream = MlidDataSource.getMlidDataStream(env, csvFilePath);

        mlidDataStream
                .addSink(new MlidLoggingSink())
                .name("master-location-identifier-database-logging-sink");

        env.execute("weather-data-demo-job");
    }
}
