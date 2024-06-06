package at.fhv.streamprocessing.flink;

import at.fhv.streamprocessing.flink.ftp.FTPSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DemoWeatherDataJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> weatherDataStream = env
                .addSource(new FTPSourceFunction())
                .name("weather-data");

        DataStream<NoaaRecord> noaaRecords = weatherDataStream
                .keyBy(a -> a)
                .process(new NoaaRecordParser())
                .name("noah-record-parser");

        noaaRecords
                .addSink(new NoaaLoggingSink())
                .name("noah-logging-sink");

        env.execute("weather-data-demo-job");
    }
}
