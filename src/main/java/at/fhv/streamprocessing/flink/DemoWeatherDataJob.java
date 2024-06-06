package at.fhv.streamprocessing.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class DemoWeatherDataJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<String> weatherDataStream = env
//                .addSource(new WeatherDataMockSource())
//                .name("weather-data");

        CsvReaderFormat<LocationPojo> csvFormat = CsvReaderFormat.forPojo(LocationPojo.class);
        FileSource<LocationPojo> source =
                FileSource.forRecordStreamFormat(csvFormat,
                        Path.fromLocalFile(new File("src/main/resources/master-location-identifier-database-202401_standard.csv"))).build();

        DataStream<LocationPojo> locationStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csvFileSource");

        locationStream
        .addSink(new LoggingSink())
        .name("noaa-logging-sink");

//        DataStream<NoaaRecord> noaaRecords = weatherDataStream
//                .keyBy(a -> a)
//                .process(new NoaaRecordParser())
//                .name("noaa-record-parser");

//        noaaRecords
//                .addSink(new NoaaLoggingSink())
//                .name("noaa-logging-sink");

        env.execute("weather-data-demo-job");
    }
}
