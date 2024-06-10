package at.fhv.streamprocessing.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DemoWeatherDataJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> weatherDataStream = env
                .addSource(new WeatherDataMockSource())
                .name("weather-data");

        CsvMapper mapper = new CsvMapper();
        CsvReaderFormat<LocationPojo> csvFormat = CsvReaderFormat.forSchema(mapper, mapper.schemaFor(LocationPojo.class).withQuoteChar('"').withColumnSeparator(','), TypeInformation.of(LocationPojo.class));

        // Use the path inside the container
        String csvFilePath = "/opt/flink/resources/master-location-identifier-database-202401_standard.csv";

        FileSource<LocationPojo> source = FileSource.forRecordStreamFormat(
                csvFormat,
                new Path(csvFilePath)
        ).build();

        DataStream<LocationPojo> locationStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csvFileSource");

        locationStream
                .addSink(new LoggingSink())
                .name("noaa-logging-sink");

        env.execute("weather-data-demo-job");
    }
}
