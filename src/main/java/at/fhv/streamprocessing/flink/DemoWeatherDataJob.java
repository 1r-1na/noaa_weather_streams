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

        CsvReaderFormat<MasterLocationIdentifierDatabasePojo> csvFormat = getCustomCsvFormat();
        String csvFilePath = "/opt/flink/resources/master-location-identifier-database-202401_standard.csv";
        FileSource<MasterLocationIdentifierDatabasePojo> source = getFileSource(csvFormat, csvFilePath);
        DataStream<MasterLocationIdentifierDatabasePojo> masterLocationIdentifierDatabaseStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csvFileSource");

        masterLocationIdentifierDatabaseStream
                .addSink(new MlidLoggingSink())
                .name("master-location-identifier-database-logging-sink");

        env.execute("weather-data-demo-job");
    }

    private static CsvReaderFormat<MasterLocationIdentifierDatabasePojo> getCustomCsvFormat() {
        CsvMapper mapper = new CsvMapper();
        return CsvReaderFormat.forSchema(
                mapper,
                mapper
                    .schemaFor(MasterLocationIdentifierDatabasePojo.class)
                    .withQuoteChar('"')
                    .withColumnSeparator(','),
                TypeInformation.of(MasterLocationIdentifierDatabasePojo.class)
        );
    }

    private static FileSource<MasterLocationIdentifierDatabasePojo> getFileSource(CsvReaderFormat<MasterLocationIdentifierDatabasePojo> csvFormat, String csvFilePath) {
        return FileSource.forRecordStreamFormat(
                csvFormat,
                new Path(csvFilePath)
        ).build();
    }
}
