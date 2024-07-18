package at.fhv.streamprocessing.flink.source;

import at.fhv.streamprocessing.flink.record.MasterLocationIdentifierRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MlidDataSource {
    public static DataStream<MasterLocationIdentifierRecord> getMlidDataStream(StreamExecutionEnvironment env) {
        String csvFilePath = "/opt/flink/resources/master-location-identifier-database-202401_standard.csv";
        CsvReaderFormat<MasterLocationIdentifierRecord> csvFormat = getCustomCsvFormat();
        FileSource<MasterLocationIdentifierRecord> source = getFileSource(csvFormat, csvFilePath);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "csvFileSource");
    }

    private static CsvReaderFormat<MasterLocationIdentifierRecord> getCustomCsvFormat() {
        CsvMapper mapper = new CsvMapper();
        return CsvReaderFormat.forSchema(
                mapper,
                mapper.schemaFor(MasterLocationIdentifierRecord.class)
                        .withQuoteChar('"')
                        .withColumnSeparator(','),
                TypeInformation.of(MasterLocationIdentifierRecord.class)
        );
    }

    private static FileSource<MasterLocationIdentifierRecord> getFileSource(CsvReaderFormat<MasterLocationIdentifierRecord> csvFormat, String csvFilePath) {
        return FileSource.forRecordStreamFormat(csvFormat, new Path(csvFilePath)).build();
    }
}
