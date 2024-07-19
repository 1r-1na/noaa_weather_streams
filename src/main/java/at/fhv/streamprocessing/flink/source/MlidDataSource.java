package at.fhv.streamprocessing.flink.source;

import at.fhv.streamprocessing.flink.record.MlidRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MlidDataSource {
    public static DataStream<MlidRecord> getMlidDataStream(StreamExecutionEnvironment env) {
        String csvFilePath = "/opt/flink/resources/master-location-identifier-database-202401_standard.csv";
        CsvReaderFormat<MlidRecord> csvFormat = getCustomCsvFormat();
        FileSource<MlidRecord> source = getFileSource(csvFormat, csvFilePath);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "csvFileSource");
    }

    private static CsvReaderFormat<MlidRecord> getCustomCsvFormat() {
        CsvMapper mapper = new CsvMapper();
        return CsvReaderFormat.forSchema(
                mapper,
                mapper.schemaFor(MlidRecord.class).withQuoteChar('"').withColumnSeparator(','),
                TypeInformation.of(MlidRecord.class)
        );
    }

    private static FileSource<MlidRecord> getFileSource(CsvReaderFormat<MlidRecord> csvFormat, String csvFilePath) {
        return FileSource.forRecordStreamFormat(csvFormat, new Path(csvFilePath)).build();
    }
}
