package at.fhv.streamprocessing.flink.function.process;

import at.fhv.streamprocessing.flink.record.NoaaRecord;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class NoaaRecordParseProcessFunction extends KeyedProcessFunction<String, String, NoaaRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(NoaaRecordParseProcessFunction.class);

    private final static DateTimeFormatter NOAA_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmm");


    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(String record, KeyedProcessFunction<String, String, NoaaRecord>.Context context, Collector<NoaaRecord> collector) {

        try {
            String year = parseYear(record);

            double airTemperature = parseAirTemperature(record);
            boolean isValidAirTemperature = !record.startsWith("+9999", 87);
            String airTemperatureQualityCode = parseAirTemperatureQualityCode(record);

            double windSpeedRate = parseWindSpeedRate(record);
            boolean isValidWindSpeedRate = !record.startsWith("9999", 65);
            String windSpeedRateQualityCode = parseWindSpeedRateQualityCode(record);
            String windTypeCode = parseWindTypeCode(record);

            String latitude = parseLatitude(record);
            String longitude = parseLongitude(record);
            String wban = parseWban(record);

            long date = parseDate(record);

            collector.collect(new NoaaRecord(year, airTemperature, isValidAirTemperature, airTemperatureQualityCode, windSpeedRate, isValidWindSpeedRate, windSpeedRateQualityCode, windTypeCode, latitude, longitude, wban, date));
        } catch (Exception e) {
            LOG.error("Could not parse {} char long Record {}", record.length(), record, e);
        }

    }

    private String parseYear(String record) {
        return record.substring(15, 19);
    }

    private double parseAirTemperature(String record) {
        return Double.parseDouble(record.substring(87, 92)) / 10;
    }

    private String parseAirTemperatureQualityCode(String record) {
        return record.substring(92, 93);
    }

    private double parseWindSpeedRate(String record) {
        return Double.parseDouble(record.substring(65, 69)) / 10;
    }

    private String parseWindSpeedRateQualityCode(String record) {
        return record.substring(69, 70);
    }

    private String parseWindTypeCode(String record) {
        return record.substring(64, 65);
    }

    private String parseLatitude(String record) {
        return String.valueOf(Double.parseDouble(record.substring(28, 34)) / 1000);
    }

    private String parseLongitude(String record) {
        return String.valueOf(Double.parseDouble(record.substring(34, 41)) / 1000);
    }

    private String parseWban(String record) {
        return record.substring(10, 15);
    }

    private long parseDate(String record) {
        String dateString = record.substring(15,27);
        LocalDateTime ldt = LocalDateTime.parse(dateString, NOAA_TIMESTAMP_FORMAT);
        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

}