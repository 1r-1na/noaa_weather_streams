package at.fhv.streamprocessing.flink.function.process;

import at.fhv.streamprocessing.flink.record.LiquidPrecipitation;
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

            boolean isValidPressure = !record.startsWith("99999", 99);
            double pressure = parsePressure(record);
            String pressureQualityCode = parsePressureQualityCode(record);

            LiquidPrecipitation liquidPrecipitation = parseLiquidPrecipitation(record);

            collector.collect(new NoaaRecord(year
                    , airTemperature
                    , isValidAirTemperature
                    , airTemperatureQualityCode
                    , windSpeedRate
                    , isValidWindSpeedRate
                    , windSpeedRateQualityCode
                    , windTypeCode
                    , latitude
                    , longitude
                    , wban
                    , date
                    , isValidPressure
                    , pressure
                    , pressureQualityCode
                    , liquidPrecipitation
            ));
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
        String dateString = record.substring(15, 27);
        LocalDateTime ldt = LocalDateTime.parse(dateString, NOAA_TIMESTAMP_FORMAT);
        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private double parsePressure(String record) {
        return Double.parseDouble(record.substring(99, 104)) / 10;
    }

    private String parsePressureQualityCode(String record) {
        return record.substring(104, 105);
    }

    private LiquidPrecipitation parseLiquidPrecipitation(String record) {
        int liquidPrecipitationRecordIndex = record.lastIndexOf("AA1");
        if (liquidPrecipitationRecordIndex == -1) {
            // invalid with quality code = MISSING
            return new LiquidPrecipitation("9");
        }

        String liquidPrecipitationRecord = record.substring(liquidPrecipitationRecordIndex + 3);
        String qualityCode = liquidPrecipitationRecord.substring(7, 8);
        if (liquidPrecipitationRecord.startsWith("99") || liquidPrecipitationRecord.startsWith("9999", 2)) {
            // invalid but parse real quality code
            return new LiquidPrecipitation(qualityCode);
        }

        int liquidPrecipitationHours = Integer.parseInt(liquidPrecipitationRecord.substring(0, 2));
        double liquidPrecipitationDepth = Double.parseDouble(liquidPrecipitationRecord.substring(2, 6)) / 10;
        // valid
        return new LiquidPrecipitation(liquidPrecipitationHours, liquidPrecipitationDepth, qualityCode);
    }

}