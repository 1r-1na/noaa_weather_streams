package at.fhv.streamprocessing.flink;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class NoaaRecordParser extends KeyedProcessFunction<String, String, NoaaRecord> {
    private static final long serialVersionUID = 1L;
    private static final double MISSING_VALUE = 999.9;

    @Override
    public void processElement(String record, KeyedProcessFunction<String, String, NoaaRecord>.Context context, Collector<NoaaRecord> collector) {
        String year = parseYear(record);

        double airTemperature = parseAirTemperature(record);
        boolean isValidAirTemperature = airTemperature != MISSING_VALUE ? true : false;
        String airTemperatureQualityCode = parseAirTemperatureQualityCode(record);

        double windSpeedRate = parseWindSpeedRate(record);
        boolean isValidWindSpeedRate = windSpeedRate != MISSING_VALUE ? true : false;
        String windSpeedRateQualityCode = parseWindSpeedRateQualityCode(record);
        String windTypeCode = parseWindTypeCode(record);

        collector.collect(new NoaaRecord(year, airTemperature, isValidAirTemperature, airTemperatureQualityCode, windSpeedRate, isValidWindSpeedRate, windSpeedRateQualityCode, windTypeCode));
    }

    private String parseYear(String record) {
        return record.substring(15,19);
    }

    private double parseAirTemperature(String record) {
        double airTemperature;
        if (record.charAt(87) == '-') {
            airTemperature = Double.parseDouble(record.substring(87,92)) / 10;
        } else {
            airTemperature = Double.parseDouble(record.substring(88,92)) / 10;
        }
        return airTemperature;
    }

    private String parseAirTemperatureQualityCode(String record) {
        return record.substring(92,93);
    }

    private double parseWindSpeedRate(String record) {
        return Double.parseDouble(record.substring(65,69)) / 10;
    }

    private String parseWindSpeedRateQualityCode(String record) {
        return record.substring(69,70);
    }

    private String parseWindTypeCode(String record) {
        return record.substring(64,65);
    }
}
