package at.fhv.streamprocessing.flink.util;

import at.fhv.streamprocessing.flink.record.*;

import java.time.Instant;
import java.util.function.Function;
import java.util.function.Predicate;

public enum MeasurementTypes {

    TEMPERATURE("TEMPERATURE", NoaaRecord::isValidAirTemperature, NoaaRecord::airTemperature, NoaaRecord::airTemperatureQualityCode),
    WIND("WIND", NoaaRecord::isValidWindSpeedRate, NoaaRecord::windSpeedRate, NoaaRecord::windSpeedRateQualityCode),
    PRESSURE("ATMOSPHERIC_PRESSURE", NoaaRecord::isPressureValid, NoaaRecord::pressure, NoaaRecord::pressureQualityCode),
    LIQUID_PRECIPITATION("LIQUID_PRECIPITATION", NoaaRecord::isValidLiquidPrecipitation, NoaaRecord::liquidPrecipitationDepthInMillimetersPerHour, NoaaRecord::liquidPrecipitationQualityCode);


    private final String measurementTypeId;
    private final Predicate<LocalizedNoaaRecord> filterFunction;
    private final Function<NoaaRecord, Double> getValue;
    private final Function<NoaaRecord, String> getQualityCode;


    MeasurementTypes(String measurementTypeId, Predicate<LocalizedNoaaRecord> filterFunction,  Function<NoaaRecord, Double> getValue, Function<NoaaRecord, String> getQualityCode) {
        this.measurementTypeId = measurementTypeId;
        this.filterFunction = filterFunction;
        this.getValue = getValue;
        this.getQualityCode = getQualityCode;
    }


    public String measurementTypeId() {
        return measurementTypeId;
    }

    public boolean filter(LocalizedNoaaRecord record) {
        return filterFunction.test(record);
    }

    public AggregatedDataRecord noaaToAggregated(LocalizedNoaaRecord record, int durationDays) {
        return new AggregatedDataRecord(
                record.country(),
                measurementTypeId,
                null,
                getValue.apply(record),
                Instant.ofEpochMilli(record.timestamp()),
                durationDays
        );
    }

    public QualityCodeRecord noaaToQualityCode(LocalizedNoaaRecord record, int durationDays) {
        return new QualityCodeRecord(
                record.wban(),
                measurementTypeId,
                getQualityCode.apply(record).charAt(0),
                1,
                Instant.ofEpochMilli(record.timestamp()),
                durationDays);
    }

    public LiveDataRecord noaaToLive(LocalizedNoaaRecord record) {
        return new LiveDataRecord(
                record.wban(),
                measurementTypeId,
                getQualityCode.apply(record).charAt(0),
                getValue.apply(record),
                Instant.ofEpochMilli(record.timestamp()),
                record.latitude(),
                record.longitude(),
                record.country());
    }

}
