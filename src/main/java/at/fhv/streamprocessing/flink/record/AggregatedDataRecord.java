package at.fhv.streamprocessing.flink.record;

import at.fhv.streamprocessing.flink.Constants;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.time.Instant;

public class AggregatedDataRecord {

    private String country;
    private String measurementType;
    private String aggregationType;
    private double value;
    private Instant startTs;
    private int durationDays;


    public AggregatedDataRecord() {
    }

    public AggregatedDataRecord(String country, String measurementType, String aggregationType, double value, Instant startTs, int durationDays) {
        this.country = country;
        this.measurementType = measurementType;
        this.aggregationType = aggregationType;
        this.value = value;
        this.startTs = startTs;
        this.durationDays = durationDays;
    }


    public String country() {
        return country;
    }

    public String measurementType() {
        return measurementType;
    }

    public String aggregationType() {
        return aggregationType;
    }

    public double value() {
        return value;
    }

    public Instant startTs() {
        return startTs;
    }

    public int durationDays() {
        return durationDays;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    public static AggregatedDataRecord forTemperatureOfLocalizedNoaaRecord(LocalizedNoaaRecord record, int durationDays) {
        return new AggregatedDataRecord(
                record.country(),
                Constants.MEASUREMENT_TYPE_TEMPERATURE,
                null,
                record.airTemperature(),
                Instant.ofEpochMilli(record.timestamp()),
                durationDays
        );
    }
}
