package at.fhv.streamprocessing.flink.record;

import at.fhv.streamprocessing.flink.Constants;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.time.Instant;

public class QualityCodeRecord {

    private String wban;
    private String measurementType;
    private Character code;
    private long amount;
    private Instant startTs;
    private int durationDays;


    public QualityCodeRecord() {
    }

    public QualityCodeRecord(String wban, String measurementType, Character code, long amount, Instant startTs, int durationDays) {
        this.wban = wban;
        this.measurementType = measurementType;
        this.code = code;
        this.amount = amount;
        this.startTs = startTs;
        this.durationDays = durationDays;
    }

    public String wban() {
        return wban;
    }

    public String measurementType() {
        return measurementType;
    }

    public Character code() {
        return code;
    }

    public long amount() {
        return amount;
    }

    public Instant startTs() {
        return startTs;
    }

    public int durationDays() {
        return durationDays;
    }

    public String getKey() {
        return wban + measurementType + code;
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

    public static QualityCodeRecord forTemperatureOfLocalizedNoaaRecord(LocalizedNoaaRecord record, int durationDays) {
        return new QualityCodeRecord(
                record.wban(),
                Constants.MEASUREMENT_TYPE_TEMPERATURE,
                record.airTemperatureQualityCode.charAt(0),
                1,
                Instant.ofEpochMilli(record.timestamp()),
                durationDays);
    }

}
