package at.fhv.streamprocessing.flink.record;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.time.Instant;

public class LiveDataRecord {

    private String wban;
    private String measurementType;
    private Character code;
    private double value;
    private Instant timestamp;
    private String lat;
    private String lon;

    private String country;


    public LiveDataRecord() {
    }

    public LiveDataRecord(String wban, String measurementType, Character code, double value, Instant timestamp, String lat, String lon, String country) {
        this.wban = wban;
        this.measurementType = measurementType;
        this.code = code;
        this.value = value;
        this.timestamp = timestamp;
        this.lat = lat;
        this.lon = lon;
        this.country = country;
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

    public double value() {
        return value;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public String lat() {
        return lat;
    }

    public String lon() {
        return lon;
    }

    public String country() {
        return country;
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
}
