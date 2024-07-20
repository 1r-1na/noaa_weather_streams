package at.fhv.streamprocessing.flink.record;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SingleValueRecord {

    private double value;
    private int count;
    private String country;
    private long timestamp;


    public SingleValueRecord() {

    }

    public SingleValueRecord(double value, String country, long timestamp) {
        this.value = value;
        this.count = 1;
        this.country = country;
        this.timestamp = timestamp;
    }

    public SingleValueRecord(double value, int count, String country, long timestamp) {
        this.value = value;
        this.count = count;
        this.country = country;
        this.timestamp = timestamp;
    }

    public double value() {
        return value;
    }

    public int count() {
        return count;
    }

    public String country() {
        return country;
    }

    public long timestamp() {
        return timestamp;
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
