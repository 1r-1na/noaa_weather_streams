package at.fhv.streamprocessing.flink.record;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class NoaaRecord {
    final private String year;
    final double airTemperature;
    final boolean isValidAirTemperature;
    final String airTemperatureQualityCode;
    final double windSpeedRate;
    final boolean isValidWindSpeedRate;
    final String windSpeedRateQualityCode;
    final String windTypeCode;
    final String latitude;
    final String longitude;
    private String countryCode;
    private final String wban;
    private final long timestamp;


    public NoaaRecord(String year, double airTemperature, boolean isValidAirTemperature, String airTemperatureQualityCode, double windSpeedRate, boolean isValidWindSpeedRate, String windSpeedRateQualityCode, String windTypeCode, String latitude, String longitude, String wban) {
        this.year = year;
        this.airTemperature = airTemperature;
        this.isValidAirTemperature = isValidAirTemperature;
        this.airTemperatureQualityCode = airTemperatureQualityCode;
        this.windSpeedRate = windSpeedRate;
        this.isValidWindSpeedRate = isValidWindSpeedRate;
        this.windSpeedRateQualityCode = windSpeedRateQualityCode;
        this.windTypeCode = windTypeCode;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = System.currentTimeMillis();
        this.wban = wban;
    }

    public String year() {
        return year;
    }

    public double airTemperature() {
        return airTemperature;
    }

    public boolean isValidAirTemperature() {
        return isValidAirTemperature;
    }

    public String airTemperatureQualityCode() {
        return airTemperatureQualityCode;
    }

    public double windSpeedRate() {
        return windSpeedRate;
    }

    public boolean isValidWindSpeedRate() {
        return isValidWindSpeedRate;
    }

    public String windSpeedRateQualityCode() {
        return windSpeedRateQualityCode;
    }

    public String windTypeCode() {
        return windTypeCode;
    }

    public long timestamp() {
        return timestamp;
    }

    public String latitude() {
        return latitude;
    }

    public String longitude() {
        return longitude;
    }

    public String wban() {
        return wban;
    }


    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}