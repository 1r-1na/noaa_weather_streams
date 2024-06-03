package at.fhv.streamprocessing.flink;

public class NoaaRecord {
    final private String year;
    final double airTemperature;
    final boolean isValidAirTemperature;
    final String airTemperatureQualityCode;
    final double windSpeedRate;
    final boolean isValidWindSpeedRate;
    final String windSpeedRateQualityCode;
    final String windTypeCode;

    public NoaaRecord(String year, double airTemperature, boolean isValidAirTemperature, String airTemperatureQualityCode, double windSpeedRate, boolean isValidWindSpeedRate, String windSpeedRateQualityCode, String windTypeCode) {
        this.year = year;
        this.airTemperature = airTemperature;
        this.isValidAirTemperature = isValidAirTemperature;
        this.airTemperatureQualityCode = airTemperatureQualityCode;
        this.windSpeedRate = windSpeedRate;
        this.isValidWindSpeedRate = isValidWindSpeedRate;
        this.windSpeedRateQualityCode = windSpeedRateQualityCode;
        this.windTypeCode = windTypeCode;
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
}
