package at.fhv.streamprocessing.flink.record;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class LocalizedNoaaRecord extends NoaaRecord {

    private String country;
    public LocalizedNoaaRecord(NoaaRecord noaaRecord, MlidRecord mlidRecord) {

        super(noaaRecord.year(),
                noaaRecord.airTemperature(),
                noaaRecord.isValidAirTemperature(),
                noaaRecord.airTemperatureQualityCode(),
                noaaRecord.windSpeedRate(),
                noaaRecord.isValidWindSpeedRate(),
                noaaRecord.windSpeedRateQualityCode(),
                noaaRecord.windTypeCode(),
                noaaRecord.latitude(),
                noaaRecord.longitude(),
                noaaRecord.wban());

        this.country = mlidRecord.country;
    }

    public String country() {
        return country;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
