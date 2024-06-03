package at.fhv.streamprocessing.flink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoaaLoggingSink implements SinkFunction<NoaaRecord> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(NoaaLoggingSink.class);

    public NoaaLoggingSink() { }

    @Override
    public void invoke(NoaaRecord record, Context context) {
        LOG.info("year: {}, " +
                "airTemperature: {}, " +
                "isValidAirTemperature: {}, " +
                "airTemperatureQualityCode: {}, " +
                "windSpeedRate: {}, " +
                "isValidWindSpeedRate: {}, " +
                "windSpeedRateQualityCode: {} " +
                "windTypeCode: {}",
                record.year(), record.airTemperature(), record.isValidAirTemperature(), record.airTemperatureQualityCode(), record.windSpeedRate(), record.isValidWindSpeedRate(), record.windSpeedRateQualityCode(), record.windTypeCode());
    }
}
