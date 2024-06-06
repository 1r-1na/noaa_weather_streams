package at.fhv.streamprocessing.flink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSink implements SinkFunction<LocationPojo> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(NoaaLoggingSink.class);

    public LoggingSink() { }

    @Override
    public void invoke(LocationPojo record, Context context) {
        LOG.info("record: {}, ",
                record);
    }
}
