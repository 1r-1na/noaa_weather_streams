package at.fhv.streamprocessing.flink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoDoubleLoggingSink implements SinkFunction<Double> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DemoDoubleLoggingSink.class);

    public DemoDoubleLoggingSink() { }

    @Override
    public void invoke(Double value, Context context) throws Exception {
        LOG.info("Temperature: {}", value.toString());
    }
}
