package at.fhv.streamprocessing.flink.function.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericLoggingSink<T> implements SinkFunction<T> {

    private static final long serialVersionUID = 1L;

    private final Logger logger;

    public GenericLoggingSink(String loggerName) {
        this.logger = LoggerFactory.getLogger(loggerName);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        logger.info("{}", value.toString());
    }
}
