package at.fhv.streamprocessing.flink.sink;

import at.fhv.streamprocessing.flink.record.MasterLocationIdentifierDatabasePojo;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MlidLoggingSink implements SinkFunction<MasterLocationIdentifierDatabasePojo> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MlidLoggingSink.class);

    public MlidLoggingSink() { }

    @Override
    public void invoke(MasterLocationIdentifierDatabasePojo record, Context context) {
        LOG.info("record: {}, ",
                record);
    }
}
