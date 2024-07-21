package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.record.SingleValueRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AverageAggregate implements AggregateFunction<SingleValueRecord, SingleValueRecord, SingleValueRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(AverageAggregate.class);

    @Override
    public SingleValueRecord createAccumulator() {
        return new SingleValueRecord(0, 0, "", Long.MAX_VALUE);
    }

    @Override
    public SingleValueRecord add(SingleValueRecord add, SingleValueRecord acc) {
        long lowerTs = Math.min(add.timestamp(), acc.timestamp());
        return new SingleValueRecord(acc.value() + add.value(), acc.count() + 1, add.country(), lowerTs);
    }

    @Override
    public SingleValueRecord getResult(SingleValueRecord acc) {
        return new SingleValueRecord(acc.value() / acc.count(), acc.count(), acc.country(), acc.timestamp());
    }

    @Override
    public SingleValueRecord merge(SingleValueRecord acc1, SingleValueRecord acc2) {
        long lowerTs = Math.min(acc1.timestamp(), acc2.timestamp());
        return new SingleValueRecord(acc1.value() + acc2.value(), acc1.count() + acc2.count(), acc1.country(), lowerTs);
    }
}
