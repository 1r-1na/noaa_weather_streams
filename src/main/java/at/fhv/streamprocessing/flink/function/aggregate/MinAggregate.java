package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.record.SingleValueRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinAggregate implements AggregateFunction<SingleValueRecord, SingleValueRecord, SingleValueRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(AverageAggregate.class);

    @Override
    public SingleValueRecord createAccumulator() {
        return new SingleValueRecord(0, 0, "", 0);
    }

    @Override
    public SingleValueRecord add(SingleValueRecord add, SingleValueRecord acc) {
        double minValue = Math.min(acc.value(), add.value());
        return new SingleValueRecord(minValue, acc.count() + 1, add.country(), add.timestamp());
    }

    @Override
    public SingleValueRecord getResult(SingleValueRecord acc) {
        return new SingleValueRecord(acc.value(), acc.count(), acc.country(), acc.timestamp());
    }

    @Override
    public SingleValueRecord merge(SingleValueRecord acc1, SingleValueRecord acc2) {
        double minValue = Math.min(acc1.value(), acc2.value());
        return new SingleValueRecord(minValue, acc1.count() + acc2.count(), acc1.country(), acc1.timestamp());
    }
}
