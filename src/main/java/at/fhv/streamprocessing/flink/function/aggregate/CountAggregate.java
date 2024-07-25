package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.record.AggregatedDataRecord;
import at.fhv.streamprocessing.flink.util.AggregationType;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class CountAggregate implements AggregateFunction<AggregatedDataRecord, AggregatedDataRecord, AggregatedDataRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(AverageAggregate.class);

    @Override
    public AggregatedDataRecord createAccumulator() {
        return new AggregatedDataRecord("", "", AggregationType.COUNT.getTypeId(), 0.0, Instant.MAX, 0);
    }

    @Override
    public AggregatedDataRecord add(AggregatedDataRecord add, AggregatedDataRecord acc) {
        Instant startTs = add.startTs().isBefore(acc.startTs()) ? add.startTs() : acc.startTs();
        return new AggregatedDataRecord(add.country(), add.measurementType(), acc.aggregationType(), acc.value() + 1, startTs, add.durationDays());
    }

    @Override
    public AggregatedDataRecord getResult(AggregatedDataRecord acc) {
        return acc;
    }

    @Override
    public AggregatedDataRecord merge(AggregatedDataRecord acc1, AggregatedDataRecord acc2) {
        double value = acc1.value() + acc2.value();
        Instant startTs = acc1.startTs().isBefore(acc2.startTs()) ? acc1.startTs() : acc2.startTs();
        return new AggregatedDataRecord(acc1.country(), acc1.measurementType(), acc1.aggregationType(), value, startTs, acc1.durationDays());
    }
}
