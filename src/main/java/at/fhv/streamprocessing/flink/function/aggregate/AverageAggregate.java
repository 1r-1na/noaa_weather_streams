package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.record.AggregatedDataRecord;
import at.fhv.streamprocessing.flink.util.AggregationType;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class AverageAggregate implements AggregateFunction<AggregatedDataRecord, Tuple2<AggregatedDataRecord, Integer>, AggregatedDataRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(AverageAggregate.class);

    @Override
    public Tuple2<AggregatedDataRecord, Integer> createAccumulator() {
        AggregatedDataRecord record = new AggregatedDataRecord("", "", AggregationType.AVG.getTypeId(), 0.0, Instant.MAX, 0);
        return new Tuple2<>(record, 1);
    }

    @Override
    public Tuple2<AggregatedDataRecord, Integer> add(AggregatedDataRecord add, Tuple2<AggregatedDataRecord, Integer> acc) {

        AggregatedDataRecord accRecord = acc.f0;
        int count = acc.f1;

        Instant startTs = add.startTs().isBefore(accRecord.startTs()) ? add.startTs() : accRecord.startTs();
        AggregatedDataRecord newAcc = new AggregatedDataRecord(add.country(), add.measurementType(), accRecord.aggregationType(), accRecord.value() + add.value(), startTs, accRecord.durationDays());
        return new Tuple2<>(newAcc, count + 1);
    }

    @Override
    public AggregatedDataRecord getResult(Tuple2<AggregatedDataRecord, Integer> acc) {
        AggregatedDataRecord accRecord = acc.f0;
        int count = acc.f1;
        return new AggregatedDataRecord(accRecord.country(), accRecord.measurementType(), accRecord.aggregationType(), accRecord.value() / count, accRecord.startTs(), accRecord.durationDays());
    }

    @Override
    public Tuple2<AggregatedDataRecord, Integer> merge(Tuple2<AggregatedDataRecord, Integer> acc1, Tuple2<AggregatedDataRecord, Integer> acc2) {

        AggregatedDataRecord accRecord1 = acc1.f0;
        AggregatedDataRecord accRecord2 = acc2.f0;
        int count = acc1.f1 + acc2.f1;
        double value = accRecord1.value() + accRecord2.value();
        Instant startTs = accRecord1.startTs().isBefore(accRecord2.startTs()) ? accRecord1.startTs() : accRecord2.startTs();

        AggregatedDataRecord newAcc = new AggregatedDataRecord(accRecord2.country(), accRecord1.measurementType(), accRecord1.measurementType(), value, startTs, accRecord1.durationDays());
        return new Tuple2<>(newAcc ,count);
    }
}
