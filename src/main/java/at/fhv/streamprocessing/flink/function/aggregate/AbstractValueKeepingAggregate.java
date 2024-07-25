package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.record.AggregatedDataRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Instant;
import java.util.LinkedList;

public abstract class AbstractValueKeepingAggregate implements AggregateFunction<AggregatedDataRecord, Tuple2<AggregatedDataRecord, LinkedList<Double>>, AggregatedDataRecord> {

    protected abstract double calculateValue(LinkedList<Double> measurements);

    protected abstract String assignAggregateType();

    @Override
    public Tuple2<AggregatedDataRecord, LinkedList<Double>> createAccumulator() {
        AggregatedDataRecord record = new AggregatedDataRecord("", "", assignAggregateType(), 0.0, Instant.MAX, 0);
        return new Tuple2<>(record, new LinkedList<>());
    }

    @Override
    public Tuple2<AggregatedDataRecord, LinkedList<Double>> add(AggregatedDataRecord add, Tuple2<AggregatedDataRecord, LinkedList<Double>> acc) {
        AggregatedDataRecord accRecord = acc.f0;
        acc.f1.add(add.value());

        Instant startTs = add.startTs().isBefore(accRecord.startTs()) ? add.startTs() : accRecord.startTs();
        AggregatedDataRecord newAcc = new AggregatedDataRecord(add.country(), add.measurementType(), accRecord.aggregationType(), 0.0, startTs, add.durationDays());
        return new Tuple2<>(newAcc, acc.f1);
    }

    @Override
    public AggregatedDataRecord getResult(Tuple2<AggregatedDataRecord, LinkedList<Double>> acc) {
        AggregatedDataRecord accRecord = acc.f0;
        if (acc.f1.isEmpty()) {
            return new AggregatedDataRecord(accRecord.country(), accRecord.measurementType(), "INVALID", 0.0, accRecord.startTs(), accRecord.durationDays());
        }
        double value = calculateValue(acc.f1);
        return new AggregatedDataRecord(accRecord.country(), accRecord.measurementType(), accRecord.aggregationType(), value, accRecord.startTs(), accRecord.durationDays());
    }


    @Override
    public Tuple2<AggregatedDataRecord, LinkedList<Double>> merge(Tuple2<AggregatedDataRecord, LinkedList<Double>> acc1, Tuple2<AggregatedDataRecord, LinkedList<Double>> acc2) {

        AggregatedDataRecord accRecord1 = acc1.f0;
        AggregatedDataRecord accRecord2 = acc2.f0;
        Instant startTs = accRecord1.startTs().isBefore(accRecord2.startTs()) ? accRecord1.startTs() : accRecord2.startTs();

        LinkedList<Double> values = acc1.f1;
        values.addAll(acc2.f1);
        AggregatedDataRecord newAcc = new AggregatedDataRecord(accRecord2.country(), accRecord1.measurementType(), accRecord1.aggregationType(), 0.0, startTs, accRecord1.durationDays());
        return new Tuple2<>(newAcc, values);
    }

}
