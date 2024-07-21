package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.record.QualityCodeRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class QualityCodeRecordCounter implements AggregateFunction<QualityCodeRecord, QualityCodeRecord, QualityCodeRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(QualityCodeRecordCounter.class);

    @Override
    public QualityCodeRecord createAccumulator() {
        return new QualityCodeRecord("", "", '0', 0, Instant.MAX, 0);
    }

    @Override
    public QualityCodeRecord add(QualityCodeRecord add, QualityCodeRecord acc) {
        Instant startTs = add.startTs().isBefore(acc.startTs()) ? add.startTs() : acc.startTs();
        return new QualityCodeRecord(add.wban(), add.measurementType(), add.code(), acc.amount() + 1, startTs, add.durationDays());
    }

    @Override
    public QualityCodeRecord getResult(QualityCodeRecord acc) {
        return acc;
    }

    @Override
    public QualityCodeRecord merge(QualityCodeRecord acc1, QualityCodeRecord acc2) {
        Instant startTs = acc1.startTs().isBefore(acc2.startTs()) ? acc1.startTs() : acc2.startTs();
        return new QualityCodeRecord(acc1.wban(), acc1.measurementType(), acc1.code(), acc1.amount() + acc2.amount(), startTs, acc1.durationDays());
    }
}
