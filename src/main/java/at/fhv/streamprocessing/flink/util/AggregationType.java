package at.fhv.streamprocessing.flink.util;

import at.fhv.streamprocessing.flink.function.aggregate.AverageAggregate;
import at.fhv.streamprocessing.flink.function.aggregate.CountAggregate;
import at.fhv.streamprocessing.flink.function.aggregate.MaxAggregate;
import at.fhv.streamprocessing.flink.function.aggregate.MinAggregate;
import at.fhv.streamprocessing.flink.record.AggregatedDataRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.function.Supplier;

public enum AggregationType {

    AVG("AVG", AverageAggregate::new),
    MIN("MIN", MinAggregate::new),
    MAX("MAX", MaxAggregate::new),
    COUNT("COUNT",CountAggregate::new);


    private final String typeId;
    private final Supplier<AggregateFunction<AggregatedDataRecord, ?, AggregatedDataRecord>> supplierFunction;

    AggregationType(String typeId, Supplier<AggregateFunction<AggregatedDataRecord, ?, AggregatedDataRecord>> supplierFunction)  {
        this.typeId = typeId;
        this.supplierFunction = supplierFunction;
    }

    public AggregateFunction<AggregatedDataRecord, ?, AggregatedDataRecord> aggregateFunction() {
        return supplierFunction.get();
    }

    public String getTypeId() {
        return typeId;
    }
}

