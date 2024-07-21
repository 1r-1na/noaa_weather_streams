package at.fhv.streamprocessing.flink.util;

import at.fhv.streamprocessing.flink.function.aggregate.*;
import at.fhv.streamprocessing.flink.record.AggregatedDataRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.function.Supplier;

public enum AggregationType {

    AVG("AVG", AverageAggregate::new),
    MIN("MIN", MinAggregate::new),
    MAX("MAX", MaxAggregate::new),
    COUNT("COUNT",CountAggregate::new),
    MEDIAN("MEDIAN", MedianAggregate::new),
    Q1("Q1", Q1Aggregate::new),
    Q3("Q3", Q3Aggregate::new),
    WHISKER_L("WHISKER_L", WhiskerLowerAggregate::new),
    WHISKER_U("WHISKER_U", WhiskerUpperAggregate::new),
    STANDARD_DEVIATION("STD", StandardDeviationAggregate::new);




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

