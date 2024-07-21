package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.util.AggregationType;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Q3Aggregate extends AbstractValueKeepingAggregate {
    
    @Override
    protected double calculateValue(LinkedList<Double> measurements) {
        Collections.sort(measurements);
        return calculateMedian(measurements.subList((measurements.size() + 1) / 2, measurements.size()));
    }

    @Override
    protected String assignAggregateType() {
        return AggregationType.Q3.getTypeId();
    }

    private double calculateMedian(List<Double> list) {
        int size = list.size();
        if (size % 2 == 0) {
            return (list.get(size / 2 - 1) + list.get(size / 2)) / 2.0;
        } else {
            return list.get(size / 2);
        }
    }
}
