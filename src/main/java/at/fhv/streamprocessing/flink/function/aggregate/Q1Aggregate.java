package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.util.AggregationType;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Q1Aggregate extends AbstractValueKeepingAggregate {
    
    @Override
    protected double calculateValue(LinkedList<Double> measurements) {
        Collections.sort(measurements);
        return calculateMedian(measurements.subList(0, measurements.size() / 2));
    }

    @Override
    protected String assignAggregateType() {
        return AggregationType.Q1.getTypeId();
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
