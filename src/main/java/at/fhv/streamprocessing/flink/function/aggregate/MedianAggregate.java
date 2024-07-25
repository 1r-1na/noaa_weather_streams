package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.util.AggregationType;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class MedianAggregate extends AbstractValueKeepingAggregate {

    @Override
    protected double calculateValue(LinkedList<Double> measurements) {
        if (measurements.size() < 2) {
            return measurements.get(0);
        }
        Collections.sort(measurements);
        return calculateMedian(measurements);
    }

    @Override
    protected String assignAggregateType() {
        return AggregationType.MEDIAN.getTypeId();
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
