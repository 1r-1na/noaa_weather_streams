package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.util.AggregationType;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class WhiskerUpperAggregate extends AbstractValueKeepingAggregate {

    @Override
    protected double calculateValue(LinkedList<Double> measurements) {
        if (measurements.size() < 2) {
            return measurements.get(0);
        }
        Collections.sort(measurements);
        double q1 = calculateMedian(measurements.subList(0, measurements.size() / 2));
        double q3 = calculateMedian(measurements.subList((measurements.size() + 1) / 2, measurements.size()));
        double iqr = q3 - q1;
        return calculateUpperWhisker(measurements, q3, iqr);
    }

    @Override
    protected String assignAggregateType() {
        return AggregationType.WHISKER_U.getTypeId();
    }

    private double calculateMedian(List<Double> list) {
        int size = list.size();
        if (size % 2 == 0) {
            return (list.get(size / 2 - 1) + list.get(size / 2)) / 2.0;
        } else {
            return list.get(size / 2);
        }
    }

    private double calculateUpperWhisker(LinkedList<Double> list, double q3, double iqr) {
        double upperBound = q3 + 1.5 * iqr;
        for (int i = list.size() - 1; i >= 0; i--) {
            if (list.get(i) <= upperBound) {
                return list.get(i);
            }
        }
        return list.getLast();
    }


}
