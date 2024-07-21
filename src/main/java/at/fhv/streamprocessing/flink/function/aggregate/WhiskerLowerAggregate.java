package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.util.AggregationType;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class WhiskerLowerAggregate extends AbstractValueKeepingAggregate {

    @Override
    protected double calculateValue(LinkedList<Double> measurements) {
        Collections.sort(measurements);
        double q1 = calculateMedian(measurements.subList(0, measurements.size() / 2));
        double q3 = calculateMedian(measurements.subList((measurements.size() + 1) / 2, measurements.size()));
        double iqr = q3 - q1;
        return calculateLowerWhisker(measurements, q1, iqr);
    }

    @Override
    protected String assignAggregateType() {
        return AggregationType.WHISKER_L.getTypeId();
    }

    private double calculateMedian(List<Double> list) {
        int size = list.size();
        if (size % 2 == 0) {
            return (list.get(size / 2 - 1) + list.get(size / 2)) / 2.0;
        } else {
            return list.get(size / 2);
        }
    }

    private double calculateLowerWhisker(LinkedList<Double> list, double q1, double iqr) {
        double lowerBound = q1 - 1.5 * iqr;
        for (double value : list) {
            if (value >= lowerBound) {
                return value;
            }
        }
        return list.getFirst();
    }
}
