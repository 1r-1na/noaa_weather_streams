package at.fhv.streamprocessing.flink.function.aggregate;

import at.fhv.streamprocessing.flink.util.AggregationType;

import java.util.LinkedList;
import java.util.List;

public class StandardDeviationAggregate extends AbstractValueKeepingAggregate {

    @Override
    protected double calculateValue(LinkedList<Double> measurements) {
        double mean = calculateMean(measurements);
        return calculateStandardDeviation(measurements, mean);
    }

    @Override
    protected String assignAggregateType() {
        return AggregationType.STANDARD_DEVIATION.getTypeId();
    }

    private double calculateMean(List<Double> list) {
        double sum = 0.0;
        for (double value : list) {
            sum += value;
        }
        return sum / list.size();
    }

    private double calculateStandardDeviation(List<Double> list, double mean) {
        double sumSquaredDifferences = 0.0;
        for (double value : list) {
            double difference = value - mean;
            sumSquaredDifferences += difference * difference;
        }
        double variance = sumSquaredDifferences / list.size();
        return Math.sqrt(variance);
    }
}
