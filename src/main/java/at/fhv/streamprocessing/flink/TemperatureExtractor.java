package at.fhv.streamprocessing.flink;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TemperatureExtractor extends KeyedProcessFunction<String, String, Double> {
    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(String measurement, Context context, Collector<Double> collector) throws Exception {
        String tempString = measurement.substring(87,92);
        double temp = Double.parseDouble(tempString) / 10;
        collector.collect(temp);
    }
}
