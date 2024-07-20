package at.fhv.streamprocessing.flink.function.window;

import at.fhv.streamprocessing.flink.record.SingleValueRecord;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowDoubleAndCountFunction implements WindowFunction<SingleValueRecord, SingleValueRecord, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow window, Iterable<SingleValueRecord> input, Collector<SingleValueRecord> out) throws Exception {
        SingleValueRecord value = input.iterator().next();
        out.collect(value);
    }
}
