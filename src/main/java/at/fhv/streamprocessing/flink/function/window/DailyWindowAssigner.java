package at.fhv.streamprocessing.flink.function.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;

import static java.time.ZoneId.systemDefault;

public class DailyWindowAssigner extends WindowAssigner<Object, TimeWindow> {
    
    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), systemDefault());
        ZonedDateTime startOfDay = zonedDateTime.toLocalDate().atStartOfDay(systemDefault());
        long start = startOfDay.toInstant().toEpochMilli();
        long end = startOfDay.plusDays(1).toInstant().toEpochMilli();
        return Collections.singletonList(new TimeWindow(start, end));
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
