package at.fhv.streamprocessing.flink.util;

import at.fhv.streamprocessing.flink.function.window.DailyWindowAssigner;
import at.fhv.streamprocessing.flink.function.window.MonthlyWindowAssigner;
import at.fhv.streamprocessing.flink.function.window.WeeklyWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Supplier;

public enum TimeWindows {
    DAY("DAY", DailyWindowAssigner::new),
    WEEK("WEEK", WeeklyWindowAssigner::new),
    MONTH("MONTH", MonthlyWindowAssigner::new);

    private final String typeId;
    private final Supplier<WindowAssigner<Object, TimeWindow>> windowAssignerSupplier;


    TimeWindows(String typeId, Supplier<WindowAssigner<Object, TimeWindow>> windowAssignerSupplier) {
        this.typeId = typeId;
        this.windowAssignerSupplier = windowAssignerSupplier;
    }

    public WindowAssigner<Object, TimeWindow> windowAssigner() {
        return windowAssignerSupplier.get();
    }

    public String typeId() {
        return typeId;
    }

    public int daysOfWindowByTimestamp(long timestamp) {

        if (this == DAY) {
            return 1;
        }

        if (this == WEEK) {
            return 7;
        }

        if (this == MONTH) {
            ZonedDateTime zdt = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
            int year = zdt.getYear();
            int month = zdt.getMonthValue();
            return YearMonth.of(year, month).lengthOfMonth();
        }

        throw new IllegalStateException("No known TimeWindow " + this);
    }

}

