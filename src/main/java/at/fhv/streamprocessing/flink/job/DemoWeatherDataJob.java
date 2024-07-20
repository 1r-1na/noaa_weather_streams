package at.fhv.streamprocessing.flink.job;

import at.fhv.streamprocessing.flink.Constants;
import at.fhv.streamprocessing.flink.function.aggregate.AverageAggregate;
import at.fhv.streamprocessing.flink.function.process.NoaaMildBroadcastProcessFunction;
import at.fhv.streamprocessing.flink.function.sink.PostgresAggregatedDataSink;
import at.fhv.streamprocessing.flink.function.window.WindowDoubleAndCountFunction;
import at.fhv.streamprocessing.flink.record.*;
import at.fhv.streamprocessing.flink.function.process.NoaaRecordParseProcessFunction;
import at.fhv.streamprocessing.flink.function.source.FtpDataSource;
import at.fhv.streamprocessing.flink.function.source.MlidDataSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.time.Instant;

public class DemoWeatherDataJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        BroadcastStream<MlidRecord> mlidStream = MlidDataSource.getMlidDataStream(env)
                .broadcast(Constants.MLID_DESCRIPTOR);

        DataStream<NoaaRecord> noaaRecords = env
                .addSource(new FtpDataSource())
                .keyBy(a -> a)
                .process(new NoaaRecordParseProcessFunction())
                .name("noah-record-parser");

        DataStream<LocalizedNoaaRecord> localizedNoaaRecords = noaaRecords
                .connect(mlidStream)
                .process(new NoaaMildBroadcastProcessFunction());

        DataStream<SingleValueRecord> temperatureStream = localizedNoaaRecords
                .filter(NoaaRecord::isValidAirTemperature)
                .map(r -> new SingleValueRecord(r.airTemperature(), r.country(), r.timestamp()));

        DataStream<SingleValueRecord> avgTempPerDayAndCountry = temperatureStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SingleValueRecord>forMonotonousTimestamps().withTimestampAssigner((e, ts) -> e.timestamp()))
                .keyBy(SingleValueRecord::country)
                .window(TumblingEventTimeWindows.of(Duration.ofDays(1)))
                .aggregate(new AverageAggregate(), new WindowDoubleAndCountFunction());

        avgTempPerDayAndCountry
                .map(r -> new AggregatedDataRecord(r.country(), "TEMPERATURE", "AVG", r.value(), Instant.ofEpochMilli(r.timestamp()), 1))
                .addSink(PostgresAggregatedDataSink.createSink())
                .name("postgres-sink");

//        avgTempPerDayAndCountry
//                .addSink(new GenericLoggingSink<>("joined-data"))
//                .name("joined-data-sink");

        env.execute("weather-data-demo-job");
    }
}