package at.fhv.streamprocessing.flink.job;

import at.fhv.streamprocessing.flink.Constants;
import at.fhv.streamprocessing.flink.function.aggregate.QualityCodeRecordCounter;
import at.fhv.streamprocessing.flink.function.process.NoaaMildBroadcastProcessFunction;
import at.fhv.streamprocessing.flink.function.sink.PostgresAggregatedDataSink;
import at.fhv.streamprocessing.flink.function.sink.PostgresLiveDataSink;
import at.fhv.streamprocessing.flink.function.sink.PostgresQualityCodeSink;
import at.fhv.streamprocessing.flink.record.*;
import at.fhv.streamprocessing.flink.function.process.NoaaRecordParseProcessFunction;
import at.fhv.streamprocessing.flink.function.source.FtpDataSource;
import at.fhv.streamprocessing.flink.function.source.MlidDataSource;
import at.fhv.streamprocessing.flink.util.AggregationType;
import at.fhv.streamprocessing.flink.util.MeasurementTypes;
import at.fhv.streamprocessing.flink.util.TimeWindows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

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


        Arrays.stream(MeasurementTypes.values()).forEach(measurementType -> {

            // live data start
            localizedNoaaRecords
                    .filter(measurementType::filter)
                    .map(measurementType::noaaToLive)
                    .addSink(PostgresLiveDataSink.createSink())
                    .name("postgres-" + measurementType.measurementTypeId() + "-live-data-sink");
            // live data end

            Arrays.stream(TimeWindows.values()).forEach(timeWindow -> {

                // minibatch aggregation quality code start
                localizedNoaaRecords
                        .filter(measurementType::filter)
                        .map(r -> measurementType.noaaToQualityCode(r, timeWindow.daysOfWindowByTimestamp(r.timestamp())))
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<QualityCodeRecord>forMonotonousTimestamps().withTimestampAssigner((e, ts) -> e.startTs().toEpochMilli()))
                        .keyBy(QualityCodeRecord::getKey)
                        .window(timeWindow.windowAssigner())
                        .aggregate(new QualityCodeRecordCounter())
                        .addSink(PostgresQualityCodeSink.createSink())
                        .name("postgres-" + measurementType.measurementTypeId() +  "-quality-code-" + timeWindow.typeId() + "-sink");
                // minibatch aggregation quality code end


                // minibatch aggregation country start

                Arrays.stream(AggregationType.values()).forEach(aggregationType -> {

                    localizedNoaaRecords
                            .filter(measurementType::filter)
                            .map(r -> measurementType.noaaToAggregated(r, timeWindow.daysOfWindowByTimestamp(r.timestamp())))
                            .assignTimestampsAndWatermarks(WatermarkStrategy.<AggregatedDataRecord>forMonotonousTimestamps().withTimestampAssigner((e, ts) -> e.startTs().toEpochMilli()))
                            .keyBy(AggregatedDataRecord::getKey)
                            .window(timeWindow.windowAssigner())
                            .aggregate(aggregationType.aggregateFunction())
                            .addSink(PostgresAggregatedDataSink.createSink())
                            .name("postgres-" + measurementType.measurementTypeId() + "-" + aggregationType.getTypeId() + "-" + timeWindow.typeId() + "-sink");

                });

                // minibatch aggregation country end

            });

        });


        env.execute("weather-data-demo-job");

    }
}