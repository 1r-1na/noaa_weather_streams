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
import at.fhv.streamprocessing.flink.util.TimeWindows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;
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


        // live data start
        localizedNoaaRecords
                .filter(NoaaRecord::isValidAirTemperature)
                .map(r -> new LiveDataRecord(r.wban(), Constants.MEASUREMENT_TYPE_TEMPERATURE, r.airTemperatureQualityCode().charAt(0), r.airTemperature(), Instant.ofEpochMilli(r.timestamp()), r.latitude(), r.longitude(), r.country()))
                .addSink(PostgresLiveDataSink.createSink())
                .name("postgres-temperature-live-data-sink");
        // live data end


        Arrays.stream(TimeWindows.values()).forEach(timeWindow -> {

            // minibatch aggregation quality code start
            localizedNoaaRecords
                    .filter(NoaaRecord::isValidAirTemperature)
                    .map(r -> QualityCodeRecord.forTemperatureOfLocalizedNoaaRecord(r, timeWindow.daysOfWindowByTimestamp(r.timestamp())))
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<QualityCodeRecord>forMonotonousTimestamps().withTimestampAssigner((e, ts) -> e.startTs().toEpochMilli()))
                    .keyBy(QualityCodeRecord::getKey)
                    .window(timeWindow.windowAssigner())
                    .aggregate(new QualityCodeRecordCounter())
                    .addSink(PostgresQualityCodeSink.createSink())
                    .name("postgres-temperature-quality-code-" + timeWindow.typeId() + "-sink");
            // minibatch aggregation quality code end


            // minibatch aggregation country start

            Arrays.stream(AggregationType.values()).forEach(type -> {

                DataStream<AggregatedDataRecord> temperatureStream = localizedNoaaRecords
                        .filter(NoaaRecord::isValidAirTemperature)
                        .map(r -> AggregatedDataRecord.forTemperatureOfLocalizedNoaaRecord(r, timeWindow.daysOfWindowByTimestamp(r.timestamp())));

                temperatureStream
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<AggregatedDataRecord>forMonotonousTimestamps().withTimestampAssigner((e, ts) -> e.startTs().toEpochMilli()))
                        .keyBy(AggregatedDataRecord::getKey)
                        .window(timeWindow.windowAssigner())
                        .aggregate(type.aggregateFunction())
                        .addSink(PostgresAggregatedDataSink.createSink())
                        .name("postgres-temperature-" + type.getTypeId() + "-" + timeWindow.typeId() + "-sink");

            });

            // minibatch aggregation country end

        });


        env.execute("weather-data-demo-job");
    }
}