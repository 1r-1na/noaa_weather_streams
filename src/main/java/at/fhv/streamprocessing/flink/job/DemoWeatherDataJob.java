package at.fhv.streamprocessing.flink.job;

import at.fhv.streamprocessing.flink.record.MasterLocationIdentifierRecord;
import at.fhv.streamprocessing.flink.record.NoaaRecord;
import at.fhv.streamprocessing.flink.process.NoaaRecordParseProcessFunction;
import at.fhv.streamprocessing.flink.sink.GenericLoggingSink;
import at.fhv.streamprocessing.flink.source.FtpDataSource;
import at.fhv.streamprocessing.flink.source.MlidDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DemoWeatherDataJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<MasterLocationIdentifierRecord> mlidDataStream = MlidDataSource
                .getMlidDataStream(env)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<MasterLocationIdentifierRecord>forMonotonousTimestamps().withTimestampAssigner((e, ts) -> e.timestamp));

        DataStream<NoaaRecord> noaaRecords = env
                .addSource(new FtpDataSource())
                .keyBy(a -> a)
                .process(new NoaaRecordParseProcessFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<NoaaRecord>forMonotonousTimestamps().withTimestampAssigner((e, ts) -> e.timestamp()))
                .name("noah-record-parser");

        DataStream<Tuple2<NoaaRecord, MasterLocationIdentifierRecord>> joinedStream = noaaRecords
                .keyBy(NoaaRecord::wban)
                .intervalJoin(mlidDataStream.keyBy(a -> StringUtils.leftPad(a.wban, 5, '0')))
                .between(Duration.ofSeconds(-50), Duration.ofSeconds(50))
                .process(new ProcessJoinFunction<>() {
                    @Override
                    public void processElement(NoaaRecord left, MasterLocationIdentifierRecord right, Context ctx, Collector<Tuple2<NoaaRecord, MasterLocationIdentifierRecord>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        joinedStream
                .addSink(new GenericLoggingSink<>("joined-data"))
                .name("joined-data-sink");

        env.execute("weather-data-demo-job");
    }
}