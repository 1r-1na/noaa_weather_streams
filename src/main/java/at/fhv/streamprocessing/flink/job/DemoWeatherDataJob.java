package at.fhv.streamprocessing.flink.job;

import at.fhv.streamprocessing.flink.Constants;
import at.fhv.streamprocessing.flink.process.NoaaMildBroadcastProcessFunction;
import at.fhv.streamprocessing.flink.record.LocalizedNoaaRecord;
import at.fhv.streamprocessing.flink.record.MlidRecord;
import at.fhv.streamprocessing.flink.record.NoaaRecord;
import at.fhv.streamprocessing.flink.process.NoaaRecordParseProcessFunction;
import at.fhv.streamprocessing.flink.sink.GenericLoggingSink;
import at.fhv.streamprocessing.flink.source.FtpDataSource;
import at.fhv.streamprocessing.flink.source.MlidDataSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<NoaaRecord>forMonotonousTimestamps().withTimestampAssigner((e, ts) -> e.timestamp()))
                .name("noah-record-parser");

        DataStream<LocalizedNoaaRecord> joinedStream = noaaRecords
                .connect(mlidStream)
                .process(new NoaaMildBroadcastProcessFunction());

        joinedStream
                .addSink(new GenericLoggingSink<>("joined-data"))
                .name("joined-data-sink");

        env.execute("weather-data-demo-job");
    }
}