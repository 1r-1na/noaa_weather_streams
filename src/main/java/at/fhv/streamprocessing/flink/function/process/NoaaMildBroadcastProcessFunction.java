package at.fhv.streamprocessing.flink.function.process;

import at.fhv.streamprocessing.flink.util.Constants;
import at.fhv.streamprocessing.flink.record.LocalizedNoaaRecord;
import at.fhv.streamprocessing.flink.record.MlidRecord;
import at.fhv.streamprocessing.flink.record.NoaaRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class NoaaMildBroadcastProcessFunction extends BroadcastProcessFunction<NoaaRecord, MlidRecord, LocalizedNoaaRecord> {

    @Override
    public void processElement(NoaaRecord value, BroadcastProcessFunction<NoaaRecord, MlidRecord, LocalizedNoaaRecord>.ReadOnlyContext ctx, Collector<LocalizedNoaaRecord> out) throws Exception {
        ReadOnlyBroadcastState<String, MlidRecord> state = ctx.getBroadcastState(Constants.MLID_DESCRIPTOR);
        MlidRecord matchingRecord = state.get(value.wban());
        if (matchingRecord != null) {
            out.collect(new LocalizedNoaaRecord(value, matchingRecord));
        }
    }

    @Override
    public void processBroadcastElement(MlidRecord value, BroadcastProcessFunction<NoaaRecord, MlidRecord, LocalizedNoaaRecord>.Context ctx, Collector<LocalizedNoaaRecord> out) throws Exception {
        BroadcastState<String, MlidRecord> state = ctx.getBroadcastState(Constants.MLID_DESCRIPTOR);
        state.put(StringUtils.leftPad(value.wban, 5, '0'), value);
    }

}
