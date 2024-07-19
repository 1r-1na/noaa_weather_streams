package at.fhv.streamprocessing.flink;

import at.fhv.streamprocessing.flink.record.MlidRecord;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;

public class Constants {

    public static MapStateDescriptor<String, MlidRecord> MLID_DESCRIPTOR = new MapStateDescriptor<>("mild-descriptor", Types.STRING, Types.POJO(MlidRecord.class));

}
