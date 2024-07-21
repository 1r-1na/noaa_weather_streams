package at.fhv.streamprocessing.flink;

import at.fhv.streamprocessing.flink.record.MlidRecord;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;

public class Constants {

    public static MapStateDescriptor<String, MlidRecord> MLID_DESCRIPTOR = new MapStateDescriptor<>("mild-descriptor", Types.STRING, Types.POJO(MlidRecord.class));

    public static JdbcExecutionOptions JDBC_EXECUTION_OPTIONS = new JdbcExecutionOptions.Builder()
            .withBatchIntervalMs(1000)
            .withBatchSize(100)
            .build();

    public static JdbcConnectionOptions JDBC_CONNECTION_OPTIONS = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withDriverName("org.postgresql.Driver")
            .withUrl("jdbc:postgresql://postgres:5432/default")
            .withUsername("dev_user")
            .withPassword("dev_pw")
            .build();

}
