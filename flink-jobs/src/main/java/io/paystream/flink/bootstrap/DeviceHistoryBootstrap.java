package io.paystream.flink.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.paystream.flink.ClientConfig;
import io.paystream.flink.rules.NewDeviceCashOutRule;
import io.paystream.flink.sink.IcebergTables;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.StateBootstrapTransformation;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Writes a savepoint holding every account's known devices, so the payments job starts with the
 * history {@link NewDeviceCashOutRule} needs instead of learning it from scratch. Without it, a new
 * deployment or a state reset leaves account-takeover detection blind until accounts have been
 * seen again on their usual phones, which for most customers takes days.
 *
 * <pre>
 * # from the lakehouse (production): the last 90 days of silver.payments
 * DeviceHistoryBootstrap --savepoint s3://.../bootstrap/device-history --maxParallelism 480 --warehouse ...
 * # from a generator file (benchmarks)
 * DeviceHistoryBootstrap --savepoint file:///... --maxParallelism 128 --jsonl history.jsonl
 * </pre>
 * Then start the job from it: {@code flink run -s <savepoint> ...}. Only this operator's state is
 * in the savepoint; every other operator starts empty, as on a fresh deployment. The max parallelism
 * must equal the job's, since it fixes how keys map to state.
 */
public final class DeviceHistoryBootstrap {
    private DeviceHistoryBootstrap() {}

    /** One account used one device, first at {@code firstSeen} (epoch millis, event time). */
    public static class DeviceSeen {
        public String accountId;
        public String deviceId;
        public long firstSeen;

        public DeviceSeen() {}

        public DeviceSeen(String accountId, String deviceId, long firstSeen) {
            this.accountId = accountId;
            this.deviceId = deviceId;
            this.firstSeen = firstSeen;
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<DeviceSeen> history = p.has("jsonl")
                ? fromJsonl(env, p.get("jsonl"))
                : fromLakehouse(env, p, p.getInt("days", 90));
        write(history, p.getRequired("savepoint"), p.getInt("maxParallelism", 480));
        env.execute("paystream-device-history-bootstrap");
    }

    /** Adds the savepoint write to {@code history}'s environment; run it with {@code env.execute()}. */
    public static void write(DataStream<DeviceSeen> history, String savepoint, int maxParallelism) {
        StateBootstrapTransformation<DeviceSeen> devices = OperatorTransformation
                .bootstrapWith(history)
                .keyBy(d -> d.accountId)
                .transform(new Devices());
        SavepointWriter.newSavepoint(history.getExecutionEnvironment(), new EmbeddedRocksDBStateBackend(true),
                        maxParallelism)
                .withOperator(OperatorIdentifier.forUid(NewDeviceCashOutRule.UID), devices)
                .write(savepoint);
    }

    /** The same rows the rule would have learned: successful, user-initiated payments with a device. */
    static DataStream<DeviceSeen> fromLakehouse(StreamExecutionEnvironment env, ParameterTool p, int days) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        IcebergTables.createCatalog(tEnv, ClientConfig.lakehouse(p));
        var rows = tEnv.sqlQuery("SELECT sender_id, device_id, min(event_time) FROM lake.silver.payments"
                + " WHERE status = 'SUCCESS' AND sender_id LIKE 'U%' AND device_id IS NOT NULL"
                + " AND event_date >= CURRENT_DATE - INTERVAL '" + days + "' DAY"
                + " GROUP BY sender_id, device_id");
        return tEnv.toDataStream(rows).map((Row r) -> new DeviceSeen((String) r.getField(0), (String) r.getField(1),
                ((Instant) r.getField(2)).toEpochMilli())).returns(DeviceSeen.class).name("device-history");
    }

    static DataStream<DeviceSeen> fromJsonl(StreamExecutionEnvironment env, String path) {
        FileSource<String> file = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(path)).build();
        ObjectMapper json = new ObjectMapper();
        return env.fromSource(file, WatermarkStrategy.noWatermarks(), "history")
                .flatMap((String line, Collector<DeviceSeen> out) -> {
                    JsonNode n = json.readTree(line);
                    String sender = n.get("sender_id").asText();
                    if ("SUCCESS".equals(n.get("status").asText()) && sender.startsWith("U") && n.hasNonNull("device_id")) {
                        out.collect(new DeviceSeen(sender, n.get("device_id").asText(), n.get("event_time").asLong()));
                    }
                })
                .returns(DeviceSeen.class).name("device-history");
    }

    /** Keeps the earliest sighting per device, as the rule does. */
    static final class Devices extends KeyedStateBootstrapFunction<String, DeviceSeen> {
        private transient MapState<String, Long> devices;

        @Override
        public void open(OpenContext ctx) {
            devices = getRuntimeContext().getMapState(NewDeviceCashOutRule.historyDescriptor());
        }

        @Override
        public void processElement(DeviceSeen d, Context ctx) throws Exception {
            Long seen = devices.get(d.deviceId);
            if (seen == null || d.firstSeen < seen) devices.put(d.deviceId, d.firstSeen);
        }
    }
}
