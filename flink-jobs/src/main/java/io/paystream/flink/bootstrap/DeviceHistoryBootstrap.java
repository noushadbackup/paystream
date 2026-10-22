package io.paystream.flink.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.paystream.flink.ClientConfig;
import io.paystream.flink.rules.NewDeviceCashOutRule;
import io.paystream.flink.rules.RuleConfig;
import io.paystream.flink.rules.VelocityRule;
import io.paystream.flink.sink.IcebergTables;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
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
 * <p>It also writes each account's recent outgoing rate for {@link VelocityRule} (the last
 * {@code velocityHours}, 6 by default, up to {@code asOf}). Without it the rule meets every busy
 * account with the prior of an ordinary one and flags it until the rate has been learned.
 *
 * <pre>
 * # from the lakehouse (production): the last 90 days of silver.payments
 * DeviceHistoryBootstrap --savepoint s3://.../bootstrap/device-history --maxParallelism 480 --warehouse ...
 * # from a generator file (benchmarks)
 * DeviceHistoryBootstrap --savepoint file:///... --maxParallelism 128 --jsonl history.jsonl \
 *     --asOf 2026-10-01T00:00:00Z
 * </pre>
 * From a file, the outgoing rates are written only when {@code --asOf} (the end of the history) is
 * given; from the lakehouse it defaults to now.
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

    /** One successful outgoing transfer (what {@link VelocityRule} counts). */
    public static class Outgoing {
        public String accountId;
        public long eventTime;

        public Outgoing() {}

        public Outgoing(String accountId, long eventTime) {
            this.accountId = accountId;
            this.eventTime = eventTime;
        }
    }

    /** The transaction types {@link VelocityRule} sees, as in the payments pipeline. */
    static final Set<String> OUTGOING = Set.of("P2P", "MERCHANT_PAY", "CASH_OUT", "AIRTIME");

    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        RuleConfig rules = RuleConfig.from(p);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        long hours = p.getLong("velocityHours", 6);
        DataStream<DeviceSeen> history;
        DataStream<Outgoing> outgoing = null;
        long asOf;
        if (p.has("jsonl")) {
            history = fromJsonl(env, p.get("jsonl"));
            asOf = p.has("asOf") ? Instant.parse(p.get("asOf")).toEpochMilli() : Long.MIN_VALUE;
            if (p.has("asOf")) outgoing = outgoingFromJsonl(env, p.get("jsonl"), asOf - hours * 3_600_000L, asOf);
        } else {
            history = fromLakehouse(env, p, p.getInt("days", 90));
            asOf = p.has("asOf") ? Instant.parse(p.get("asOf")).toEpochMilli() : System.currentTimeMillis();
            outgoing = outgoingFromLakehouse(env, p, asOf - hours * 3_600_000L, asOf);
        }
        write(history, outgoing, asOf, hours * 3_600_000L, rules, p.getRequired("savepoint"),
                p.getInt("maxParallelism", 480));
        env.execute("paystream-device-history-bootstrap");
    }

    /** Adds the savepoint write to {@code history}'s environment; run it with {@code env.execute()}. */
    public static void write(DataStream<DeviceSeen> history, String savepoint, int maxParallelism) {
        write(history, null, 0, 0, new RuleConfig(), savepoint, maxParallelism);
    }

    /**
     * As above, plus each account's outgoing transfers in the {@code watchedMs} before {@code asOf}
     * for {@link VelocityRule}, unless {@code outgoing} is null.
     */
    public static void write(DataStream<DeviceSeen> history, DataStream<Outgoing> outgoing, long asOf,
            long watchedMs, RuleConfig rules, String savepoint, int maxParallelism) {
        StateBootstrapTransformation<DeviceSeen> devices = OperatorTransformation
                .bootstrapWith(history)
                .keyBy(d -> d.accountId)
                .transform(new Devices());
        var writer = SavepointWriter.newSavepoint(history.getExecutionEnvironment(),
                        new EmbeddedRocksDBStateBackend(true), maxParallelism)
                .withOperator(OperatorIdentifier.forUid(NewDeviceCashOutRule.UID), devices);
        if (outgoing != null) {
            writer = writer.withOperator(OperatorIdentifier.forUid(VelocityRule.UID), OperatorTransformation
                    .bootstrapWith(outgoing)
                    .keyBy(o -> o.accountId)
                    .transform(new Rates(asOf, watchedMs, rules)));
        }
        writer.write(savepoint);
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

    static DataStream<Outgoing> outgoingFromLakehouse(StreamExecutionEnvironment env, ParameterTool p, long from,
            long to) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        IcebergTables.createCatalog(tEnv, ClientConfig.lakehouse(p));
        var rows = tEnv.sqlQuery("SELECT sender_id, event_time FROM lake.silver.payments"
                + " WHERE status = 'SUCCESS' AND txn_type IN ('" + String.join("', '", OUTGOING) + "')"
                + " AND event_time > TO_TIMESTAMP_LTZ(" + from + ", 3) AND event_time <= TO_TIMESTAMP_LTZ(" + to + ", 3)");
        return tEnv.toDataStream(rows).map((Row r) -> new Outgoing((String) r.getField(0),
                ((Instant) r.getField(1)).toEpochMilli())).returns(Outgoing.class).name("outgoing-history");
    }

    static DataStream<Outgoing> outgoingFromJsonl(StreamExecutionEnvironment env, String path, long from, long to) {
        FileSource<String> file = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(path)).build();
        ObjectMapper json = new ObjectMapper();
        return env.fromSource(file, WatermarkStrategy.noWatermarks(), "outgoing")
                .flatMap((String line, Collector<Outgoing> out) -> {
                    JsonNode n = json.readTree(line);
                    long t = n.get("event_time").asLong();
                    if ("SUCCESS".equals(n.get("status").asText()) && OUTGOING.contains(n.get("txn_type").asText())
                            && t > from && t <= to) {
                        out.collect(new Outgoing(n.get("sender_id").asText(), t));
                    }
                })
                .returns(Outgoing.class).name("outgoing-history");
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

    /** Each account's decayed count of outgoing transfers up to {@code asOf}, as the rule keeps it. */
    static final class Rates extends KeyedStateBootstrapFunction<String, Outgoing> {
        private final long asOf;
        private final long watchedMs;
        private final RuleConfig rules;
        private transient ValueState<double[]> history;

        Rates(long asOf, long watchedMs, RuleConfig rules) {
            this.asOf = asOf;
            this.watchedMs = watchedMs;
            this.rules = rules;
        }

        @Override
        public void open(OpenContext ctx) {
            history = getRuntimeContext().getState(VelocityRule.historyDescriptor(rules));
        }

        @Override
        public void processElement(Outgoing o, Context ctx) throws Exception {
            double[] h = history.value();
            if (h == null) h = VelocityRule.emptyHistory(asOf, watchedMs, rules);
            VelocityRule.addToHistory(h, o.eventTime, rules);
            history.update(h);
        }
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
