package io.paystream.flink.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.paystream.flink.model.Envelope;
import io.paystream.flink.model.Payment;
import io.paystream.flink.online.RedisSink;
import io.paystream.flink.pipeline.PaymentsPipeline;
import io.paystream.flink.rules.RuleConfig;
import io.paystream.flink.rules.dynamic.RuleUpdates;
import io.paystream.flink.sink.IcebergTables;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Replays a generator file through the production job graph as fast as it will go and reports what
 * it measured: the same operators, RocksDB state with incremental exactly-once checkpoints, every
 * Iceberg table, and the online store written to a real Redis. Only Kafka is replaced, by a file
 * read in arrival order, so the number is the job's processing capacity, not the cluster's.
 *
 * <pre>
 * --events FILE  --work DIR  [--rules ../rules/library] [--parallelism N] [--checkpointMs 10000]
 * [--redisPort 6379 | 0 to discard the online store writes] [--fromSavepoint DIR] [--maxParallelism 128]
 * </pre>
 * {@code --fromSavepoint} starts from a savepoint written by {@code DeviceHistoryBootstrap}, as a
 * production deployment would, so account-takeover detection has device history.
 * Writes {@code DIR/flink.json} (runtime, row counts) and {@code DIR/alerts.jsonl} for scoring.
 */
public final class ReplayBenchmark {
    private ReplayBenchmark() {}

    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        Path events = Path.of(p.getRequired("events")).toAbsolutePath();
        Path work = Path.of(p.getRequired("work")).toAbsolutePath();
        Path rulesDir = Path.of(p.get("rules", "../rules/library"));
        int parallelism = p.getInt("parallelism", Runtime.getRuntime().availableProcessors());
        int redisPort = p.getInt("redisPort", 6379);
        Files.createDirectories(work);

        Configuration conf = new Configuration();
        conf.set(StateBackendOptions.STATE_BACKEND, p.get("stateBackend", "rocksdb"));
        conf.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, work.resolve("checkpoints").toUri().toString());
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse(p.get("managedMemory", "6g")));
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("256m"));
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("256m"));
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "none"); // a run that restarted is not a measurement
        conf.set(PipelineOptions.MAX_PARALLELISM, p.getInt("maxParallelism", 128));
        if (p.has("fromSavepoint")) conf.set(StateRecoveryOptions.SAVEPOINT_PATH, p.get("fromSavepoint"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        env.enableCheckpointing(p.getLong("checkpointMs", 10_000), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2_000);

        // one reader keeps arrival order, as one Kafka partition per key would; parsing fans out
        FileSource<String> file = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new org.apache.flink.core.fs.Path(events.toUri()))
                .build();
        DataStream<Envelope> envelopes = env
                .fromSource(file, WatermarkStrategy.noWatermarks(), "events").setParallelism(1)
                .rebalance()
                .map(new Parse(p.getLong("holdMs", 2_000))).name("parse")
                .assignTimestampsAndWatermarks(PaymentsPipeline.watermarks());

        List<String> rules = new ArrayList<>();
        try (Stream<Path> files = Files.list(rulesDir)) {
            for (Path f : files.filter(x -> x.toString().endsWith(".json")).sorted().toList()) {
                rules.add(Files.readString(f));
            }
        }
        DataStream<String> ruleUpdates = env.fromData(rules).setParallelism(1)
                .assignTimestampsAndWatermarks(RuleUpdates.watermarks());

        PaymentsPipeline.Outputs out = PaymentsPipeline.build(envelopes, new RuleConfig(), Duration.ofHours(2),
                ruleUpdates);
        var online = PaymentsPipeline.onlineStore(out, Duration.ofMillis(200), Duration.ofHours(1));
        if (redisPort > 0) {
            online.sinkTo(new RedisSink("localhost", redisPort, 500, 50)).name("redis");
        } else {
            online.sinkTo(new DiscardingSink<>()).name("online-discard");
        }
        out.dlq().sinkTo(new DiscardingSink<>()).name("dlq");
        if (p.has("printAlerts")) out.alerts().map(a -> "ALERT " + a.alertId).print();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        String warehouse = work.resolve("warehouse").toUri().toString();
        IcebergTables.createCatalog(tEnv, Map.of("catalog-type", "hadoop", "warehouse", warehouse));
        StreamStatementSet set = tEnv.createStatementSet();
        IcebergTables.insertAll(tEnv, set, out);
        set.attachAsDataStream();

        long t0 = System.nanoTime();
        JobExecutionResult result = env.execute("paystream-replay");
        long wallMs = (System.nanoTime() - t0) / 1_000_000;

        ObjectMapper json = new ObjectMapper();
        ObjectNode report = json.createObjectNode();
        report.put("parallelism", parallelism);
        report.put("net_runtime_ms", result.getNetRuntime());
        report.put("wall_ms", wallMs);
        report.put("online_store", redisPort > 0 ? "redis" : "discarded");
        report.put("from_savepoint", p.has("fromSavepoint"));
        report.put("processors", Runtime.getRuntime().availableProcessors());
        report.put("max_heap_mb", Runtime.getRuntime().maxMemory() >> 20);

        report.set("counts", LakeReport.counts(warehouse, json));
        LakeReport.exportAlerts(warehouse, work.resolve("alerts.jsonl"), json, false);
        json.writerWithDefaultPrettyPrinter().writeValue(work.resolve("flink.json").toFile(), report);
        System.out.println(json.writerWithDefaultPrettyPrinter().writeValueAsString(report));
    }

    /** A generator line to the envelope the Kafka deserializer would produce; ingest time is event time. */
    static final class Parse extends RichMapFunction<String, Envelope> {
        private final long holdMs;
        private transient ObjectMapper json;
        private transient int subtask;
        private transient long offset;
        private transient boolean started;

        Parse(long holdMs) {
            this.holdMs = holdMs;
        }

        @Override
        public void open(OpenContext ctx) {
            json = new ObjectMapper();
            subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        }

        @Override
        public Envelope map(String line) throws Exception {
            if (!started) {
                Thread.sleep(holdMs); // rule publication precedes traffic, as on a running cluster
                started = true;
            }
            JsonNode n = json.readTree(line);
            Payment p = new Payment();
            p.txnId = n.get("txn_id").asText();
            p.eventTime = n.get("event_time").asLong();
            p.txnType = n.get("txn_type").asText();
            p.senderId = n.get("sender_id").asText();
            p.receiverId = n.get("receiver_id").asText();
            p.amountPaisa = n.get("amount_paisa").asLong();
            p.currency = n.get("currency").asText();
            p.deviceId = n.hasNonNull("device_id") ? n.get("device_id").asText() : null;
            p.district = n.hasNonNull("district") ? n.get("district").asText() : null;
            p.status = n.get("status").asText();
            p.channel = n.hasNonNull("channel") ? n.get("channel").asText() : null;
            p.ingestTime = p.eventTime;
            p.kafkaPartition = subtask;
            p.kafkaOffset = offset++;
            return Envelope.ok(p);
        }
    }
}
