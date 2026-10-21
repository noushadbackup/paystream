package io.paystream.flink.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import io.paystream.flink.model.Envelope;
import io.paystream.flink.model.Payment;
import io.paystream.flink.rules.RuleConfig;
import io.paystream.flink.sink.IcebergTables;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The whole job graph against a real Iceberg catalog on local disk: dedup, late data, DLQ, KPIs and
 * fraud alerts all land where they should. Only Kafka is swapped for an in-memory source.
 */
class PipelineIcebergIT {
    private static final long T0 = 1_790_000_000_000L;
    static final ConcurrentLinkedQueue<Envelope> DLQ = new ConcurrentLinkedQueue<>();

    @TempDir
    Path warehouse;

    @Test
    void endToEndIntoIceberg() throws Exception {
        List<Envelope> input = new ArrayList<>();
        int n = 0;
        // 3 minutes of normal traffic, 2 transfers per second from different users
        for (int s = 0; s < 180; s++) {
            for (int k = 0; k < 2; k++) {
                n++;
                input.add(Envelope.ok(Payment.of("N" + n, T0 + s * 1000L + k * 300, k == 0 ? "P2P" : "MERCHANT_PAY",
                        "U" + n, k == 0 ? "U" + (n + 1) : "M1", 50_000, "D" + n)));
            }
            if (s == 100) {
                // a velocity burst: 8 transfers from one account in 16 seconds
                for (int b = 0; b < 8; b++) {
                    input.add(Envelope.ok(Payment.of("B" + b, T0 + s * 1000L + b * 2000L, "P2P", "U_BURST",
                            "U" + b, 300_000, "D_BURST")));
                }
            }
        }
        input.add(input.get(10)); // an app retry: same txn_id again
        input.add(Envelope.failed(new byte[] {9, 9}, "bad", "payments.events", 0, 7, T0)); // undecodable
        input.add(Envelope.ok(Payment.of("LATE1", T0 + 5_000, "P2P", "U1", "U2", 10_000, "D1"))); // 3 min late

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Envelope> envelopes = env.fromData(input).assignTimestampsAndWatermarks(perEventWatermarks());
        PaymentsPipeline.Outputs out = PaymentsPipeline.build(envelopes, new RuleConfig(), Duration.ofHours(2));
        out.dlq().addSink(new SinkFunction<>() {
            @Override
            public void invoke(Envelope e, Context ctx) {
                DLQ.add(e);
            }
        });

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        Map<String, String> catalog = Map.of("catalog-type", "hadoop", "warehouse", warehouse.toUri().toString());
        IcebergTables.createCatalog(tEnv, catalog);
        StreamStatementSet set = tEnv.createStatementSet();
        IcebergTables.insertAll(tEnv, set, out);
        set.attachAsDataStream();
        env.execute("pipeline-it");

        TableEnvironment batch = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        batch.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        batch.executeSql("CREATE CATALOG lake WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
                + warehouse.toUri() + "')");

        long normal = 360, burst = 8;
        assertThat(count(batch, "SELECT count(*) FROM lake.bronze.payment_events")).isEqualTo(normal + burst + 2);
        assertThat(count(batch, "SELECT count(*) FROM lake.silver.payments")).isEqualTo(normal + burst + 1);
        assertThat(count(batch, "SELECT count(DISTINCT txn_id) FROM lake.silver.payments")).isEqualTo(normal + burst + 1);
        assertThat(count(batch, "SELECT count(*) FROM lake.silver.late_payments WHERE txn_id = 'LATE1'")).isEqualTo(1);
        assertThat(count(batch, "SELECT count(*) FROM lake.silver.fraud_alerts"
                + " WHERE rule = 'VELOCITY' AND account_id = 'U_BURST'")).isEqualTo(1);
        // the KPI windows hold every on-time transaction exactly once
        assertThat(count(batch, "SELECT sum(txn_count) FROM lake.gold.kpi_minute")).isEqualTo(normal + burst);
        assertThat(DLQ).singleElement().satisfies(e -> assertThat(e.offset).isEqualTo(7));
    }

    private static long count(TableEnvironment t, String sql) throws Exception {
        try (CloseableIterator<Row> it = t.executeSql(sql).collect()) {
            return ((Number) it.next().getField(0)).longValue();
        }
    }

    /** Test input arrives instantly, so emit a watermark after every event instead of periodically. */
    private static WatermarkStrategy<Envelope> perEventWatermarks() {
        return WatermarkStrategy.<Envelope>forGenerator(ctx -> new WatermarkGenerator<>() {
            long max = Long.MIN_VALUE;

            @Override
            public void onEvent(Envelope e, long ts, WatermarkOutput out) {
                max = Math.max(max, ts);
                out.emitWatermark(new Watermark(max - PaymentsPipeline.MAX_OUT_OF_ORDERNESS.toMillis()));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput out) {}
        }).withTimestampAssigner((e, ts) -> e.isValid() ? e.payment.eventTime : e.ingestTime);
    }
}
