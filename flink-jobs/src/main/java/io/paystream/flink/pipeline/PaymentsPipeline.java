package io.paystream.flink.pipeline;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Envelope;
import io.paystream.flink.model.KpiMinute;
import io.paystream.flink.model.Payment;
import io.paystream.flink.rules.AgentFloatSpikeRule;
import io.paystream.flink.rules.MuleFanInRule;
import io.paystream.flink.rules.NewDeviceCashOutRule;
import io.paystream.flink.rules.RuleConfig;
import io.paystream.flink.rules.VelocityRule;
import io.paystream.flink.rules.dynamic.DynamicKeyFunction;
import io.paystream.flink.rules.dynamic.DynamicRuleEvaluator;
import io.paystream.flink.rules.dynamic.RuleUpdates;
import java.time.Duration;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The processing graph, independent of where data comes from or goes to.
 * {@link io.paystream.flink.PaymentsJob} wires it to Kafka and Iceberg; tests wire it to collections.
 *
 * <pre>
 *  envelopes ─┬─ invalid ──────────────────────────────────────────────▶ dlq
 *             └─ valid ─┬──────────────────────────────────────────────▶ bronze (every copy)
 *                       └─ dedup by txn_id ─┬──────────────────────────▶ silver (one row per txn)
 *                                           ├─ 1-min KPI windows ─┬────▶ kpis
 *                                           │                     └────▶ late (missed their window)
 *                                           ├─ success ─ 4 keyed rules ▶ alerts
 *                                           └─ dynamic keys ─ analyst rules ▶ alerts
 *                                                  ▲
 *                     rule updates (broadcast) ────┘
 * </pre>
 */
public final class PaymentsPipeline {
    public static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(10);
    private static final OutputTag<Envelope> INVALID = new OutputTag<>("invalid") {};
    private static final OutputTag<Payment> LATE = new OutputTag<>("late") {};
    private static final Set<String> OUTGOING = Set.of("P2P", "MERCHANT_PAY", "CASH_OUT", "AIRTIME");

    public record Outputs(DataStream<Payment> bronze, DataStream<Payment> silver, DataStream<Alert> alerts,
                          DataStream<KpiMinute> kpis, DataStream<Payment> late, DataStream<Envelope> dlq) {}

    private PaymentsPipeline() {}

    /**
     * Event time comes from the payment; undecodable records use their Kafka timestamp so they
     * never hold the watermark back. Idle partitions are ignored after a minute.
     */
    public static WatermarkStrategy<Envelope> watermarks() {
        return WatermarkStrategy.<Envelope>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner((e, ts) -> e.isValid() ? e.payment.eventTime : e.ingestTime)
                .withIdleness(Duration.ofMinutes(1));
    }

    public static Outputs build(DataStream<Envelope> envelopes, RuleConfig rules, Duration dedupTtl) {
        return build(envelopes, rules, dedupTtl, null);
    }

    /** @param ruleUpdates analyst rules as published JSON (see {@link RuleUpdates}); null for none. */
    public static Outputs build(DataStream<Envelope> envelopes, RuleConfig rules, Duration dedupTtl,
                                DataStream<String> ruleUpdates) {
        SingleOutputStreamOperator<Payment> valid = envelopes
                .process(new ProcessFunction<Envelope, Payment>() {
                    @Override
                    public void processElement(Envelope e, Context ctx, Collector<Payment> out) {
                        if (e.isValid()) out.collect(e.payment); else ctx.output(INVALID, e);
                    }
                })
                .name("split-valid").uid("split-valid");

        DataStream<Payment> silver = valid
                .keyBy(p -> p.txnId)
                .process(new Dedup(dedupTtl))
                .name("dedup").uid("dedup");

        DataStream<Payment> success = silver.filter(Payment::isSuccess).name("success-only").uid("success-only");

        DataStream<Alert> velocity = success
                .filter(p -> OUTGOING.contains(p.txnType)).name("outgoing").uid("outgoing")
                .keyBy(p -> p.senderId)
                .process(new VelocityRule(rules)).name("rule-velocity").uid("rule-velocity");

        DataStream<Alert> newDevice = success
                .filter(p -> p.senderId.startsWith("U")).name("user-initiated").uid("user-initiated")
                .keyBy(p -> p.senderId)
                .process(new NewDeviceCashOutRule(rules)).name("rule-new-device").uid("rule-new-device");

        DataStream<Alert> mule = success
                .filter(p -> "P2P".equals(p.txnType)).name("p2p").uid("p2p")
                .keyBy(p -> p.receiverId)
                .process(new MuleFanInRule(rules)).name("rule-mule").uid("rule-mule");

        DataStream<Alert> agentSpike = success
                .filter(p -> "CASH_OUT".equals(p.txnType) && p.receiverId.startsWith("A"))
                .name("agent-cash-out").uid("agent-cash-out")
                .keyBy(p -> p.receiverId)
                .process(new AgentFloatSpikeRule(rules)).name("rule-agent-spike").uid("rule-agent-spike");

        DataStream<Alert> alerts = velocity.union(newDevice, mule, agentSpike);
        if (ruleUpdates != null) {
            // Dynamic rules see failed payments too: "5 failed PINs in 10 minutes" is a rule worth writing.
            var broadcast = RuleUpdates.broadcast(ruleUpdates);
            DataStream<Alert> dynamic = silver
                    .connect(broadcast).process(new DynamicKeyFunction()).name("dynamic-keys").uid("dynamic-keys")
                    .keyBy(k -> k.key)
                    .connect(broadcast).process(new DynamicRuleEvaluator())
                    .name("rule-dynamic").uid("rule-dynamic");
            alerts = alerts.union(dynamic);
        }

        SingleOutputStreamOperator<KpiMinute> kpis = silver
                .keyBy(p -> p.txnType)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .sideOutputLateData(LATE)
                .aggregate(new KpiAggregate(), new KpiAggregate.StampWindow())
                .name("kpi-1min").uid("kpi-1min");

        return new Outputs(valid, silver, alerts, kpis,
                kpis.getSideOutput(LATE), valid.getSideOutput(INVALID));
    }
}
