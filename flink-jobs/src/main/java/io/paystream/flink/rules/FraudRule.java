package io.paystream.flink.rules;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Base for keyed fraud rules.
 *
 * <p>Rules fire as soon as an event arrives instead of waiting for the watermark, because waiting
 * would add the out-of-orderness bound (10 s) to every alert. Windows are still measured in event
 * time, so a late event is counted against the moment it happened, not when it arrived.
 */
public abstract class FraudRule extends KeyedProcessFunction<String, Payment, Alert> {
    protected final RuleConfig cfg;
    private final String name;
    private transient ValueState<Long> lastAlert;
    private transient Counter alerts;
    private transient Histogram detectionLatency;

    protected FraudRule(String name, RuleConfig cfg) {
        this.name = name;
        this.cfg = cfg;
    }

    @Override
    public void open(OpenContext ctx) throws Exception {
        lastAlert = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAlert", Long.class));
        var group = getRuntimeContext().getMetricGroup().addGroup("rule", name);
        alerts = group.counter("alerts");
        detectionLatency = group.histogram("detectionLatencyMs", new DescriptiveStatisticsHistogram(2048));
    }

    /** Emits unless this rule already alerted on this key within the cooldown (in event time). */
    protected void alert(Payment trigger, double score, String details, Collector<Alert> out) throws Exception {
        Long last = lastAlert.value();
        if (last != null && trigger.eventTime - last < cfg.cooldown.toMillis() && trigger.eventTime >= last) {
            return;
        }
        lastAlert.update(trigger.eventTime);
        Alert a = Alert.of(name, getCurrentKey(), trigger, score, details);
        alerts.inc();
        detectionLatency.update(Math.max(0, a.detectedAt - trigger.ingestTime));
        out.collect(a);
    }

    private String currentKey;

    @Override
    public void processElement(Payment p, Context ctx, Collector<Alert> out) throws Exception {
        currentKey = ctx.getCurrentKey();
        evaluate(p, ctx, out);
    }

    protected String getCurrentKey() {
        return currentKey;
    }

    protected abstract void evaluate(Payment p, Context ctx, Collector<Alert> out) throws Exception;

    /** Watermark, or Long.MIN_VALUE before the first one: state older than this minus a window is dead. */
    protected static long watermark(Context ctx) {
        return ctx.timerService().currentWatermark();
    }
}
