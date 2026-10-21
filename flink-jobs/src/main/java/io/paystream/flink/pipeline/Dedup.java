package io.paystream.flink.pipeline;

import io.paystream.flink.model.Payment;
import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed by txn_id: passes the first copy of each transaction and drops app retries.
 *
 * <p>State lives for {@code ttl} after the first copy. That must cover the longest gap between a
 * transaction and its retry plus the lateness we accept (30 min); anything older that slips through
 * is caught again by the batch layer's dedup in dbt.
 */
public class Dedup extends KeyedProcessFunction<String, Payment, Payment> {
    private final Duration ttl;
    private transient ValueState<Boolean> seen;
    private transient Counter duplicates;

    public Dedup(Duration ttl) {
        this.ttl = ttl;
    }

    @Override
    public void open(OpenContext ctx) {
        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Boolean.class);
        desc.enableTimeToLive(StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(10_000)
                .build());
        seen = getRuntimeContext().getState(desc);
        duplicates = getRuntimeContext().getMetricGroup().counter("duplicatesDropped");
    }

    @Override
    public void processElement(Payment p, Context ctx, Collector<Payment> out) throws Exception {
        if (seen.value() != null) {
            duplicates.inc();
            return;
        }
        seen.update(true);
        out.collect(p);
    }
}
