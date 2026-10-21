package io.paystream.flink.rules;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.util.Collector;

/** Keyed by receiver: many distinct people sending money to one account within minutes (mule account). */
public class MuleFanInRule extends FraudRule {
    private transient MapState<String, Long> senders;

    public MuleFanInRule(RuleConfig cfg) {
        super("MULE_FAN_IN", cfg);
    }

    @Override
    public void open(OpenContext ctx) throws Exception {
        super.open(ctx);
        senders = getRuntimeContext().getMapState(new MapStateDescriptor<>("senders", String.class, Long.class));
    }

    @Override
    protected void evaluate(Payment p, Context ctx, Collector<Alert> out) throws Exception {
        Long prev = senders.get(p.senderId);
        if (prev == null || p.eventTime > prev) senders.put(p.senderId, p.eventTime);

        long window = cfg.muleWindow.toMillis();
        int distinct = 0;
        for (Map.Entry<String, Long> e : senders.entries()) {
            long t = e.getValue();
            if (t > p.eventTime - window && t <= p.eventTime) distinct++;
        }
        ctx.timerService().registerEventTimeTimer(p.eventTime + window);

        if (distinct >= cfg.muleMinSenders) {
            alert(p, (double) distinct / cfg.muleMinSenders,
                    distinct + " distinct senders in " + cfg.muleWindow.toMinutes() + " min", out);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, Long> e : senders.entries()) {
            if (e.getValue() <= timestamp - cfg.muleWindow.toMillis()) expired.add(e.getKey());
        }
        for (String s : expired) senders.remove(s);
    }
}
