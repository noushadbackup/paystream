package io.paystream.flink.rules;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.util.Collector;

/** Keyed by sender: too many outgoing transfers in a short event-time window (account draining). */
public class VelocityRule extends FraudRule {
    private transient ListState<Long> times;

    public VelocityRule(RuleConfig cfg) {
        super("VELOCITY", cfg);
    }

    @Override
    public void open(OpenContext ctx) throws Exception {
        super.open(ctx);
        times = getRuntimeContext().getListState(new ListStateDescriptor<>("outgoingTimes", Long.class));
    }

    @Override
    protected void evaluate(Payment p, Context ctx, Collector<Alert> out) throws Exception {
        long window = cfg.velocityWindow.toMillis();
        long horizon = watermark(ctx) == Long.MIN_VALUE ? Long.MIN_VALUE : watermark(ctx) - window;
        List<Long> kept = new ArrayList<>();
        int inWindow = 1;
        for (Long t : times.get()) {
            if (t < horizon) continue;
            kept.add(t);
            if (t > p.eventTime - window && t <= p.eventTime) inWindow++;
        }
        kept.add(p.eventTime);
        times.update(kept);
        ctx.timerService().registerEventTimeTimer(p.eventTime + window);

        if (inWindow >= cfg.velocityMaxTxns) {
            alert(p, (double) inWindow / cfg.velocityMaxTxns,
                    inWindow + " outgoing transfers in " + cfg.velocityWindow.toSeconds() + "s", out);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        List<Long> kept = new ArrayList<>();
        for (Long t : times.get()) {
            if (t > timestamp - cfg.velocityWindow.toMillis()) kept.add(t);
        }
        if (kept.isEmpty()) times.clear(); else times.update(kept);
    }
}
