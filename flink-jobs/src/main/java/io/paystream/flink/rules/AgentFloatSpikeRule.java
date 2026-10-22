package io.paystream.flink.rules;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;

/**
 * Keyed by agent: cash-out volume in the last 10 minutes far above the agent's own baseline.
 *
 * <p>A fixed threshold does not work for agents: a busy Dhaka agent does more in an hour than a rural
 * agent in a week. Each agent is compared with its own previous hour, treating the 10-minute count
 * as Poisson with the agent's hourly rate: alert when a count this high would happen by chance with
 * probability below {@code agentSpikeAlpha}, and at least a minimum count.
 *
 * <p>Why an exact tail and so small an alpha: the rule tests every agent on every cash-out, so
 * the false alarm rate is per test times tests. A normal approximation (mean + 4 sigma) put the bar
 * at 24 for an agent averaging 11, and on 20,000 agents it raised about 160 false alerts an hour in
 * the replay benchmark. Alpha 1e-6 on the exact tail asks for 31, still well under a real spike.
 */
public class AgentFloatSpikeRule extends FraudRule {
    private static final long MINUTE = 60_000L;
    private static final int CURRENT_MINUTES = 10;
    private static final int HISTORY_MINUTES = 60;

    private transient MapState<Long, Long> perMinute;
    private transient ValueState<Long> firstMinute;

    public AgentFloatSpikeRule(RuleConfig cfg) {
        super("AGENT_FLOAT_SPIKE", cfg);
    }

    @Override
    public void open(OpenContext ctx) throws Exception {
        super.open(ctx);
        perMinute = getRuntimeContext().getMapState(new MapStateDescriptor<>("cashOutsPerMinute", Long.class, Long.class));
        firstMinute = getRuntimeContext().getState(new ValueStateDescriptor<>("firstMinute", Long.class));
    }

    @Override
    protected void evaluate(Payment p, Context ctx, Collector<Alert> out) throws Exception {
        long m = p.eventTime / MINUTE;
        Long n = perMinute.get(m);
        perMinute.put(m, n == null ? 1 : n + 1);
        Long first = firstMinute.value();
        if (first == null || m < first) firstMinute.update(first = m);
        ctx.timerService().registerEventTimeTimer((m + CURRENT_MINUTES + HISTORY_MINUTES + 1) * MINUTE);

        long current = 0, history = 0;
        for (Map.Entry<Long, Long> e : perMinute.entries()) {
            long age = m - e.getKey();
            if (age >= 0 && age < CURRENT_MINUTES) current += e.getValue();
            else if (age >= CURRENT_MINUTES && age < CURRENT_MINUTES + HISTORY_MINUTES) history += e.getValue();
        }
        long historyMinutes = Math.min(HISTORY_MINUTES, m - CURRENT_MINUTES - first + 1);
        if (historyMinutes < cfg.agentWarmupMinutes) return;

        double baseline = (double) history * CURRENT_MINUTES / historyMinutes;
        if (current < cfg.agentSpikeMinCount) return;
        double pValue = poissonTail(current, baseline);
        if (pValue < cfg.agentSpikeAlpha) {
            double score = pValue > 0 ? Math.log10(pValue) / Math.log10(cfg.agentSpikeAlpha) : 10.0;
            alert(p, score, String.format("%d cash-outs in 10 min vs baseline %.1f (p=%.1e)", current, baseline, pValue),
                    out);
        }
    }

    /** P(X >= k) for X ~ Poisson(lambda). */
    static double poissonTail(long k, double lambda) {
        if (k <= 0) return 1.0;
        if (lambda <= 0) return 0.0;
        double logFact = 0;
        for (long i = 2; i <= k; i++) logFact += Math.log(i);
        double term = Math.exp(-lambda + k * Math.log(lambda) - logFact);
        double sum = 0;
        for (long i = k; term > 1e-300 && (sum == 0 || term > sum * 1e-12); i++) {
            sum += term;
            term *= lambda / (i + 1);
        }
        return Math.min(1.0, sum);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        long oldest = timestamp / MINUTE - CURRENT_MINUTES - HISTORY_MINUTES;
        List<Long> expired = new ArrayList<>();
        for (Long minute : perMinute.keys()) {
            if (minute < oldest) expired.add(minute);
        }
        for (Long minute : expired) perMinute.remove(minute);
    }
}
