package io.paystream.flink.rules;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;

/**
 * Keyed by sender: far more outgoing transfers in a short event-time window than this account
 * usually makes (account draining).
 *
 * <p>A fixed bar (six in a minute) is right for a typical account and wrong for a busy one: an
 * account that normally sends two or three a minute crosses it by chance every few minutes. On the
 * desktop benchmark (1M users at 2,000/s) such accounts raised 2,717 of 2,760 alerts. So each
 * account is compared with its own history, as {@link AgentFloatSpikeRule} does for agents.
 *
 * <p>The account's rate is Gamma-distributed: a prior worth {@code velocityPriorMinutes} of an
 * ordinary account ({@code velocityPriorPerMin}), updated with the account's own transfers before
 * the current window, decayed with time constant {@code velocityHistory}. The count in the window
 * is then negative binomial, and the rule alerts when a count this high has probability below
 * {@code velocityAlpha}, and is at least {@code velocityMaxTxns}. With no history the prior alone
 * gives the old behaviour: six in a minute alerts. The posterior also stays wide while history is
 * short, so a busy account is not flagged before its rate is known.
 *
 * <p>Transfers in a window that met the alert condition are kept out of the history, so a drain
 * does not raise the account's baseline and hide the next one. A late event is compared with a
 * baseline that may already include its neighbours, so it errs towards not alerting.
 */
public class VelocityRule extends FraudRule {
    private static final long MINUTE = 60_000L;
    // history: decayed count, decayed exposure (ms), history end, excluded interval from, to
    private static final int COUNT = 0, EXPOSURE = 1, END = 2, EXCLUDE_FROM = 3, EXCLUDE_TO = 4;

    private transient ListState<Long> times;
    private transient ValueState<double[]> history;

    public VelocityRule(RuleConfig cfg) {
        super("VELOCITY", cfg);
    }

    @Override
    public void open(OpenContext ctx) throws Exception {
        super.open(ctx);
        times = getRuntimeContext().getListState(new ListStateDescriptor<>("outgoingTimes", Long.class));
        ValueStateDescriptor<double[]> desc = new ValueStateDescriptor<>("outgoingHistory", double[].class);
        // after six time constants the history weighs 0.25%: an idle account falls back to the prior
        desc.enableTimeToLive(StateTtlConfig.newBuilder(cfg.velocityHistory.multipliedBy(6)).build());
        history = getRuntimeContext().getState(desc);
    }

    @Override
    protected void evaluate(Payment p, Context ctx, Collector<Alert> out) throws Exception {
        long window = cfg.velocityWindow.toMillis();
        long horizon = watermark(ctx) == Long.MIN_VALUE ? Long.MIN_VALUE : watermark(ctx) - window;
        double[] h = history.value();
        if (h == null) h = new double[] {0, 0, p.eventTime - window, 0, Double.NEGATIVE_INFINITY};

        List<Long> kept = new ArrayList<>();
        int inWindow = 1;
        for (Long t : times.get()) {
            if (t < horizon) continue;
            kept.add(t);
            if (t > p.eventTime - window && t <= p.eventTime) inWindow++;
        }
        boolean late = p.eventTime <= h[END];
        advance(h, p.eventTime - window, kept);

        double tail = 1.0;
        if (inWindow >= cfg.velocityMaxTxns) {
            tail = tail(inWindow, h);
            if (tail < cfg.velocityAlpha) {
                h[EXCLUDE_FROM] = p.eventTime - window > h[EXCLUDE_TO] ? p.eventTime - window : h[EXCLUDE_FROM];
                h[EXCLUDE_TO] = Math.max(h[EXCLUDE_TO], p.eventTime + window);
            }
        }
        if (late) absorb(h, p.eventTime);
        kept.add(p.eventTime);
        times.update(kept);
        history.update(h);
        ctx.timerService().registerEventTimeTimer(p.eventTime + window);

        if (tail < cfg.velocityAlpha) {
            double score = tail > 0 ? Math.log10(tail) / Math.log10(cfg.velocityAlpha) : 10.0;
            alert(p, score, String.format("%d outgoing transfers in %ds vs usual %.1f (p=%.1e)", inWindow,
                    cfg.velocityWindow.toSeconds(), expected(h), tail), out);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        long window = cfg.velocityWindow.toMillis();
        List<Long> all = new ArrayList<>();
        for (Long t : times.get()) all.add(t);
        double[] h = history.value();
        if (h != null) {
            advance(h, timestamp - window, all);
            history.update(h);
        }
        List<Long> kept = new ArrayList<>();
        for (Long t : all) {
            if (t > timestamp - window) kept.add(t);
        }
        if (kept.isEmpty()) times.clear(); else times.update(kept);
    }

    /** Moves the end of the history to {@code end}, counting the transfers that left the window. */
    private void advance(double[] h, long end, List<Long> pending) {
        if (end <= h[END]) return;
        double tau = cfg.velocityHistory.toMillis();
        double decay = Math.exp(-(end - h[END]) / tau);
        h[COUNT] *= decay;
        h[EXPOSURE] = h[EXPOSURE] * decay + tau * (1 - decay);
        double previousEnd = h[END];
        h[END] = end;
        for (Long t : pending) {
            if (t > previousEnd && t <= end) absorb(h, t);
        }
    }

    /** Adds one transfer at {@code t <= h[END]} to the history, unless it was part of a burst. */
    private void absorb(double[] h, long t) {
        if (t >= h[EXCLUDE_FROM] && t <= h[EXCLUDE_TO]) return;
        h[COUNT] += Math.exp(-(h[END] - t) / cfg.velocityHistory.toMillis());
    }

    /** Posterior mean count in one window. */
    private double expected(double[] h) {
        return shape(h) / rate(h) * cfg.velocityWindow.toMillis();
    }

    private double shape(double[] h) {
        return cfg.velocityPriorPerMin * cfg.velocityPriorMinutes + h[COUNT];
    }

    private double rate(double[] h) {
        return cfg.velocityPriorMinutes * (double) MINUTE + h[EXPOSURE];
    }

    private double tail(int k, double[] h) {
        double rate = rate(h);
        return negativeBinomialTail(k, shape(h), rate / (rate + cfg.velocityWindow.toMillis()));
    }

    /**
     * P(X >= k) for X ~ NegBin(r, p), pmf(i) = Γ(i+r) / (i! Γ(r)) p^r (1-p)^i: the count in a window
     * when the rate is Gamma(r, β) and p = β / (β + window).
     */
    static double negativeBinomialTail(int k, double r, double p) {
        if (k <= 0) return 1.0;
        if (k <= r * (1 - p) / p) return 1.0; // at or below the mean: nowhere near any alpha
        double logTerm = r * Math.log(p);
        for (int i = 0; i < k; i++) logTerm += Math.log((i + r) / (i + 1)) + Math.log1p(-p);
        double term = Math.exp(logTerm);
        double sum = 0;
        for (int i = k; term > 1e-300 && (sum == 0 || term > sum * 1e-12) && i < k + 100_000; i++) {
            sum += term;
            term *= (i + r) / (i + 1) * (1 - p);
        }
        return Math.min(1.0, sum);
    }
}
