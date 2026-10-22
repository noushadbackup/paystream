package io.paystream.flink.rules.dynamic;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Evaluates analyst rules against keyed history, with rules hot-swapped through broadcast state.
 *
 * <p>Per key (for example {@code sender_id|U00042}) it keeps the matching payments of the last
 * {@code max(window)} of the rules that group by that field, bucketed by event time. Each arriving
 * payment is checked against every live rule it matches, on arrival, like the built-in rules.
 *
 * <p>Detection does not depend on arrival order. A payment can arrive after later ones for the same
 * key: they come through different Kafka partitions (three agents' cash-ins into one account), and
 * partitions lag by different amounts after a restart or a rebalance. So an arrival is checked
 * against every window it falls in, not only its own: the windows ending at each matching payment
 * in {@code [t, t + window)}. The replay benchmark found the gap: a structuring pattern whose middle
 * cash-in arrived after its last was never flagged.
 * When a rule's window grows, retention grows with it; when the last rule on a field is disabled,
 * that field's state is dropped at the next timer.
 */
public class DynamicRuleEvaluator extends KeyedBroadcastProcessFunction<String, KeyedPayment, String, Alert> {
    private static final long TIMER_GRANULARITY_MS = 60_000;
    private static final MapStateDescriptor<Long, List<Payment>> EVENTS = new MapStateDescriptor<>("events",
            Types.LONG, Types.LIST(TypeInformation.of(Payment.class)));

    private transient MapState<Long, List<Payment>> events;
    private transient MapState<String, Long> lastAlert;
    private transient RuleBook book;
    private transient Map<String, Counter> counters;
    private transient Map<String, Histogram> latency;
    private transient int liveRules;

    @Override
    public void open(OpenContext ctx) throws Exception {
        events = getRuntimeContext().getMapState(EVENTS);
        lastAlert = getRuntimeContext().getMapState(new MapStateDescriptor<>("lastAlert", Types.STRING, Types.LONG));
        book = new RuleBook();
        counters = new HashMap<>();
        latency = new HashMap<>();
        getRuntimeContext().getMetricGroup().gauge("dynamicRulesLive", () -> liveRules);
    }

    @Override
    public void processElement(KeyedPayment k, ReadOnlyContext ctx, Collector<Alert> out) throws Exception {
        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(RuleBook.STATE);
        Payment p = k.payment;
        List<RuleDef> rules = new ArrayList<>();
        long maxWindow = 0;
        for (RuleDef r : book.live(state)) {
            if (!r.groupBy.equals(k.groupBy)) continue;
            maxWindow = Math.max(maxWindow, r.windowMs);
            if (r.matches(p)) rules.add(r);
        }
        if (rules.isEmpty()) return;

        List<Payment> bucket = events.get(p.eventTime);
        if (bucket == null) bucket = new ArrayList<>(1);
        bucket.add(p);
        events.put(p.eventTime, bucket);
        ctx.timerService().registerEventTimeTimer(roundUp(p.eventTime + maxWindow));

        for (RuleDef r : rules) {
            for (Map.Entry<Long, Payment> end : windowEnds(r, p).entrySet()) {
                double observed = aggregate(r, end.getKey());
                if (r.crosses(observed)) alert(r, k, end.getValue(), observed, out);
            }
        }
    }

    /**
     * The windows {@code p} falls in: one ending at {@code p} and one ending at each later matching
     * payment within the window, in event-time order, each with the payment that ends it.
     */
    private TreeMap<Long, Payment> windowEnds(RuleDef r, Payment p) throws Exception {
        TreeMap<Long, Payment> ends = new TreeMap<>();
        ends.put(p.eventTime, p);
        for (Map.Entry<Long, List<Payment>> e : events.entries()) {
            long s = e.getKey();
            if (s <= p.eventTime || s >= p.eventTime + r.windowMs) continue;
            for (Payment q : e.getValue()) if (r.matches(q)) ends.put(s, q); // the last to arrive at s
        }
        return ends;
    }

    /** Aggregate over this key's payments that match the rule and happened in (t - window, t]. */
    double aggregate(RuleDef r, long t) throws Exception {
        long n = 0;
        double sum = 0, min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;
        Set<String> distinct = r.fn == RuleDef.Fn.COUNT_DISTINCT ? new HashSet<>() : null;
        for (Map.Entry<Long, List<Payment>> e : events.entries()) {
            if (e.getKey() <= t - r.windowMs || e.getKey() > t) continue;
            for (Payment q : e.getValue()) {
                if (!r.matches(q)) continue;
                n++;
                if (r.aggField == null) continue;
                Object v = RuleDef.field(q, r.aggField);
                if (distinct != null) {
                    if (v != null) distinct.add(v.toString());
                } else if (v != null) {
                    double x = ((Long) v).doubleValue();
                    sum += x;
                    min = Math.min(min, x);
                    max = Math.max(max, x);
                }
            }
        }
        return switch (r.fn) {
            case COUNT -> n;
            case SUM -> sum;
            case MIN -> n == 0 ? 0 : min;
            case MAX -> n == 0 ? 0 : max;
            case AVG -> n == 0 ? 0 : sum / n;
            case COUNT_DISTINCT -> distinct.size();
        };
    }

    private void alert(RuleDef r, KeyedPayment k, Payment trigger, double observed, Collector<Alert> out) throws Exception {
        // cooldown in event time, either side: a pattern completed late is the same incident
        Long last = lastAlert.get(r.ruleId);
        if (last != null && Math.abs(trigger.eventTime - last) < r.cooldownMs) return;
        lastAlert.put(r.ruleId, last == null ? trigger.eventTime : Math.max(last, trigger.eventTime));

        Alert a = Alert.of("DYNAMIC", k.value, trigger, observed / r.threshold, String.format(
                "%s: %s%s = %s in %ds by %s", r.name, r.fn, r.aggField == null ? "" : "(" + r.aggField + ")",
                observed == Math.rint(observed) ? Long.toString((long) observed) : String.format("%.2f", observed),
                r.windowMs / 1000, r.groupBy));
        a.alertId = r.ruleId + ":" + k.value + ":" + trigger.txnId;
        a.ruleId = r.ruleId;
        a.ruleVersion = r.version;
        a.shadow = r.state == RuleDef.State.SHADOW;
        a.action = r.action;

        String metric = a.shadow ? "shadowAlerts" : "alerts";
        counters.computeIfAbsent(r.ruleId + "/" + metric,
                x -> getRuntimeContext().getMetricGroup().addGroup("rule", r.ruleId).counter(metric)).inc();
        latency.computeIfAbsent(r.ruleId, x -> getRuntimeContext().getMetricGroup().addGroup("rule", r.ruleId)
                .histogram("detectionLatencyMs", new DescriptiveStatisticsHistogram(2048)))
                .update(Math.max(0, a.detectedAt - trigger.ingestTime));
        out.collect(a);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        String groupBy = ctx.getCurrentKey().substring(0, ctx.getCurrentKey().indexOf('|'));
        long maxWindow = book.maxWindowMs(ctx.getBroadcastState(RuleBook.STATE), groupBy);
        long oldest = Long.MAX_VALUE;
        for (Iterator<Long> it = events.keys().iterator(); it.hasNext(); ) {
            long t = it.next();
            if (maxWindow == 0 || t <= ts - maxWindow) it.remove(); else oldest = Math.min(oldest, t);
        }
        if (oldest != Long.MAX_VALUE) {
            ctx.timerService().registerEventTimeTimer(Math.max(ts + 1, roundUp(oldest + maxWindow)));
            return;
        }
        events.clear(); // removing through the iterator can leave an empty map behind for the key
        // No history left: drop cooldowns that have run out, keep a timer for the ones that have not.
        Map<String, Long> cooldowns = new HashMap<>();
        for (RuleDef r : book.live(ctx.getBroadcastState(RuleBook.STATE))) cooldowns.put(r.ruleId, r.cooldownMs);
        for (Iterator<Map.Entry<String, Long>> it = lastAlert.iterator(); it.hasNext(); ) {
            Map.Entry<String, Long> e = it.next();
            long until = e.getValue() + cooldowns.getOrDefault(e.getKey(), 0L);
            if (until <= ts) it.remove(); else ctx.timerService().registerEventTimeTimer(roundUp(until));
        }
        if (lastAlert.isEmpty()) lastAlert.clear();
    }

    @Override
    public void processBroadcastElement(String json, Context ctx, Collector<Alert> out) throws Exception {
        RuleDef r = RuleDef.parse(json);
        if (!RuleBook.supersedes(r, ctx.getBroadcastState(RuleBook.STATE).get(r.ruleId))) return;
        ctx.getBroadcastState(RuleBook.STATE).put(r.ruleId, json);
        liveRules = book.live(ctx.getBroadcastState(RuleBook.STATE)).size();
        if (book.maxWindowMs(ctx.getBroadcastState(RuleBook.STATE), r.groupBy) == 0) {
            // the last rule on this grouping is gone: free its history now rather than at each key's next timer
            String prefix = r.groupBy + "|";
            ctx.applyToKeyedState(EVENTS, (key, state) -> {
                if (key.startsWith(prefix)) state.clear();
            });
        }
    }

    /** Timers are coalesced to the minute: one timer per key per minute, not per payment. */
    private static long roundUp(long t) {
        return Math.floorDiv(t + TIMER_GRANULARITY_MS - 1, TIMER_GRANULARITY_MS) * TIMER_GRANULARITY_MS;
    }
}
