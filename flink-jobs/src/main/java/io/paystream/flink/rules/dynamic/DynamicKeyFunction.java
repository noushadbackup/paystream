package io.paystream.flink.rules.dynamic;

import io.paystream.flink.model.Payment;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Fans each payment out to the keys the current rules group by.
 *
 * <p>Keys are not known when the job is built: a new rule grouping by {@code device_id} starts a
 * new partitioning of the stream the moment it is published. A payment is emitted once per
 * grouping field that has at least one live rule it matches, so a payment no rule cares about
 * costs nothing downstream.
 */
public class DynamicKeyFunction extends BroadcastProcessFunction<Payment, String, KeyedPayment> {
    private transient RuleBook book;

    @Override
    public void processElement(Payment p, ReadOnlyContext ctx, Collector<KeyedPayment> out) throws Exception {
        for (KeyedPayment k : keysFor(p, book().live(ctx.getBroadcastState(RuleBook.STATE)))) out.collect(k);
    }

    @Override
    public void processBroadcastElement(String json, Context ctx, Collector<KeyedPayment> out) throws Exception {
        RuleDef r = RuleDef.parse(json);
        if (RuleBook.supersedes(r, ctx.getBroadcastState(RuleBook.STATE).get(r.ruleId))) {
            ctx.getBroadcastState(RuleBook.STATE).put(r.ruleId, json);
        }
    }

    public static List<KeyedPayment> keysFor(Payment p, List<RuleDef> rules) {
        Set<String> fields = new LinkedHashSet<>();
        for (RuleDef r : rules) if (!fields.contains(r.groupBy) && r.matches(p)) fields.add(r.groupBy);
        List<KeyedPayment> out = new ArrayList<>();
        for (String f : fields) {
            Object v = RuleDef.field(p, f);
            if (v != null) out.add(new KeyedPayment(f, v.toString(), p));
        }
        return out;
    }

    private RuleBook book() {
        if (book == null) book = new RuleBook();
        return book;
    }
}
