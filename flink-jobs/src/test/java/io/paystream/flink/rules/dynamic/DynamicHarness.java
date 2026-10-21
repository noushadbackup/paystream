package io.paystream.flink.rules.dynamic;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;

/**
 * The evaluator in Flink's keyed-broadcast test harness (real keyed state, broadcast state, timers),
 * with the key function applied in front of it the way the job graph does.
 */
final class DynamicHarness implements AutoCloseable {
    final KeyedBroadcastOperatorTestHarness<String, KeyedPayment, String, Alert> h;
    private final Map<String, String> rules = new HashMap<>();

    DynamicHarness() throws Exception {
        this(null);
    }

    /** @param restoreFrom a snapshot to start from, as after a failure; null for a fresh start */
    DynamicHarness(OperatorSubtaskState restoreFrom) throws Exception {
        h = new KeyedBroadcastOperatorTestHarness<>(
                new CoBroadcastWithKeyedOperator<>(new DynamicRuleEvaluator(), List.of(RuleBook.STATE)),
                k -> k.key, Types.STRING, 1, 1, 0);
        h.setup();
        if (restoreFrom != null) h.initializeState(restoreFrom); else h.initializeEmptyState();
        h.open();
        // as RuleUpdates.watermarks() does in the job: the rules side never holds event time back
        h.processBroadcastWatermark(new Watermark(Long.MAX_VALUE));
    }

    /** Rules known to this harness's key function; after a restore they come back from the snapshot. */
    DynamicHarness knowing(DynamicHarness other) {
        rules.putAll(other.rules);
        return this;
    }

    OperatorSubtaskState snapshot() throws Exception {
        return h.snapshot(1, 1);
    }

    DynamicHarness publish(String json) throws Exception {
        RuleDef r = RuleDef.parse(json);
        if (RuleBook.supersedes(r, rules.get(r.ruleId))) rules.put(r.ruleId, json);
        h.processBroadcastElement(json, 0);
        return this;
    }

    DynamicHarness send(Payment p) throws Exception {
        List<RuleDef> live = new ArrayList<>();
        for (String json : rules.values()) {
            RuleDef r = RuleDef.parse(json);
            if (r.state != RuleDef.State.DISABLED) live.add(r);
        }
        live.sort(Comparator.comparing(r -> r.ruleId));
        for (KeyedPayment k : DynamicKeyFunction.keysFor(p, live)) h.processElement(k, p.eventTime);
        return this;
    }

    DynamicHarness watermark(long t) throws Exception {
        h.processWatermark(t);
        return this;
    }

    List<Alert> alerts() {
        return h.extractOutputValues();
    }

    int stateEntries() {
        return ((HeapKeyedStateBackend<?>) h.getOperator().getKeyedStateBackend()).numKeyValueStateEntries();
    }

    int timers() {
        return h.numEventTimeTimers();
    }

    @Override
    public void close() throws Exception {
        h.close();
    }
}
