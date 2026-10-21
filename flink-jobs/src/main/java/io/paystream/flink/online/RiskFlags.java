package io.paystream.flink.online;

import io.paystream.flink.model.Alert;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Turns alerts into flags the decision service enforces on the account's next payment: a rule with
 * action DECLINE blocks, REVIEW holds for an analyst. Each rule writes its own field, so a REVIEW
 * from one rule never overwrites a DECLINE from another. Shadow alerts never become flags.
 */
public class RiskFlags implements FlatMapFunction<Alert, RedisWrite> {
    private final long ttlSeconds;

    public RiskFlags(long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    @Override
    public void flatMap(Alert a, Collector<RedisWrite> out) {
        if (a.shadow) return;
        String rule = a.ruleId != null ? a.ruleId : a.rule;
        String json = "{\"rule\":\"" + rule + "\",\"action\":\"" + a.action + "\",\"alert_id\":\"" + a.alertId
                + "\",\"at\":" + a.detectedAt + ",\"until\":" + (a.detectedAt + ttlSeconds * 1000) + "}";
        out.collect(RedisWrite.hset("flags:" + a.accountId, rule, json, ttlSeconds));
    }
}
