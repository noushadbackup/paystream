package io.paystream.flink.rules.dynamic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.paystream.flink.model.Payment;
import org.junit.jupiter.api.Test;

/** Hot reload, version ordering, state lifecycle and recovery of analyst rules. */
class DynamicRulesTest {
    private static final long T0 = 1_790_000_000_000L;
    private static final long S = 1_000L, MIN = 60_000L;
    private int seq;

    private static String rule(int version, String state, int threshold, int windowS) {
        return """
            {"rule_id": "p2p-burst", "version": %d, "state": "%s", "group_by": "sender_id",
             "when": [{"field": "txn_type", "op": "==", "value": "P2P"}],
             "aggregate": {"fn": "COUNT"}, "window_s": %d,
             "threshold": {"op": ">=", "value": %d}, "cooldown_s": 0, "action": "DECLINE"}
            """.formatted(version, state, windowS, threshold);
    }

    private Payment p2p(long t, String from) {
        return Payment.of("T" + (seq++), t, "P2P", from, "U2", 10_000, "D1");
    }

    @Test
    void publishingANewVersionChangesBehaviourWithoutARestart() throws Exception {
        try (var h = new DynamicHarness()) {
            h.publish(rule(1, "ACTIVE", 5, 60));
            for (int i = 0; i < 3; i++) h.send(p2p(T0 + i * S, "U1"));
            assertThat(h.alerts()).isEmpty();

            h.publish(rule(2, "ACTIVE", 4, 60)); // analyst tightens the threshold
            h.send(p2p(T0 + 3 * S, "U1"));
            assertThat(h.alerts()).singleElement().satisfies(a -> {
                assertThat(a.ruleId).isEqualTo("p2p-burst");
                assertThat(a.ruleVersion).isEqualTo(2);
                assertThat(a.rule).isEqualTo("DYNAMIC");
                assertThat(a.action).isEqualTo("DECLINE");
                assertThat(a.alertId).isEqualTo("p2p-burst:U1:T3");
                assertThat(a.details).contains("COUNT = 4 in 60s by sender_id");
            });
        }
    }

    @Test
    void anOlderVersionArrivingLateNeverRollsARuleBack() throws Exception {
        try (var h = new DynamicHarness()) {
            h.publish(rule(3, "ACTIVE", 2, 60));
            h.publish(rule(2, "ACTIVE", 50, 60));
            h.send(p2p(T0, "U1")).send(p2p(T0 + S, "U1"));
            assertThat(h.alerts()).singleElement().satisfies(a -> assertThat(a.ruleVersion).isEqualTo(3));
        }
    }

    @Test
    void shadowRulesAlertButAreFlagged() throws Exception {
        try (var h = new DynamicHarness()) {
            h.publish(rule(1, "SHADOW", 2, 60));
            h.send(p2p(T0, "U1")).send(p2p(T0 + S, "U1"));
            assertThat(h.alerts()).singleElement().satisfies(a -> assertThat(a.shadow).isTrue());
        }
    }

    @Test
    void historyIsKeptForTheWindowOnlyAndDroppedWhenTheRuleIsDisabled() throws Exception {
        try (var h = new DynamicHarness()) {
            h.publish(rule(1, "ACTIVE", 100, 600));
            h.send(p2p(T0, "U1")).send(p2p(T0 + S, "U2"));
            assertThat(h.stateEntries()).isEqualTo(2);

            h.watermark(T0 + 5 * MIN);
            assertThat(h.stateEntries()).as("still inside the 10-minute window").isEqualTo(2);
            h.watermark(T0 + 11 * MIN);
            assertThat(h.stateEntries()).isZero();
            assertThat(h.timers()).isZero();

            h.send(p2p(T0 + 12 * MIN, "U1"));
            h.publish(rule(2, "DISABLED", 100, 600));
            h.send(p2p(T0 + 12 * MIN + S, "U1"));
            assertThat(h.stateEntries()).as("disabling the last rule on a grouping frees its history").isZero();
            assertThat(h.alerts()).isEmpty();
        }
    }

    @Test
    void wideningTheWindowExtendsRetentionOfExistingHistory() throws Exception {
        try (var h = new DynamicHarness()) {
            h.publish(rule(1, "ACTIVE", 3, 60));
            h.send(p2p(T0, "U1")).send(p2p(T0 + S, "U1"));
            h.publish(rule(2, "ACTIVE", 3, 3600));
            h.watermark(T0 + 10 * MIN);
            h.send(p2p(T0 + 20 * MIN, "U1"));
            assertThat(h.alerts()).as("the first two payments were kept under the new 1-hour window").hasSize(1);
        }
    }

    @Test
    void rulesAndHistorySurviveARestoreFromCheckpoint() throws Exception {
        var snapshot = (org.apache.flink.runtime.checkpoint.OperatorSubtaskState) null;
        DynamicHarness before = new DynamicHarness();
        before.publish(rule(1, "ACTIVE", 3, 60));
        before.send(p2p(T0, "U1")).send(p2p(T0 + S, "U1"));
        snapshot = before.snapshot();
        before.close();

        try (var after = new DynamicHarness(snapshot).knowing(before)) {
            after.send(p2p(T0 + 2 * S, "U1"));
            assertThat(after.alerts()).singleElement()
                    .satisfies(a -> assertThat(a.details).contains("COUNT = 3"));
        }
    }

    @Test
    void invalidRulesAreRejectedWithAReadableReason() {
        assertThatThrownBy(() -> RuleDef.parse("{\"rule_id\": \"x\"}"))
                .hasMessageContaining("rule_id must be 3-64 chars");
        assertThatThrownBy(() -> RuleDef.parse(rule(1, "ACTIVE", 3, 60).replace("\"COUNT\"", "\"MEDIAN\"")))
                .hasMessageContaining("aggregate.fn must be one of");
        assertThatThrownBy(() -> RuleDef.parse(rule(1, "ACTIVE", 3, 60).replace("\"P2P\"", "5")))
                .hasMessageContaining("txn_type compares to text");
        assertThatThrownBy(() -> RuleDef.parse(rule(1, "ACTIVE", 3, 60)
                .replace("\"field\": \"txn_type\", \"op\": \"==\"", "\"field\": \"txn_type\", \"op\": \">\"")))
                .hasMessageContaining("needs a numeric field");
        assertThatThrownBy(() -> RuleDef.parse(rule(1, "ACTIVE", 3, 999_999_999)))
                .hasMessageContaining("window_s must be");
        assertThatThrownBy(() -> RuleDef.parse("not json")).hasMessageContaining("not valid JSON");
    }

    @Test
    void hourDhakaIsUtcPlusSix() {
        Payment p = Payment.of("T", java.time.Instant.parse("2026-10-01T18:30:00Z").toEpochMilli(),
                "P2P", "U1", "U2", 1, "D");
        assertThat(RuleDef.field(p, "hour_dhaka")).isEqualTo(0L);
    }
}
