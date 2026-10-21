package io.paystream.flink.rules;

import static org.assertj.core.api.Assertions.assertThat;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import org.junit.jupiter.api.Test;

class FraudRulesTest {
    private static final long T0 = 1_790_000_000_000L;
    private static final long S = 1_000L, MIN = 60_000L;
    private final RuleConfig cfg = new RuleConfig();
    private int seq;

    private Payment pay(long t, String type, String from, String to, long taka, String device) {
        return Payment.of("T" + (seq++), t, type, from, to, taka * 100, device);
    }

    @Test
    void velocityFiresOnSixthTransferWithinAMinute() throws Exception {
        try (var h = new RuleHarness(new VelocityRule(cfg), p -> p.senderId)) {
            for (int i = 0; i < 5; i++) h.send(pay(T0 + i * 5 * S, "P2P", "U1", "U" + (10 + i), 1000, "D1"));
            assertThat(h.alerts()).isEmpty();
            h.send(pay(T0 + 30 * S, "P2P", "U1", "U99", 1000, "D1"));
            assertThat(h.alerts()).singleElement().satisfies(a -> {
                assertThat(a.rule).isEqualTo("VELOCITY");
                assertThat(a.accountId).isEqualTo("U1");
                assertThat(a.details).startsWith("6 outgoing");
            });
        }
    }

    @Test
    void velocityIgnoresTransfersSpreadOverTime() throws Exception {
        try (var h = new RuleHarness(new VelocityRule(cfg), p -> p.senderId)) {
            for (int i = 0; i < 20; i++) h.send(pay(T0 + i * 15 * S, "P2P", "U1", "U2", 100, "D1"));
            assertThat(h.alerts()).isEmpty();
        }
    }

    @Test
    void velocityCountsLateEventAtItsEventTime() throws Exception {
        try (var h = new RuleHarness(new VelocityRule(cfg), p -> p.senderId)) {
            for (int i = 0; i < 5; i++) h.send(pay(T0 + i * S, "P2P", "U1", "U2", 100, "D1"));
            h.send(pay(T0 + 10 * MIN, "P2P", "U1", "U2", 100, "D1"));
            // arrives late but happened inside the burst: it completes the burst
            h.send(pay(T0 + 6 * S, "P2P", "U1", "U2", 100, "D1"));
            assertThat(h.alerts()).hasSize(1);
        }
    }

    @Test
    void cooldownSuppressesRepeatAlerts() throws Exception {
        try (var h = new RuleHarness(new VelocityRule(cfg), p -> p.senderId)) {
            for (int i = 0; i < 12; i++) h.send(pay(T0 + i * 2 * S, "P2P", "U1", "U2", 100, "D1"));
            assertThat(h.alerts()).hasSize(1);
            for (int i = 0; i < 6; i++) h.send(pay(T0 + 11 * MIN + i * S, "P2P", "U1", "U2", 100, "D1"));
            assertThat(h.alerts()).hasSize(2);
        }
    }

    @Test
    void velocityStateIsCleanedAfterTheWindow() throws Exception {
        try (var h = new RuleHarness(new VelocityRule(cfg), p -> p.senderId)) {
            h.send(pay(T0, "P2P", "U1", "U2", 100, "D1"));
            h.watermark(T0 + 2 * MIN);
            assertThat(h.stateKeys()).isZero();
        }
    }

    @Test
    void muleFanInNeedsDistinctSenders() throws Exception {
        try (var h = new RuleHarness(new MuleFanInRule(cfg), p -> p.receiverId)) {
            for (int i = 0; i < 20; i++) h.send(pay(T0 + i * S, "P2P", "U1", "U500", 3000, "D1"));
            assertThat(h.alerts()).isEmpty();
            for (int i = 2; i <= 8; i++) h.send(pay(T0 + 30 * S + i * 10 * S, "P2P", "U" + i, "U500", 3000, "D" + i));
            assertThat(h.alerts()).singleElement().satisfies(a -> {
                assertThat(a.rule).isEqualTo("MULE_FAN_IN");
                assertThat(a.accountId).isEqualTo("U500");
            });
        }
    }

    @Test
    void muleIgnoresSendersOutsideTheWindow() throws Exception {
        try (var h = new RuleHarness(new MuleFanInRule(cfg), p -> p.receiverId)) {
            for (int i = 0; i < 12; i++) h.send(pay(T0 + i * 2 * MIN, "P2P", "U" + i, "U500", 3000, "D" + i));
            assertThat(h.alerts()).isEmpty();
        }
    }

    @Test
    void newDeviceCashOutAfterHistory() throws Exception {
        try (var h = new RuleHarness(new NewDeviceCashOutRule(cfg), p -> p.senderId)) {
            h.send(pay(T0, "P2P", "U1", "U2", 500, "D1"));
            h.send(pay(T0 + 2 * 3600 * S, "CASH_OUT", "U1", "A1", 20_000, "D1")); // known device: fine
            h.send(pay(T0 + 3 * 3600 * S, "CASH_OUT", "U1", "A1", 2_000, "X9")); // new device, small: fine
            assertThat(h.alerts()).isEmpty();
            h.send(pay(T0 + 3 * 3600 * S + 30 * S, "CASH_OUT", "U1", "A1", 22_000, "X9"));
            assertThat(h.alerts()).singleElement().satisfies(a -> assertThat(a.rule).isEqualTo("NEW_DEVICE_CASH_OUT"));
        }
    }

    @Test
    void newDeviceIsSilentOnColdStart() throws Exception {
        try (var h = new RuleHarness(new NewDeviceCashOutRule(cfg), p -> p.senderId)) {
            h.send(pay(T0, "CASH_OUT", "U7", "A1", 24_000, "D7"));
            assertThat(h.alerts()).isEmpty();
        }
    }

    @Test
    void agentSpikeComparesWithOwnBaseline() throws Exception {
        try (var h = new RuleHarness(new AgentFloatSpikeRule(cfg), p -> p.receiverId)) {
            // 40 minutes of a steady 2 cash-outs per minute: baseline 20 per 10 min
            for (int m = 0; m < 40; m++) {
                h.send(pay(T0 + m * MIN, "CASH_OUT", "U" + m, "A1", 3000, "D1"));
                h.send(pay(T0 + m * MIN + 30 * S, "CASH_OUT", "U" + m, "A1", 3000, "D1"));
            }
            assertThat(h.alerts()).isEmpty();
            // then 40 extra in 5 minutes
            for (int i = 0; i < 40; i++) h.send(pay(T0 + 40 * MIN + i * 7 * S, "CASH_OUT", "V" + i, "A1", 9000, "D1"));
            assertThat(h.alerts()).singleElement().satisfies(a -> {
                assertThat(a.rule).isEqualTo("AGENT_FLOAT_SPIKE");
                assertThat(a.score).isGreaterThan(1.0);
            });
        }
    }

    @Test
    void agentSpikeWaitsForWarmup() throws Exception {
        try (var h = new RuleHarness(new AgentFloatSpikeRule(cfg), p -> p.receiverId)) {
            for (int i = 0; i < 60; i++) h.send(pay(T0 + i * 5 * S, "CASH_OUT", "U" + i, "A2", 9000, "D1"));
            assertThat(h.alerts()).isEmpty();
        }
    }

    @Test
    void alertIdIsDeterministic() {
        Payment p = Payment.of("T1", T0, "P2P", "U1", "U2", 100, "D1");
        assertThat(Alert.of("VELOCITY", "U1", p, 1.0, "x").alertId)
                .isEqualTo(Alert.of("VELOCITY", "U1", p, 2.0, "y").alertId)
                .isEqualTo("VELOCITY:U1:T1");
    }
}
