package io.paystream.flink.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import io.paystream.flink.bootstrap.DeviceHistoryBootstrap.DeviceSeen;
import io.paystream.flink.bootstrap.DeviceHistoryBootstrap.Outgoing;
import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import io.paystream.flink.rules.NewDeviceCashOutRule;
import io.paystream.flink.rules.RuleConfig;
import io.paystream.flink.rules.VelocityRule;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A savepoint written from history makes the payments job's account-takeover and velocity rules warm from the start. */
class DeviceHistoryBootstrapIT {
    private static final long T0 = 1_790_000_000_000L;
    private static final long DAY = 86_400_000L;
    private static final int MAX_PARALLELISM = 128;

    @TempDir
    Path dir;

    @Test
    void historyFromTheSavepointLetsTheRuleFireOnTheFirstPayment() throws Exception {
        StreamExecutionEnvironment boot = StreamExecutionEnvironment.getExecutionEnvironment();
        boot.setRuntimeMode(RuntimeExecutionMode.BATCH);
        boot.setParallelism(2);
        String savepoint = dir.resolve("device-history").toUri().toString();
        DeviceHistoryBootstrap.write(boot.fromData(
                new DeviceSeen("U1", "D1", T0 - 3 * DAY),
                new DeviceSeen("U1", "D1", T0 - 10 * DAY), // the earliest sighting wins
                new DeviceSeen("U2", "D2", T0 - 2 * DAY)), savepoint, MAX_PARALLELISM);
        boot.execute("bootstrap");

        // U1 cashes out 20,000 BDT from a phone it has never used: an account takeover
        Payment takeover = Payment.of("T1", T0, "CASH_OUT", "U1", "A1", 2_000_000, "X9");
        Payment usual = Payment.of("T2", T0, "CASH_OUT", "U2", "A1", 2_000_000, "D2");

        assertThat(run(List.of(takeover, usual), savepoint))
                .singleElement().satisfies(a -> {
                    assertThat(a.rule).isEqualTo("NEW_DEVICE_CASH_OUT");
                    assertThat(a.accountId).isEqualTo("U1");
                });
        assertThat(run(List.of(takeover, usual), null)).as("cold start: no history, no alert").isEmpty();
    }

    @Test
    void outgoingRatesFromTheSavepointKeepABusyAccountFromLookingLikeADrain() throws Exception {
        StreamExecutionEnvironment boot = StreamExecutionEnvironment.getExecutionEnvironment();
        boot.setRuntimeMode(RuntimeExecutionMode.BATCH);
        boot.setParallelism(2);
        String savepoint = dir.resolve("history").toUri().toString();
        long hour = 3_600_000L;
        List<Outgoing> busy = new ArrayList<>();
        for (int i = 0; i < 150; i++) busy.add(new Outgoing("U3", T0 - hour + i * 24_000L)); // 2.5 a minute
        RuleConfig rules = new RuleConfig();
        DeviceHistoryBootstrap.write(boot.fromData(new DeviceSeen("U3", "D3", T0 - DAY)), boot.fromData(busy),
                T0, hour, rules, savepoint, MAX_PARALLELISM);
        boot.execute("bootstrap");

        // six transfers in 50 s: usual for U3, a drain for U4, which has no history
        List<Payment> live = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            live.add(Payment.of("B" + i, T0 + i * 10_000L, "P2P", "U3", "U" + (100 + i), 50_000, "D3"));
            live.add(Payment.of("C" + i, T0 + i * 10_000L, "P2P", "U4", "U" + (100 + i), 50_000, "D4"));
        }
        assertThat(runVelocity(live, savepoint)).singleElement().satisfies(a -> assertThat(a.accountId).isEqualTo("U4"));
        assertThat(runVelocity(live, null)).as("cold start: both look like drains").hasSize(2);
    }

    private static List<Alert> runVelocity(List<Payment> payments, String savepoint) throws Exception {
        Configuration conf = new Configuration();
        conf.set(PipelineOptions.MAX_PARALLELISM, MAX_PARALLELISM);
        if (savepoint != null) {
            conf.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
            conf.set(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, true); // the new-device rule's state
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        return env.fromData(payments)
                .keyBy(p -> p.senderId)
                .process(new VelocityRule(new RuleConfig())).uid(VelocityRule.UID)
                .executeAndCollect(10);
    }

    private static List<Alert> run(List<Payment> payments, String savepoint) throws Exception {
        Configuration conf = new Configuration();
        conf.set(PipelineOptions.MAX_PARALLELISM, MAX_PARALLELISM);
        if (savepoint != null) conf.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        return env.fromData(payments)
                .keyBy(p -> p.senderId)
                .process(new NewDeviceCashOutRule(new RuleConfig())).uid(NewDeviceCashOutRule.UID)
                .executeAndCollect(10);
    }
}
