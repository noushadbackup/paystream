package io.paystream.flink.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import io.paystream.flink.bootstrap.DeviceHistoryBootstrap.DeviceSeen;
import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import io.paystream.flink.rules.NewDeviceCashOutRule;
import io.paystream.flink.rules.RuleConfig;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A savepoint written from history makes the payments job's account-takeover rule warm from the start. */
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
