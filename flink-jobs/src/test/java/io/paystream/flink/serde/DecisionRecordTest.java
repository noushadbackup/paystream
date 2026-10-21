package io.paystream.flink.serde;

import static org.assertj.core.api.Assertions.assertThat;

import io.paystream.flink.model.DecisionRecord;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** schemas/decision_log.example.json is the contract; the decision service's tests check it too. */
class DecisionRecordTest {
    @Test
    void parsesTheDecisionServiceLogFormat() throws Exception {
        DecisionRecord d = DecisionRecord.parse(Files.readAllBytes(Path.of("..", "schemas", "decision_log.example.json")));
        assertThat(d.outcome).isEqualTo("REVIEW");
        assertThat(d.reasons).isEqualTo("RECEIVER_FLAGGED,VELOCITY");
        assertThat(d.senderId).isEqualTo("U0001234");
        assertThat(d.amountPaisa).isEqualTo(150_000);
        assertThat(d.latencyUs).isEqualTo(640);
        assertThat(d.features).contains("\"s_txn_2m\":5");
        assertThat(d.flags).isEqualTo("MULE_FAN_IN");
    }
}
