package io.paystream.flink.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import io.paystream.flink.model.Payment;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class DedupTest {
    @Test
    void passesFirstCopyOnlyAndSurvivesRestore() throws Exception {
        Payment a = Payment.of("T1", 1000, "P2P", "U1", "U2", 100, "D1");
        Payment b = Payment.of("T2", 2000, "P2P", "U1", "U2", 100, "D1");

        var h = harness();
        h.open();
        h.processElement(a, a.eventTime);
        h.processElement(a, a.eventTime);
        var snapshot = h.snapshot(1, 1);
        h.close();

        // after a failure, the restored operator still remembers T1
        var restored = harness();
        restored.initializeState(snapshot);
        restored.open();
        restored.processElement(a, a.eventTime);
        restored.processElement(b, b.eventTime);
        assertThat(restored.extractOutputValues()).extracting(p -> p.txnId).containsExactly("T2");
        restored.close();
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, Payment, Payment> harness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new Dedup(Duration.ofHours(2))), p -> p.txnId, Types.STRING);
    }
}
