package io.paystream.flink.rules;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

/** Runs one keyed rule in Flink's operator test harness: real keyed state, timers and watermarks. */
final class RuleHarness implements AutoCloseable {
    private final KeyedOneInputStreamOperatorTestHarness<String, Payment, Alert> h;

    RuleHarness(FraudRule rule, KeySelector<Payment, String> key) throws Exception {
        h = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(rule), key, Types.STRING);
        h.open();
    }

    RuleHarness send(Payment p) throws Exception {
        h.processElement(p, p.eventTime);
        return this;
    }

    RuleHarness watermark(long t) throws Exception {
        h.processWatermark(t);
        return this;
    }

    List<Alert> alerts() {
        return h.extractOutputValues();
    }

    int stateKeys() {
        return h.numKeyedStateEntries();
    }

    @Override
    public void close() throws Exception {
        h.close();
    }
}
