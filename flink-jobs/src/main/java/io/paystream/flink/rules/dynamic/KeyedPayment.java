package io.paystream.flink.rules.dynamic;

import io.paystream.flink.model.Payment;

/** A payment routed to the evaluator for one grouping field, e.g. {@code device_id|D42}. */
public class KeyedPayment {
    public String key;
    public String groupBy;
    public String value;
    public Payment payment;

    public KeyedPayment() {}

    public KeyedPayment(String groupBy, String value, Payment payment) {
        this.key = groupBy + "|" + value;
        this.groupBy = groupBy;
        this.value = value;
        this.payment = payment;
    }
}
