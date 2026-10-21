package io.paystream.flink.model;

/** Ground truth for one transaction: which fraud pattern it belongs to and which account it is about. */
public class Label {
    public String txnId;
    public String scenario;
    public String accountId;
    /** Kafka timestamp: when the label became known. */
    public long labelledAt;

    public Label() {}
}
