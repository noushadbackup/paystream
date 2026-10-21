package io.paystream.flink.model;

/** What the Kafka source emits: a decoded payment, or the raw bytes of a record that failed to decode. */
public class Envelope {
    public Payment payment;
    public byte[] raw;
    public String error;
    public String topic;
    public int partition;
    public long offset;
    public long ingestTime;

    public Envelope() {}

    public static Envelope ok(Payment p) {
        Envelope e = new Envelope();
        e.payment = p;
        e.partition = p.kafkaPartition;
        e.offset = p.kafkaOffset;
        e.ingestTime = p.ingestTime;
        return e;
    }

    public static Envelope failed(byte[] raw, String error, String topic, int partition, long offset, long ingestTime) {
        Envelope e = new Envelope();
        e.raw = raw;
        e.error = error;
        e.topic = topic;
        e.partition = partition;
        e.offset = offset;
        e.ingestTime = ingestTime;
        return e;
    }

    public boolean isValid() {
        return payment != null;
    }
}
