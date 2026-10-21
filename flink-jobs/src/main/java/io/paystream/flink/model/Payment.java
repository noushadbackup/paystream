package io.paystream.flink.model;

import java.util.Objects;

/**
 * A payment as processed inside Flink.
 *
 * <p>Deliberately a plain POJO rather than the generated Avro class: Flink serializes POJOs
 * efficiently, the processing model does not change when the wire schema evolves, and tests
 * can build one in a line.
 */
public class Payment {
    public String txnId;
    /** When the customer initiated the payment, epoch millis UTC. Drives event-time logic. */
    public long eventTime;
    public String txnType;
    public String senderId;
    public String receiverId;
    public long amountPaisa;
    public String currency;
    public String deviceId;
    public String district;
    public String status;
    /** Null for events written with schema v1. */
    public String channel;
    /** Kafka record timestamp: when the gateway received it. */
    public long ingestTime;
    public int kafkaPartition;
    public long kafkaOffset;

    public Payment() {}

    public static Payment of(String txnId, long eventTime, String txnType, String sender, String receiver,
                             long amountPaisa, String deviceId) {
        Payment p = new Payment();
        p.txnId = txnId;
        p.eventTime = eventTime;
        p.txnType = txnType;
        p.senderId = sender;
        p.receiverId = receiver;
        p.amountPaisa = amountPaisa;
        p.currency = "BDT";
        p.deviceId = deviceId;
        p.district = "Dhaka";
        p.status = "SUCCESS";
        p.ingestTime = eventTime;
        return p;
    }

    public boolean isSuccess() {
        return "SUCCESS".equals(status);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Payment p)) return false;
        return eventTime == p.eventTime && amountPaisa == p.amountPaisa && ingestTime == p.ingestTime
                && kafkaPartition == p.kafkaPartition && kafkaOffset == p.kafkaOffset
                && Objects.equals(txnId, p.txnId) && Objects.equals(txnType, p.txnType)
                && Objects.equals(senderId, p.senderId) && Objects.equals(receiverId, p.receiverId)
                && Objects.equals(currency, p.currency) && Objects.equals(deviceId, p.deviceId)
                && Objects.equals(district, p.district) && Objects.equals(status, p.status)
                && Objects.equals(channel, p.channel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txnId, eventTime, ingestTime, kafkaOffset);
    }

    @Override
    public String toString() {
        return "Payment{" + txnId + " " + txnType + " " + senderId + "->" + receiverId + " " + amountPaisa + "p @" + eventTime + "}";
    }
}
