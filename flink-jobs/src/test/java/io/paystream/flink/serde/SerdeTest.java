package io.paystream.flink.serde;

import static org.assertj.core.api.Assertions.assertThat;

import io.paystream.avro.FraudAlert;
import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Envelope;
import io.paystream.flink.model.Payment;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class SerdeTest {
    private final StubRegistry registry = new StubRegistry();

    private List<Envelope> decode(byte[] value) {
        var out = new ArrayList<Envelope>();
        var record = new ConsumerRecord<byte[], byte[]>("payments.events", 3, 42L, 1_790_000_000_500L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, value.length, null, value,
                new org.apache.kafka.common.header.internals.RecordHeaders(), java.util.Optional.empty());
        new PaymentEventDeserializer(registry).deserialize(record, new Collector<>() {
            public void collect(Envelope e) { out.add(e); }
            public void close() {}
        });
        return out;
    }

    @Test
    void decodesV2WithKafkaMetadata() {
        byte[] bytes = Avro.encode(registry, Avro.V2,
                Avro.payment("T1", 1_790_000_000_000L, "CASH_OUT", "U1", "A1", 250_000, "D1", "USSD"));
        Payment p = decode(bytes).get(0).payment;
        assertThat(p.txnId).isEqualTo("T1");
        assertThat(p.txnType).isEqualTo("CASH_OUT");
        assertThat(p.amountPaisa).isEqualTo(250_000);
        assertThat(p.channel).isEqualTo("USSD");
        assertThat(p.ingestTime).isEqualTo(1_790_000_000_500L);
        assertThat(p.kafkaPartition).isEqualTo(3);
        assertThat(p.kafkaOffset).isEqualTo(42L);
    }

    @Test
    void v1RecordsReadWithNullChannel() {
        byte[] bytes = Avro.encode(registry, Avro.V1,
                Avro.payment("T2", 1_790_000_000_000L, "P2P", "U1", "U2", 10_000, "D1", null));
        Envelope e = decode(bytes).get(0);
        assertThat(e.isValid()).isTrue();
        assertThat(e.payment.channel).isNull();
    }

    @Test
    void garbageGoesToDeadLetterInsteadOfFailingTheJob() {
        Envelope e = decode(new byte[] {7, 1, 2, 3, 4, 5, 6}).get(0);
        assertThat(e.isValid()).isFalse();
        assertThat(e.error).contains("magic byte");
        assertThat(e.offset).isEqualTo(42L);
    }

    @Test
    void alertRoundTripsThroughAvro() throws Exception {
        var ser = new AlertSerializer(registry, "fraud.alerts");
        ser.open(null, null);
        Alert a = Alert.of("MULE_FAN_IN", "U9", Payment.of("T9", 1_790_000_000_000L, "P2P", "U1", "U9", 100, "D1"),
                1.5, "9 distinct senders");
        byte[] bytes = ser.serialize(a, null, null).value();
        assertThat(bytes[0]).isZero();
        FraudAlert back = new SpecificDatumReader<>(FraudAlert.class)
                .read(null, DecoderFactory.get().binaryDecoder(bytes, 5, bytes.length - 5, null));
        assertThat(back.getAlertId()).isEqualTo("MULE_FAN_IN:U9:T9");
        assertThat(back.getRule().name()).isEqualTo("MULE_FAN_IN");
        assertThat(back.getScore()).isEqualTo(1.5);
    }
}
