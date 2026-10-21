package io.paystream.flink.serde;

import io.paystream.avro.PaymentEvent;
import io.paystream.flink.model.Envelope;
import io.paystream.flink.model.Payment;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Decodes Avro payment events, resolving each record's writer schema against the job's reader schema.
 *
 * <p>Schema evolution happens here: a v1 record (no {@code channel}) read with the v2 reader schema
 * gets {@code channel = null}. A record that cannot be decoded is not dropped and does not fail the
 * job; it is emitted as a failed {@link Envelope} and routed to the dead-letter topic.
 */
public class PaymentEventDeserializer implements KafkaRecordDeserializationSchema<Envelope> {
    private final SchemaRegistry registry;
    private transient Map<Integer, SpecificDatumReader<PaymentEvent>> readers;
    private transient BinaryDecoder decoder;
    private transient Counter invalid;

    public PaymentEventDeserializer(SchemaRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        invalid = context.getMetricGroup().counter("invalidRecords");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Envelope> out) {
        try {
            Payment p = toPayment(decode(record.value()));
            p.ingestTime = record.timestamp();
            p.kafkaPartition = record.partition();
            p.kafkaOffset = record.offset();
            out.collect(Envelope.ok(p));
        } catch (RuntimeException | IOException e) {
            if (invalid != null) invalid.inc();
            out.collect(Envelope.failed(record.value(), e.getClass().getSimpleName() + ": " + e.getMessage(),
                    record.topic(), record.partition(), record.offset(), record.timestamp()));
        }
    }

    PaymentEvent decode(byte[] message) throws IOException {
        int id = ConfluentWireFormat.schemaId(message);
        if (readers == null) readers = new HashMap<>();
        SpecificDatumReader<PaymentEvent> reader = readers.computeIfAbsent(id, i -> {
            Schema writer = registry.byId(i);
            return new SpecificDatumReader<>(writer, PaymentEvent.getClassSchema());
        });
        decoder = DecoderFactory.get().binaryDecoder(message, ConfluentWireFormat.HEADER,
                message.length - ConfluentWireFormat.HEADER, decoder);
        return reader.read(null, decoder);
    }

    static Payment toPayment(PaymentEvent e) {
        Payment p = new Payment();
        p.txnId = e.getTxnId();
        p.eventTime = e.getEventTime().toEpochMilli();
        p.txnType = e.getTxnType().name();
        p.senderId = e.getSenderId();
        p.receiverId = e.getReceiverId();
        p.amountPaisa = e.getAmountPaisa();
        p.currency = e.getCurrency();
        p.deviceId = e.getDeviceId();
        p.district = e.getDistrict();
        p.status = e.getStatus().name();
        p.channel = e.getChannel() == null ? null : e.getChannel().name();
        return p;
    }

    @Override
    public TypeInformation<Envelope> getProducedType() {
        return TypeInformation.of(Envelope.class);
    }
}
