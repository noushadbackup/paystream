package io.paystream.flink.serde;

import io.paystream.avro.FraudAlert;
import io.paystream.avro.FraudRule;
import io.paystream.flink.model.Alert;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Writes alerts as Confluent-framed Avro, keyed by account so one account's alerts stay ordered. */
public class AlertSerializer implements KafkaRecordSerializationSchema<Alert> {
    private final SchemaRegistry registry;
    private final String topic;
    private transient int schemaId;
    private transient SpecificDatumWriter<FraudAlert> writer;
    private transient BinaryEncoder encoder;

    public AlertSerializer(SchemaRegistry registry, String topic) {
        this.registry = registry;
        this.topic = topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) {
        schemaId = registry.register(topic + "-value", FraudAlert.getClassSchema());
        writer = new SpecificDatumWriter<>(FraudAlert.class);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Alert a, KafkaSinkContext context, Long timestamp) {
        return new ProducerRecord<>(topic, null, a.detectedAt,
                a.accountId.getBytes(StandardCharsets.UTF_8), encode(a));
    }

    byte[] encode(Alert a) {
        FraudAlert avro = FraudAlert.newBuilder()
                .setAlertId(a.alertId)
                .setRule(FraudRule.valueOf(a.rule))
                .setAccountId(a.accountId)
                .setTriggerTxnId(a.triggerTxnId)
                .setEventTime(Instant.ofEpochMilli(a.eventTime))
                .setIngestTime(Instant.ofEpochMilli(a.ingestTime))
                .setDetectedAt(Instant.ofEpochMilli(a.detectedAt))
                .setScore(a.score)
                .setDetails(a.details)
                .build();
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(256);
            out.write(ConfluentWireFormat.header(schemaId));
            encoder = EncoderFactory.get().binaryEncoder(out, encoder);
            writer.write(avro, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
