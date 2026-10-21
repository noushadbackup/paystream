package io.paystream.flink.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.paystream.flink.model.Envelope;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Undecodable records go to a DLQ as JSON: the original bytes plus where they came from and why they failed. */
public class DeadLetterSerializer implements KafkaRecordSerializationSchema<Envelope> {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final String topic;

    public DeadLetterSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Envelope e, KafkaSinkContext context, Long timestamp) {
        ObjectNode n = JSON.createObjectNode()
                .put("source_topic", e.topic)
                .put("partition", e.partition)
                .put("offset", e.offset)
                .put("ingest_time", e.ingestTime)
                .put("error", e.error)
                .put("raw_base64", e.raw == null ? null : Base64.getEncoder().encodeToString(e.raw));
        return new ProducerRecord<>(topic, n.toString().getBytes(StandardCharsets.UTF_8));
    }
}
