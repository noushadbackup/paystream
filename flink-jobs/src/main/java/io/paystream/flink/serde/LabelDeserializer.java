package io.paystream.flink.serde;

import io.paystream.avro.FraudLabel;
import io.paystream.flink.model.Label;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Decodes fraud labels into the lakehouse, where backtests measure rule precision against them. */
public class LabelDeserializer implements KafkaRecordDeserializationSchema<Label> {
    private static final Logger LOG = LoggerFactory.getLogger(LabelDeserializer.class);
    private final SchemaRegistry registry;
    private transient Map<Integer, SpecificDatumReader<FraudLabel>> readers;
    private transient BinaryDecoder decoder;

    public LabelDeserializer(SchemaRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Label> out) {
        try {
            byte[] m = record.value();
            if (readers == null) readers = new HashMap<>();
            SpecificDatumReader<FraudLabel> reader = readers.computeIfAbsent(ConfluentWireFormat.schemaId(m),
                    id -> new SpecificDatumReader<>(registry.byId(id), FraudLabel.getClassSchema()));
            decoder = DecoderFactory.get().binaryDecoder(m, ConfluentWireFormat.HEADER,
                    m.length - ConfluentWireFormat.HEADER, decoder);
            FraudLabel f = reader.read(null, decoder);
            Label l = new Label();
            l.txnId = f.getTxnId();
            l.scenario = f.getScenario();
            l.accountId = f.getAccountId();
            l.labelledAt = record.timestamp();
            out.collect(l);
        } catch (RuntimeException | IOException e) {
            // labels are measurement data: a bad one is logged and skipped, never allowed to stop payments
            LOG.warn("skipping undecodable label at {}-{}@{}: {}", record.topic(), record.partition(), record.offset(),
                    e.getMessage());
        }
    }

    @Override
    public TypeInformation<Label> getProducedType() {
        return TypeInformation.of(Label.class);
    }
}
