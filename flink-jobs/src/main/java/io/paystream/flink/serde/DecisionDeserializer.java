package io.paystream.flink.serde;

import io.paystream.flink.model.DecisionRecord;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JSON decision log records; a malformed one is logged and skipped rather than failing the job. */
public class DecisionDeserializer implements DeserializationSchema<DecisionRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(DecisionDeserializer.class);

    @Override
    public DecisionRecord deserialize(byte[] message) {
        throw new UnsupportedOperationException("use the collector variant");
    }

    @Override
    public void deserialize(byte[] message, Collector<DecisionRecord> out) {
        try {
            out.collect(DecisionRecord.parse(message));
        } catch (IOException | RuntimeException e) {
            LOG.warn("skipping malformed decision record: {}", e.getMessage());
        }
    }

    @Override
    public boolean isEndOfStream(DecisionRecord next) {
        return false;
    }

    @Override
    public TypeInformation<DecisionRecord> getProducedType() {
        return TypeInformation.of(DecisionRecord.class);
    }
}
