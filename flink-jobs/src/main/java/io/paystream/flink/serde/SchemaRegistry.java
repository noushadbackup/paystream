package io.paystream.flink.serde;

import java.io.Serializable;
import org.apache.avro.Schema;

/**
 * The two Schema Registry calls the job needs. An interface so tests can run without a registry.
 *
 * <p>Implemented by hand on the registry's REST API instead of pulling in Confluent's client, which
 * keeps the job jar on Maven Central dependencies only.
 */
public interface SchemaRegistry extends Serializable {
    /** Writer schema for a Confluent wire-format schema id. */
    Schema byId(int id);

    /** Registers (or finds) a schema under a subject and returns its id. */
    int register(String subject, Schema schema);
}
