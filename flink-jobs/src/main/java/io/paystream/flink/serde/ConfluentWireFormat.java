package io.paystream.flink.serde;

import java.nio.ByteBuffer;

/** Confluent framing: magic byte 0, 4-byte big-endian schema id, then the Avro binary body. */
final class ConfluentWireFormat {
    static final byte MAGIC = 0;
    static final int HEADER = 5;

    private ConfluentWireFormat() {}

    static int schemaId(byte[] message) {
        if (message == null || message.length < HEADER) {
            throw new IllegalArgumentException("record too short for Confluent framing");
        }
        if (message[0] != MAGIC) {
            throw new IllegalArgumentException("unknown magic byte " + message[0]);
        }
        return ByteBuffer.wrap(message, 1, 4).getInt();
    }

    static byte[] header(int schemaId) {
        return ByteBuffer.allocate(HEADER).put(MAGIC).putInt(schemaId).array();
    }
}
