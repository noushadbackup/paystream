package io.paystream.flink.serde;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;

/** In-memory registry. Static map so it survives Flink serializing the deserializer to tasks. */
public class StubRegistry implements SchemaRegistry {
    private static final Map<Integer, Schema> SCHEMAS = new HashMap<>();

    @Override
    public synchronized Schema byId(int id) {
        Schema s = SCHEMAS.get(id);
        if (s == null) throw new IllegalStateException("no schema " + id);
        return s;
    }

    @Override
    public synchronized int register(String subject, Schema schema) {
        for (var e : SCHEMAS.entrySet()) if (e.getValue().equals(schema)) return e.getKey();
        int id = SCHEMAS.size() + 1;
        SCHEMAS.put(id, schema);
        return id;
    }
}
