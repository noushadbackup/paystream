package io.paystream.flink.serde;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/** Test helper: encode a payment the way the Python producer does, with any schema version. */
public final class Avro {
    public static final Schema V1 = parse("../schemas/history/payment_event.v1.avsc");
    public static final Schema V2 = parse("../schemas/payment_event.avsc");

    private Avro() {}

    private static Schema parse(String path) {
        try {
            return new Schema.Parser().parse(new File(path));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] encode(SchemaRegistry registry, Schema schema, Map<String, Object> fields) {
        GenericRecord r = new GenericData.Record(schema);
        fields.forEach((k, v) -> {
            Schema.Field f = schema.getField(k);
            if (f == null) return;
            Schema fs = f.schema();
            if (fs.getType() == Schema.Type.UNION) fs = fs.getTypes().get(1);
            r.put(k, fs.getType() == Schema.Type.ENUM && v != null ? new GenericData.EnumSymbol(fs, v) : v);
        });
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(ConfluentWireFormat.header(registry.register("payments.events-value", schema)));
            BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(schema).write(r, enc);
            enc.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Map<String, Object> payment(String txnId, long t, String type, String from, String to, long paisa,
                                              String device, String channel) {
        var m = new java.util.HashMap<String, Object>();
        m.put("txn_id", txnId);
        m.put("event_time", t);
        m.put("txn_type", type);
        m.put("sender_id", from);
        m.put("receiver_id", to);
        m.put("amount_paisa", paisa);
        m.put("currency", "BDT");
        m.put("device_id", device);
        m.put("district", "Dhaka");
        m.put("status", "SUCCESS");
        m.put("channel", channel);
        return m;
    }
}
