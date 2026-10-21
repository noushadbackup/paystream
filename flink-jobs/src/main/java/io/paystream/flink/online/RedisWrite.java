package io.paystream.flink.online;

/** One idempotent write to the online store. Replaying it after a failure leaves the same result. */
public class RedisWrite {
    public enum Op { SET, HSET, SADD }

    public Op op;
    public String key;
    /** HSET field or SADD member; null for SET. */
    public String field;
    public String value;
    public long ttlSeconds;

    public RedisWrite() {}

    public static RedisWrite set(String key, String value, long ttlSeconds) {
        return of(Op.SET, key, null, value, ttlSeconds);
    }

    public static RedisWrite hset(String key, String field, String value, long ttlSeconds) {
        return of(Op.HSET, key, field, value, ttlSeconds);
    }

    public static RedisWrite sadd(String key, String member, long ttlSeconds) {
        return of(Op.SADD, key, member, null, ttlSeconds);
    }

    private static RedisWrite of(Op op, String key, String field, String value, long ttl) {
        RedisWrite w = new RedisWrite();
        w.op = op;
        w.key = key;
        w.field = field;
        w.value = value;
        w.ttlSeconds = ttl;
        return w;
    }

    @Override
    public String toString() {
        return op + " " + key + (field == null ? "" : " " + field) + (value == null ? "" : " " + value);
    }
}
