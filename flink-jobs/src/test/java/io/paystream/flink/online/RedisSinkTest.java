package io.paystream.flink.online;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPooled;

/** Against a real Redis (a service container in CI); skipped when none is reachable. */
class RedisSinkTest {
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

    @Test
    void batchesAreWrittenWithTtlsAndLastWriteWins() throws Exception {
        JedisPooled redis = new JedisPooled("localhost", PORT);
        try {
            redis.ping();
        } catch (RuntimeException e) {
            assumeTrue(false, "no Redis on localhost:" + PORT);
        }
        redis.del("f:s:TEST", "flags:TEST", "devs:TEST");
        try (var w = new RedisSink.Writer(new JedisPooled("localhost", PORT), 1000, 60_000, new SimpleCounter(),
                new DescriptiveStatisticsHistogram(10))) {
            w.write(RedisWrite.set("f:s:TEST", "{\"v\":1}", 60), null);
            w.write(RedisWrite.set("f:s:TEST", "{\"v\":2}", 60), null);
            w.write(RedisWrite.hset("flags:TEST", "VELOCITY", "{}", 60), null);
            w.write(RedisWrite.sadd("devs:TEST", "D1", 60), null);
            assertThat(redis.get("f:s:TEST")).as("buffered until flush").isNull();
            w.flush(false); // what a checkpoint does
        }
        assertThat(redis.get("f:s:TEST")).isEqualTo("{\"v\":2}");
        assertThat(redis.ttl("f:s:TEST")).isBetween(1L, 60L);
        assertThat(redis.hget("flags:TEST", "VELOCITY")).isEqualTo("{}");
        assertThat(redis.sismember("devs:TEST", "D1")).isTrue();
        redis.close();
    }
}
