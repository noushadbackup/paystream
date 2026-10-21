package io.paystream.flink.online;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;

/**
 * Writes to Redis in pipelined batches: one round trip per batch instead of per write.
 *
 * <p>At-least-once: the buffer is flushed on every checkpoint, so a write acknowledged by a
 * checkpoint is in Redis. Every write is an overwrite (SET/HSET/SADD), so replays are harmless.
 * Writes to the same key inside a batch are collapsed to the last one. A background flush keeps
 * the online store fresh between checkpoints.
 */
public class RedisSink implements Sink<RedisWrite> {
    private final String host;
    private final int port;
    private final int maxBatch;
    private final long maxDelayMs;

    public RedisSink(String host, int port, int maxBatch, long maxDelayMs) {
        this.host = host;
        this.port = port;
        this.maxBatch = maxBatch;
        this.maxDelayMs = maxDelayMs;
    }

    @Override
    @SuppressWarnings("deprecation")
    public SinkWriter<RedisWrite> createWriter(InitContext ctx) {
        return new Writer(new JedisPooled(host, port), maxBatch, maxDelayMs,
                ctx.metricGroup().counter("redisWrites"),
                ctx.metricGroup().histogram("redisFlushMs", new DescriptiveStatisticsHistogram(1024)));
    }

    static final class Writer implements SinkWriter<RedisWrite> {
        private final JedisPooled redis;
        private final int maxBatch;
        private final Counter writes;
        private final Histogram flushMs;
        private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "redis-sink-flush");
            t.setDaemon(true);
            return t;
        });
        private final Map<String, RedisWrite> buffer = new LinkedHashMap<>();
        private volatile IOException failure;

        Writer(JedisPooled redis, int maxBatch, long maxDelayMs, Counter writes, Histogram flushMs) {
            this.redis = redis;
            this.maxBatch = maxBatch;
            this.writes = writes;
            this.flushMs = flushMs;
            ticker.scheduleWithFixedDelay(() -> {
                try {
                    flushBuffer();
                } catch (IOException e) {
                    failure = e;  // surfaced on the task thread at the next write, which fails the task
                }
            }, maxDelayMs, maxDelayMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public void write(RedisWrite w, Context ctx) throws IOException {
            if (failure != null) throw failure;
            boolean full;
            synchronized (buffer) {
                buffer.put(w.op + "|" + w.key + "|" + w.field, w);
                full = buffer.size() >= maxBatch;
            }
            if (full) flushBuffer();
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            flushBuffer();
            if (failure != null) throw failure;
        }

        void flushBuffer() throws IOException {
            List<RedisWrite> batch;
            synchronized (buffer) {
                if (buffer.isEmpty()) return;
                batch = new ArrayList<>(buffer.values());
                buffer.clear();
            }
            long start = System.nanoTime();
            try (Pipeline p = redis.pipelined()) {
                for (RedisWrite w : batch) {
                    switch (w.op) {
                        case SET -> p.setex(w.key, w.ttlSeconds, w.value);
                        case HSET -> {
                            p.hset(w.key, w.field, w.value);
                            p.expire(w.key, w.ttlSeconds);
                        }
                        case SADD -> {
                            p.sadd(w.key, w.field);
                            p.expire(w.key, w.ttlSeconds);
                        }
                    }
                }
                p.sync();
            } catch (RuntimeException e) {
                throw new IOException("Redis write of " + batch.size() + " entries failed", e);
            }
            writes.inc(batch.size());
            flushMs.update((System.nanoTime() - start) / 1_000_000);
        }

        @Override
        public void close() {
            ticker.shutdownNow();
            redis.close();
        }
    }
}
