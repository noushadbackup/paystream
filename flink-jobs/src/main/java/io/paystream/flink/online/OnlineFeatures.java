package io.paystream.flink.online;

import io.paystream.flink.model.Payment;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keeps an account's (or device's) recent activity as time buckets and publishes them to the online
 * store, where the decision service reads them in the payment's critical path.
 *
 * <p>Why buckets and not finished numbers: a feature like "transfers in the last 10 minutes" written
 * as a number is only true at the moment it was written. Buckets let the reader compute the window
 * at decision time, so a quiet account's counters age out correctly without any write. Writing the
 * whole bucket set is also idempotent, so replay after a failure cannot double count.
 *
 * <p>Writes are coalesced per key: at most one every {@code flushInterval} of processing time, which
 * bounds Redis load for busy accounts at the cost of that much staleness.
 *
 * <p>State layout is chosen for RocksDB, where every state access is a lookup and a
 * (de)serialization: the minute and hour buckets, the event-time clock and the pending-flush flag
 * live in one value per key ({@link Activity}), so a payment costs one read and one write for all
 * of them instead of one per bucket. Peers and devices stay in map state: they can hold hundreds
 * of entries and only one changes per payment.
 *
 * <p>Value format (contract with paystream_decision.features):
 * {@code {"v":1,"as_of":ms,"m":[[minuteStart,n,amt,failed,cashOut]...],"h":[[hourStart,...]...],"p":[[peer,minute]...]}}
 */
public class OnlineFeatures extends KeyedProcessFunction<String, Payment, RedisWrite> {
    public enum Entity {
        /** The paying account: everything it sent, failed attempts included; peers are its receivers. */
        SENDER("f:s:"),
        /** The receiving account: everything it got; peers are its senders. */
        RECEIVER("f:r:"),
        /** The handset: everything sent from it; peers are the accounts that used it. */
        DEVICE("f:d:");

        final String prefix;

        Entity(String prefix) {
            this.prefix = prefix;
        }
    }

    static final long MINUTE = 60_000, HOUR = 3_600_000;
    static final int MINUTES_KEPT = 60, HOURS_KEPT = 24, MAX_PEERS = 500;
    static final long TTL_S = 25 * 3600;
    static final long KNOWN_DEVICE_TTL_S = 30L * 24 * 3600;

    private final Entity entity;
    private final long flushIntervalMs;
    private transient ValueState<Activity> activity;
    private transient MapState<String, Long> peers;
    private transient MapState<String, Long> devices;

    public OnlineFeatures(Entity entity, Duration flushInterval) {
        this.entity = entity;
        this.flushIntervalMs = flushInterval.toMillis();
    }

    public static String keyOf(Entity e, Payment p) {
        return switch (e) {
            case SENDER -> p.senderId;
            case RECEIVER -> p.receiverId;
            case DEVICE -> p.deviceId;
        };
    }

    @Override
    public void open(OpenContext ctx) {
        activity = getRuntimeContext().getState(new ValueStateDescriptor<>("activity", Types.POJO(Activity.class)));
        peers = getRuntimeContext().getMapState(new MapStateDescriptor<>("peers", Types.STRING, Types.LONG));
        devices = getRuntimeContext().getMapState(new MapStateDescriptor<>("devices", Types.STRING, Types.LONG));
    }

    @Override
    public void processElement(Payment p, Context ctx, Collector<RedisWrite> out) throws Exception {
        boolean ok = p.isSuccess();
        long amt = ok ? p.amountPaisa : 0;
        long cashOut = ok && "CASH_OUT".equals(p.txnType) ? p.amountPaisa : 0;
        Activity a = activity.value();
        if (a == null) a = new Activity();
        a.minutes = add(a.minutes, Math.floorDiv(p.eventTime, MINUTE) * MINUTE, amt, ok ? 0 : 1, cashOut);
        a.hours = add(a.hours, Math.floorDiv(p.eventTime, HOUR) * HOUR, amt, ok ? 0 : 1, cashOut);
        if (p.eventTime > a.asOf) a.asOf = p.eventTime;

        String peer = entity == Entity.SENDER ? p.receiverId : p.senderId;
        long minute = Math.floorDiv(p.eventTime, MINUTE) * MINUTE;
        Long seen = peers.get(peer);
        if (seen == null || seen < minute) peers.put(peer, minute);

        // Devices an account has paid from: the decision service steps up authentication on a new one.
        if (entity == Entity.SENDER && ok && p.senderId.startsWith("U") && p.deviceId != null) {
            Long last = devices.get(p.deviceId);
            if (last == null || p.eventTime - last > 24 * HOUR) {  // refresh the TTL at most daily
                devices.put(p.deviceId, p.eventTime);
                out.collect(RedisWrite.sadd("devs:" + p.senderId, p.deviceId, KNOWN_DEVICE_TTL_S));
            }
        }

        if (!a.flushPending) {
            a.flushPending = true;
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + flushIntervalMs);
        }
        activity.update(a);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<RedisWrite> out) throws Exception {
        Activity a = activity.value();
        long now = a.asOf;
        a.flushPending = false;
        a.minutes = prune(a.minutes, Math.floorDiv(now, MINUTE) * MINUTE - (MINUTES_KEPT - 1) * MINUTE);
        a.hours = prune(a.hours, Math.floorDiv(now, HOUR) * HOUR - (HOURS_KEPT - 1) * HOUR);
        activity.update(a);
        List<Map.Entry<String, Long>> recent = new ArrayList<>();
        for (Iterator<Map.Entry<String, Long>> it = peers.iterator(); it.hasNext(); ) {
            Map.Entry<String, Long> e = it.next();
            if (e.getValue() <= now - MINUTES_KEPT * MINUTE) it.remove(); else recent.add(Map.entry(e.getKey(), e.getValue()));
        }
        recent.sort(Map.Entry.<String, Long>comparingByValue().reversed());

        StringBuilder json = new StringBuilder(512).append("{\"v\":1,\"as_of\":").append(now);
        appendBuckets(json.append(",\"m\":"), a.minutes);
        appendBuckets(json.append(",\"h\":"), a.hours);
        json.append(",\"p\":[");
        for (int i = 0; i < Math.min(MAX_PEERS, recent.size()); i++) {
            if (i > 0) json.append(',');
            json.append("[\"").append(escape(recent.get(i).getKey())).append("\",").append(recent.get(i).getValue()).append(']');
        }
        json.append("]}");
        out.collect(RedisWrite.set(entity.prefix + ctx.getCurrentKey(), json.toString(), TTL_S));
    }

    /** Adds to the bucket starting at {@code start}, creating it in order if new. */
    static long[] add(long[] rows, long start, long amt, long failed, long cashOut) {
        int i = 0;
        while (i < rows.length && rows[i] < start) i += Activity.WIDTH;
        if (i == rows.length || rows[i] != start) {
            long[] grown = new long[rows.length + Activity.WIDTH];
            System.arraycopy(rows, 0, grown, 0, i);
            System.arraycopy(rows, i, grown, i + Activity.WIDTH, rows.length - i);
            grown[i] = start;
            rows = grown;
        }
        rows[i + 1]++;
        rows[i + 2] += amt;
        rows[i + 3] += failed;
        rows[i + 4] += cashOut;
        return rows;
    }

    /** Drops buckets that start before {@code oldestKept}; rows are in start order. */
    static long[] prune(long[] rows, long oldestKept) {
        int i = 0;
        while (i < rows.length && rows[i] < oldestKept) i += Activity.WIDTH;
        return i == 0 ? rows : Arrays.copyOfRange(rows, i, rows.length);
    }

    private static void appendBuckets(StringBuilder json, long[] rows) {
        json.append('[');
        for (int i = 0; i < rows.length; i += Activity.WIDTH) {
            if (i > 0) json.append(',');
            json.append('[').append(rows[i]).append(',').append(rows[i + 1]).append(',').append(rows[i + 2])
                    .append(',').append(rows[i + 3]).append(',').append(rows[i + 4]).append(']');
        }
        json.append(']');
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * One key's buckets and clock. {@code minutes} and {@code hours} are rows of
     * {@code [start, count, amount, failed, cashOut]} flattened and kept in start order.
     */
    public static class Activity {
        static final int WIDTH = 5;
        public long asOf = Long.MIN_VALUE;
        public boolean flushPending;
        public long[] minutes = new long[0];
        public long[] hours = new long[0];

        public Activity() {}
    }
}
