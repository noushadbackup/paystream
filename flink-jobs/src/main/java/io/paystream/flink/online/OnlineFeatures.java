package io.paystream.flink.online;

import io.paystream.flink.model.Payment;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
    private transient MapState<Long, long[]> minutes;
    private transient MapState<Long, long[]> hours;
    private transient MapState<String, Long> peers;
    private transient MapState<String, Long> devices;
    private transient ValueState<Long> asOf;
    private transient ValueState<Boolean> flushPending;

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
        TypeInformation<long[]> arr = PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO;
        minutes = getRuntimeContext().getMapState(new MapStateDescriptor<>("minutes", Types.LONG, arr));
        hours = getRuntimeContext().getMapState(new MapStateDescriptor<>("hours", Types.LONG, arr));
        peers = getRuntimeContext().getMapState(new MapStateDescriptor<>("peers", Types.STRING, Types.LONG));
        devices = getRuntimeContext().getMapState(new MapStateDescriptor<>("devices", Types.STRING, Types.LONG));
        asOf = getRuntimeContext().getState(new ValueStateDescriptor<>("asOf", Types.LONG));
        flushPending = getRuntimeContext().getState(new ValueStateDescriptor<>("flushPending", Types.BOOLEAN));
    }

    @Override
    public void processElement(Payment p, Context ctx, Collector<RedisWrite> out) throws Exception {
        boolean ok = p.isSuccess();
        long amt = ok ? p.amountPaisa : 0;
        long cashOut = ok && "CASH_OUT".equals(p.txnType) ? p.amountPaisa : 0;
        add(minutes, Math.floorDiv(p.eventTime, MINUTE) * MINUTE, amt, ok ? 0 : 1, cashOut);
        add(hours, Math.floorDiv(p.eventTime, HOUR) * HOUR, amt, ok ? 0 : 1, cashOut);

        String peer = entity == Entity.SENDER ? p.receiverId : p.senderId;
        long minute = Math.floorDiv(p.eventTime, MINUTE) * MINUTE;
        Long seen = peers.get(peer);
        if (seen == null || seen < minute) peers.put(peer, minute);

        Long prev = asOf.value();
        if (prev == null || p.eventTime > prev) asOf.update(p.eventTime);

        // Devices an account has paid from: the decision service steps up authentication on a new one.
        if (entity == Entity.SENDER && ok && p.senderId.startsWith("U") && p.deviceId != null) {
            Long last = devices.get(p.deviceId);
            if (last == null || p.eventTime - last > 24 * HOUR) {  // refresh the TTL at most daily
                devices.put(p.deviceId, p.eventTime);
                out.collect(RedisWrite.sadd("devs:" + p.senderId, p.deviceId, KNOWN_DEVICE_TTL_S));
            }
        }

        if (!Boolean.TRUE.equals(flushPending.value())) {
            flushPending.update(true);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + flushIntervalMs);
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<RedisWrite> out) throws Exception {
        flushPending.clear();
        long now = asOf.value();
        prune(minutes, Math.floorDiv(now, MINUTE) * MINUTE - (MINUTES_KEPT - 1) * MINUTE);
        prune(hours, Math.floorDiv(now, HOUR) * HOUR - (HOURS_KEPT - 1) * HOUR);
        List<Map.Entry<String, Long>> recent = new ArrayList<>();
        for (Iterator<Map.Entry<String, Long>> it = peers.iterator(); it.hasNext(); ) {
            Map.Entry<String, Long> e = it.next();
            if (e.getValue() <= now - MINUTES_KEPT * MINUTE) it.remove(); else recent.add(Map.entry(e.getKey(), e.getValue()));
        }
        recent.sort(Map.Entry.<String, Long>comparingByValue().reversed());

        StringBuilder json = new StringBuilder(512).append("{\"v\":1,\"as_of\":").append(now);
        appendBuckets(json.append(",\"m\":"), minutes);
        appendBuckets(json.append(",\"h\":"), hours);
        json.append(",\"p\":[");
        for (int i = 0; i < Math.min(MAX_PEERS, recent.size()); i++) {
            if (i > 0) json.append(',');
            json.append("[\"").append(escape(recent.get(i).getKey())).append("\",").append(recent.get(i).getValue()).append(']');
        }
        json.append("]}");
        out.collect(RedisWrite.set(entity.prefix + ctx.getCurrentKey(), json.toString(), TTL_S));
    }

    private static void add(MapState<Long, long[]> state, long bucket, long amt, long failed, long cashOut) throws Exception {
        long[] b = state.get(bucket);
        if (b == null) b = new long[4];
        b[0]++;
        b[1] += amt;
        b[2] += failed;
        b[3] += cashOut;
        state.put(bucket, b);
    }

    private static void prune(MapState<Long, long[]> state, long oldestKept) throws Exception {
        for (Iterator<Long> it = state.keys().iterator(); it.hasNext(); ) {
            if (it.next() < oldestKept) it.remove();
        }
    }

    private static void appendBuckets(StringBuilder json, MapState<Long, long[]> state) throws Exception {
        List<Map.Entry<Long, long[]>> rows = new ArrayList<>();
        for (Map.Entry<Long, long[]> e : state.entries()) rows.add(Map.entry(e.getKey(), e.getValue()));
        rows.sort(Comparator.comparingLong(Map.Entry::getKey));
        json.append('[');
        for (int i = 0; i < rows.size(); i++) {
            long[] b = rows.get(i).getValue();
            if (i > 0) json.append(',');
            json.append('[').append(rows.get(i).getKey()).append(',').append(b[0]).append(',').append(b[1])
                    .append(',').append(b[2]).append(',').append(b[3]).append(']');
        }
        json.append(']');
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
