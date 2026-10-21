package io.paystream.flink.online;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class OnlineFeaturesTest {
    private static final long T0 = 1_790_000_070_000L; // 30 s into a minute
    private static final ObjectMapper JSON = new ObjectMapper();
    private int seq;

    private KeyedOneInputStreamOperatorTestHarness<String, Payment, RedisWrite> harness(OnlineFeatures.Entity e)
            throws Exception {
        var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new OnlineFeatures(e, Duration.ofMillis(200))),
                p -> OnlineFeatures.keyOf(e, p), Types.STRING);
        h.open();
        h.setProcessingTime(0);
        return h;
    }

    private Payment pay(long t, String type, String from, String to, long taka, String device, String status) {
        Payment p = Payment.of("T" + (seq++), t, type, from, to, taka * 100, device);
        p.status = status;
        return p;
    }

    private static List<RedisWrite> sets(List<RedisWrite> out) {
        return out.stream().filter(w -> w.op == RedisWrite.Op.SET).toList();
    }

    @Test
    void writesAreCoalescedPerKeyAndCarryMinuteBuckets() throws Exception {
        try (var h = harness(OnlineFeatures.Entity.SENDER)) {
            h.processElement(pay(T0, "P2P", "U1", "U2", 1000, "D1", "SUCCESS"), T0);
            h.processElement(pay(T0 + 5_000, "CASH_OUT", "U1", "A1", 5000, "D1", "SUCCESS"), T0);
            h.processElement(pay(T0 + 30_000, "P2P", "U1", "U3", 700, "D1", "FAILED"), T0); // next minute
            assertThat(sets(h.extractOutputValues())).as("nothing until the flush timer").isEmpty();

            h.setProcessingTime(200);
            List<RedisWrite> out = sets(h.extractOutputValues());
            assertThat(out).singleElement().satisfies(w -> {
                assertThat(w.key).isEqualTo("f:s:U1");
                assertThat(w.ttlSeconds).isEqualTo(25 * 3600);
            });
            JsonNode v = JSON.readTree(out.get(0).value);
            assertThat(v.get("as_of").asLong()).isEqualTo(T0 + 30_000);
            long m0 = T0 / 60_000 * 60_000;
            assertThat(v.get("m").toString()).isEqualTo("[[" + m0 + ",2,600000,0,500000],[" + (m0 + 60_000) + ",1,0,1,0]]");
            assertThat(v.get("p").size()).isEqualTo(3);
        }
    }

    @Test
    void oldBucketsAndPeersAgeOut() throws Exception {
        try (var h = harness(OnlineFeatures.Entity.RECEIVER)) {
            h.processElement(pay(T0, "P2P", "U9", "U1", 100, "D9", "SUCCESS"), T0);
            h.setProcessingTime(200);
            h.processElement(pay(T0 + 2 * 3_600_000, "P2P", "U8", "U1", 100, "D8", "SUCCESS"), T0);
            h.setProcessingTime(400);
            JsonNode v = JSON.readTree(sets(h.extractOutputValues()).get(1).value);
            assertThat(v.get("m").size()).isEqualTo(1);
            assertThat(v.get("h").size()).as("hour buckets keep 24 h").isEqualTo(2);
            assertThat(v.get("p").toString()).contains("U8").doesNotContain("U9");
        }
    }

    @Test
    void knownDevicesAreRecordedOncePerDay() throws Exception {
        try (var h = harness(OnlineFeatures.Entity.SENDER)) {
            h.processElement(pay(T0, "P2P", "U1", "U2", 10, "D1", "SUCCESS"), T0);
            h.processElement(pay(T0 + 1_000, "P2P", "U1", "U2", 10, "D1", "SUCCESS"), T0);
            h.processElement(pay(T0 + 2_000, "P2P", "U1", "U2", 10, "D2", "FAILED"), T0); // a failed attempt proves nothing
            List<RedisWrite> sadd = h.extractOutputValues().stream().filter(w -> w.op == RedisWrite.Op.SADD).toList();
            assertThat(sadd).singleElement().satisfies(w -> {
                assertThat(w.key).isEqualTo("devs:U1");
                assertThat(w.field).isEqualTo("D1");
            });
        }
    }

    @Test
    void onlyLiveAlertsBecomeFlags() {
        Payment p = Payment.of("T1", T0, "P2P", "U1", "U2", 1, "D1");
        Alert live = Alert.of("DYNAMIC", "U1", p, 1.0, "x");
        live.ruleId = "failed-pin-burst";
        live.action = "DECLINE";
        Alert shadow = Alert.of("DYNAMIC", "U1", p, 1.0, "x");
        shadow.shadow = true;
        List<RedisWrite> out = new ArrayList<>();
        Collector<RedisWrite> c = new Collector<>() {
            public void collect(RedisWrite w) {
                out.add(w);
            }

            public void close() {}
        };
        new RiskFlags(3600).flatMap(live, c);
        new RiskFlags(3600).flatMap(shadow, c);
        assertThat(out).singleElement().satisfies(w -> {
            assertThat(w.key).isEqualTo("flags:U1");
            assertThat(w.field).isEqualTo("failed-pin-burst");
            assertThat(w.value).contains("\"action\":\"DECLINE\"");
        });
    }
}
