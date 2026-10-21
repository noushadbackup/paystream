package io.paystream.flink.rules.dynamic;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/** Runs every vector in rules/conformance: the Python backtester runs the same files. */
class ConformanceTest {
    static final Path VECTORS = Path.of("..", "rules", "conformance");
    private static final ObjectMapper JSON = new ObjectMapper();

    @TestFactory
    Stream<DynamicTest> conformance() throws Exception {
        List<Path> files;
        try (Stream<Path> s = Files.list(VECTORS)) {
            files = s.filter(p -> p.toString().endsWith(".json")).sorted().toList();
        }
        assertThat(files).isNotEmpty();
        return files.stream().map(f -> DynamicTest.dynamicTest(f.getFileName().toString(), () -> run(f)));
    }

    @Test
    void everyLibraryRuleParses() throws Exception {
        try (Stream<Path> s = Files.list(Path.of("..", "rules", "library"))) {
            List<Path> files = s.filter(p -> p.toString().endsWith(".json")).toList();
            assertThat(files).isNotEmpty();
            for (Path f : files) {
                RuleDef r = RuleDef.parse(Files.readString(f));
                assertThat(f.getFileName().toString()).isEqualTo(r.ruleId + ".json");
            }
        }
    }

    private static void run(Path file) throws Exception {
        JsonNode v = JSON.readTree(file.toFile());
        Map<String, Double> thresholds = new HashMap<>();
        try (DynamicHarness h = new DynamicHarness()) {
            for (JsonNode rule : v.get("rules")) {
                h.publish(JSON.writeValueAsString(rule));
                thresholds.put(rule.get("rule_id").asText(), rule.get("threshold").get("value").asDouble());
            }
            for (JsonNode e : v.get("events")) h.send(payment(e));

            List<String> actual = new ArrayList<>();
            for (Alert a : h.alerts()) {
                actual.add(row(a.ruleId, a.accountId, a.triggerTxnId, a.score * thresholds.get(a.ruleId), a.shadow));
            }
            List<String> expected = new ArrayList<>();
            for (JsonNode x : v.get("expected")) {
                expected.add(row(x.get("rule_id").asText(), x.get("key").asText(), x.get("trigger_txn_id").asText(),
                        x.get("observed").asDouble(), x.path("shadow").asBoolean(false)));
            }
            assertThat(actual).as(v.get("description").asText()).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    private static String row(String rule, String key, String txn, double observed, boolean shadow) {
        return String.format("%s %s %s %.2f%s", rule, key, txn, observed, shadow ? " shadow" : "");
    }

    static Payment payment(JsonNode e) {
        Payment p = Payment.of(e.get("txn_id").asText(), Instant.parse(e.get("at").asText()).toEpochMilli(),
                e.path("txn_type").asText("P2P"), e.path("sender_id").asText("U1"), e.path("receiver_id").asText("U2"),
                e.path("amount_paisa").asLong(100_000), e.path("device_id").asText("D1"));
        p.district = e.path("district").asText("Dhaka");
        p.status = e.path("status").asText("SUCCESS");
        p.channel = e.path("channel").asText("APP");
        p.currency = e.path("currency").asText("BDT");
        return p;
    }
}
