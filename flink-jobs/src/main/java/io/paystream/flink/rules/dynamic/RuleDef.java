package io.paystream.flink.rules.dynamic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.paystream.flink.model.Payment;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A rule an analyst writes as JSON and publishes without a deploy (contract: {@code rules/schema/rule.schema.json}).
 *
 * <pre>
 * { "rule_id": "pin-brute-force", "version": 3, "state": "ACTIVE",
 *   "group_by": "sender_id",
 *   "when": [{"field": "status", "op": "==", "value": "FAILED"}],
 *   "aggregate": {"fn": "COUNT"}, "window_s": 600,
 *   "threshold": {"op": ">=", "value": 5}, "cooldown_s": 600, "action": "DECLINE" }
 * </pre>
 *
 * Meaning: for each payment that matches {@code when}, aggregate every payment with the same
 * {@code group_by} value that also matches {@code when} and happened in (t - window, t]; alert if the
 * aggregate crosses the threshold. The Python backtester ({@code rules-service}) implements the same
 * semantics, and both run the vectors in {@code rules/conformance}.
 */
public final class RuleDef implements Serializable {
    public enum State { ACTIVE, SHADOW, DISABLED }

    public enum Fn { COUNT, SUM, MIN, MAX, AVG, COUNT_DISTINCT }

    public static final Set<String> GROUP_BY = Set.of("sender_id", "receiver_id", "device_id", "district");
    public static final Set<String> STRING_FIELDS =
            Set.of("txn_type", "sender_id", "receiver_id", "device_id", "district", "status", "channel", "currency");
    public static final Set<String> NUMBER_FIELDS = Set.of("amount_paisa", "hour_dhaka");
    private static final Pattern ID = Pattern.compile("[a-z0-9][a-z0-9-]{2,63}");
    private static final long MAX_WINDOW_S = 7L * 24 * 3600;
    private static final ObjectMapper JSON = new ObjectMapper();

    public String ruleId;
    public int version;
    public String name;
    public State state;
    public String groupBy;
    public List<Condition> when = new ArrayList<>();
    public Fn fn;
    public String aggField;
    public long windowMs;
    public boolean thresholdInclusive;
    public double threshold;
    public long cooldownMs;
    public String action;

    /** Parses and validates; throws {@link IllegalArgumentException} with a message an analyst can act on. */
    public static RuleDef parse(String json) {
        JsonNode n;
        try {
            n = JSON.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("not valid JSON: " + e.getMessage());
        }
        if (n == null || !n.isObject()) throw new IllegalArgumentException("a rule must be a JSON object");
        RuleDef r = new RuleDef();
        r.ruleId = text(n, "rule_id");
        if (!ID.matcher(r.ruleId).matches()) {
            throw new IllegalArgumentException("rule_id must be 3-64 chars of a-z, 0-9 and '-': " + r.ruleId);
        }
        r.version = (int) number(n, "version");
        if (r.version < 1) throw new IllegalArgumentException("version must be >= 1");
        r.name = n.path("name").asText(r.ruleId);
        r.state = enumOf(State.class, text(n, "state"), "state");
        r.groupBy = text(n, "group_by");
        if (!GROUP_BY.contains(r.groupBy)) throw new IllegalArgumentException("group_by must be one of " + GROUP_BY);

        for (JsonNode c : n.path("when")) r.when.add(Condition.parse(c));

        JsonNode agg = n.path("aggregate");
        r.fn = enumOf(Fn.class, text(agg, "fn"), "aggregate.fn");
        if (r.fn != Fn.COUNT) {
            r.aggField = text(agg, "field");
            boolean numeric = NUMBER_FIELDS.contains(r.aggField);
            if (r.fn == Fn.COUNT_DISTINCT ? !(numeric || STRING_FIELDS.contains(r.aggField)) : !numeric) {
                throw new IllegalArgumentException(r.fn + " needs a " + (r.fn == Fn.COUNT_DISTINCT ? "" : "numeric ")
                        + "field, got " + r.aggField);
            }
        }

        long windowS = number(n, "window_s");
        if (windowS < 1 || windowS > MAX_WINDOW_S) throw new IllegalArgumentException("window_s must be 1.." + MAX_WINDOW_S);
        r.windowMs = windowS * 1000;

        JsonNode th = n.path("threshold");
        String op = text(th, "op");
        if (!op.equals(">=") && !op.equals(">")) throw new IllegalArgumentException("threshold.op must be >= or >");
        r.thresholdInclusive = op.equals(">=");
        r.threshold = th.path("value").asDouble(Double.NaN);
        if (!(r.threshold > 0)) throw new IllegalArgumentException("threshold.value must be a positive number");

        r.cooldownMs = n.path("cooldown_s").asLong(600) * 1000;
        if (r.cooldownMs < 0) throw new IllegalArgumentException("cooldown_s must be >= 0");
        r.action = n.path("action").asText("REVIEW");
        if (!r.action.equals("REVIEW") && !r.action.equals("DECLINE")) {
            throw new IllegalArgumentException("action must be REVIEW or DECLINE");
        }
        return r;
    }

    public boolean matches(Payment p) {
        for (Condition c : when) if (!c.test(p)) return false;
        return true;
    }

    public boolean crosses(double observed) {
        return thresholdInclusive ? observed >= threshold : observed > threshold;
    }

    /** The value of a field on a payment: a String, a Long, or null. */
    public static Object field(Payment p, String field) {
        return switch (field) {
            case "txn_type" -> p.txnType;
            case "sender_id" -> p.senderId;
            case "receiver_id" -> p.receiverId;
            case "device_id" -> p.deviceId;
            case "district" -> p.district;
            case "status" -> p.status;
            case "channel" -> p.channel;
            case "currency" -> p.currency;
            case "amount_paisa" -> p.amountPaisa;
            // Asia/Dhaka is UTC+6 all year (no DST since 2009)
            case "hour_dhaka" -> Math.floorMod(Math.floorDiv(p.eventTime, 3_600_000L) + 6, 24L);
            default -> throw new IllegalArgumentException("unknown field " + field);
        };
    }

    private static String text(JsonNode n, String key) {
        JsonNode v = n.get(key);
        if (v == null || !v.isTextual() || v.asText().isEmpty()) throw new IllegalArgumentException(key + " is required");
        return v.asText();
    }

    private static long number(JsonNode n, String key) {
        JsonNode v = n.get(key);
        if (v == null || !v.isIntegralNumber()) throw new IllegalArgumentException(key + " must be an integer");
        return v.asLong();
    }

    private static <E extends Enum<E>> E enumOf(Class<E> type, String value, String key) {
        try {
            return Enum.valueOf(type, value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(key + " must be one of " + java.util.Arrays.toString(type.getEnumConstants()));
        }
    }

    /** One {@code {"field", "op", "value"}} clause; all clauses of a rule must hold. */
    public static final class Condition implements Serializable {
        public String field;
        public String op;
        public Set<String> strings;
        public double number;

        static Condition parse(JsonNode n) {
            Condition c = new Condition();
            c.field = text(n, "field");
            c.op = text(n, "op");
            JsonNode v = n.get("value");
            if (v == null) throw new IllegalArgumentException("condition on " + c.field + " needs a value");
            boolean numeric = NUMBER_FIELDS.contains(c.field);
            if (!numeric && !STRING_FIELDS.contains(c.field)) throw new IllegalArgumentException("unknown field " + c.field);
            switch (c.op) {
                case "==", "!=" -> {
                    if (numeric) c.number = requireNumber(v, c); else c.strings = Set.of(requireText(v, c));
                }
                case "in", "not_in" -> {
                    if (!v.isArray() || v.isEmpty()) throw new IllegalArgumentException(c.op + " needs a non-empty list");
                    c.strings = new HashSet<>();
                    for (JsonNode x : v) c.strings.add(numeric ? String.valueOf(requireNumber(x, c)) : requireText(x, c));
                }
                case "<", "<=", ">", ">=" -> {
                    if (!numeric) throw new IllegalArgumentException(c.op + " needs a numeric field, got " + c.field);
                    c.number = requireNumber(v, c);
                }
                case "starts_with" -> {
                    if (numeric) throw new IllegalArgumentException("starts_with needs a text field, got " + c.field);
                    c.strings = Set.of(requireText(v, c));
                }
                default -> throw new IllegalArgumentException("unknown op " + c.op);
            }
            return c;
        }

        boolean test(Payment p) {
            Object v = field(p, field);
            if (v == null) return op.equals("!=") || op.equals("not_in");
            if (v instanceof Long l) {
                double x = l;
                return switch (op) {
                    case "==" -> x == number;
                    case "!=" -> x != number;
                    case "<" -> x < number;
                    case "<=" -> x <= number;
                    case ">" -> x > number;
                    case ">=" -> x >= number;
                    case "in" -> strings.contains(String.valueOf(x));
                    case "not_in" -> !strings.contains(String.valueOf(x));
                    default -> false;
                };
            }
            String s = (String) v;
            return switch (op) {
                case "==", "in" -> strings.contains(s);
                case "!=", "not_in" -> !strings.contains(s);
                case "starts_with" -> s.startsWith(strings.iterator().next());
                default -> false;
            };
        }

        private static double requireNumber(JsonNode v, Condition c) {
            if (!v.isNumber()) throw new IllegalArgumentException(c.field + " compares to numbers, got " + v);
            return v.asDouble();
        }

        private static String requireText(JsonNode v, Condition c) {
            if (!v.isTextual()) throw new IllegalArgumentException(c.field + " compares to text, got " + v);
            return v.asText();
        }
    }
}
