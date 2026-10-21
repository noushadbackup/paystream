package io.paystream.flink.rules.dynamic;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;

/**
 * The current rule set, kept in Flink broadcast state as the JSON each rule was published with.
 *
 * <p>State holds the raw JSON, not a parsed object, so the checkpoint format is the same contract
 * analysts write: changing {@link RuleDef} never breaks a restore. Parsing is cached per operator.
 */
public final class RuleBook {
    public static final MapStateDescriptor<String, String> STATE =
            new MapStateDescriptor<>("dynamic-rules", Types.STRING, Types.STRING);

    private final Map<String, RuleDef> parsed = new HashMap<>();
    private final Map<String, String> source = new HashMap<>();

    /** Rules that are not DISABLED, in rule_id order so evaluation is deterministic. */
    public List<RuleDef> live(ReadOnlyBroadcastState<String, String> state) throws Exception {
        List<RuleDef> out = new ArrayList<>();
        for (Map.Entry<String, String> e : state.immutableEntries()) {
            RuleDef r = get(e.getKey(), e.getValue());
            if (r.state != RuleDef.State.DISABLED) out.add(r);
        }
        out.sort(Comparator.comparing(r -> r.ruleId));
        return out;
    }

    /** Longest window among live rules for one group_by field: how long its events must be kept. */
    public long maxWindowMs(ReadOnlyBroadcastState<String, String> state, String groupBy) throws Exception {
        long max = 0;
        for (RuleDef r : live(state)) if (r.groupBy.equals(groupBy)) max = Math.max(max, r.windowMs);
        return max;
    }

    /**
     * Whether an update should replace what is in state. Versions only move forward, so replaying the
     * rules topic, or an old message arriving late, never rolls a rule back.
     */
    public static boolean supersedes(RuleDef update, String currentJson) {
        return currentJson == null || update.version >= RuleDef.parse(currentJson).version;
    }

    private RuleDef get(String id, String json) {
        if (!json.equals(source.get(id))) {
            parsed.put(id, RuleDef.parse(json));
            source.put(id, json);
        }
        return parsed.get(id);
    }
}
