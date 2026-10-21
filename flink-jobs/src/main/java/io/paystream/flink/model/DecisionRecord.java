package io.paystream.flink.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/** One decision of the decision service, as logged to Kafka, flattened for the lakehouse. */
public class DecisionRecord {
    private static final ObjectMapper JSON = new ObjectMapper();

    public String decisionId;
    public String txnId;
    public long decidedAt;
    public long eventTime;
    public String outcome;
    public String reasons;
    public double riskScore;
    public String policyVersion;
    public boolean degraded;
    public long latencyUs;
    public String txnType;
    public String senderId;
    public String receiverId;
    public String deviceId;
    public long amountPaisa;
    /** The exact features the decision saw, as JSON: training data without training-serving skew. */
    public String features;
    public String flags;
    /** Champion model that scored the payment (null without one), and the challenger scored alongside. */
    public String modelVersion;
    public Double modelScore;
    public String challengerVersion;
    public Double challengerScore;

    public DecisionRecord() {}

    public static DecisionRecord parse(byte[] json) throws IOException {
        JsonNode n = JSON.readTree(json);
        JsonNode req = n.path("request");
        DecisionRecord d = new DecisionRecord();
        d.decisionId = n.path("decision_id").asText();
        d.txnId = n.path("txn_id").asText();
        d.decidedAt = n.path("decided_at").asLong();
        d.eventTime = n.path("event_time").asLong();
        d.outcome = n.path("outcome").asText();
        d.reasons = join(n.path("reasons"));
        d.riskScore = n.path("risk_score").asDouble();
        d.policyVersion = n.path("policy_version").asText();
        d.degraded = n.path("degraded").asBoolean();
        d.latencyUs = n.path("latency_us").asLong();
        d.txnType = req.path("txn_type").asText();
        d.senderId = req.path("sender_id").asText();
        d.receiverId = req.path("receiver_id").asText();
        d.deviceId = req.path("device_id").asText(null);
        d.amountPaisa = req.path("amount_paisa").asLong();
        d.features = n.path("features").isNull() ? null : n.path("features").toString();
        d.flags = join(n.path("flags"));
        d.modelVersion = n.path("model").path("version").asText(null);
        d.modelScore = n.path("model").has("score") ? n.path("model").path("score").asDouble() : null;
        d.challengerVersion = n.path("challenger").path("version").asText(null);
        d.challengerScore = n.path("challenger").has("score") ? n.path("challenger").path("score").asDouble() : null;
        return d;
    }

    private static String join(JsonNode arr) {
        StringBuilder b = new StringBuilder();
        for (JsonNode x : arr) b.append(b.length() == 0 ? "" : ",").append(x.asText());
        return b.toString();
    }
}
