package io.paystream.flink.model;

/** A fraud rule hit. {@code alertId} is deterministic so a replay after failure produces the same id. */
public class Alert {
    public String alertId;
    public String rule;
    public String accountId;
    public String triggerTxnId;
    public long eventTime;
    public long ingestTime;
    public long detectedAt;
    public double score;
    public String details;
    /** Set for analyst-defined rules (rule = DYNAMIC); null for the built-in detectors. */
    public String ruleId;
    public Integer ruleVersion;
    /** A shadow rule is evaluated and recorded but must not trigger any action: how a new rule is trialled. */
    public boolean shadow;
    /** REVIEW or DECLINE: what the decision service should do with the account. */
    public String action = "REVIEW";

    public Alert() {}

    public static Alert of(String rule, String accountId, Payment trigger, double score, String details) {
        Alert a = new Alert();
        a.alertId = rule + ":" + accountId + ":" + trigger.txnId;
        a.rule = rule;
        a.accountId = accountId;
        a.triggerTxnId = trigger.txnId;
        a.eventTime = trigger.eventTime;
        a.ingestTime = trigger.ingestTime;
        a.detectedAt = System.currentTimeMillis();
        a.score = score;
        a.details = details;
        return a;
    }

    @Override
    public String toString() {
        return "Alert{" + alertId + (shadow ? " shadow" : "") + " score=" + score + " " + details + "}";
    }
}
