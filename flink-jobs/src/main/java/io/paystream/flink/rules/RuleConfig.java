package io.paystream.flink.rules;

import java.io.Serializable;
import java.time.Duration;
import org.apache.flink.api.java.utils.ParameterTool;

/** Fraud rule thresholds. Every value can be overridden with a job argument of the same name. */
public class RuleConfig implements Serializable {
    /** Outgoing transfers from one account within {@code velocityWindow} that raise an alert. */
    public int velocityMaxTxns = 6;
    public Duration velocityWindow = Duration.ofSeconds(60);

    /** Distinct P2P senders into one account within {@code muleWindow}. */
    public int muleMinSenders = 8;
    public Duration muleWindow = Duration.ofMinutes(10);

    /** Cash-out at or above this amount from a device the account has not used before. */
    public long newDeviceMinPaisa = 15_000L * 100;
    /** A device stays "new" for this long after first use, so a second cash-out is caught too. */
    public Duration newDeviceWindow = Duration.ofHours(1);

    /** Agent cash-out count in 10 minutes vs the agent's own baseline: alert when its Poisson tail is below this. */
    public double agentSpikeAlpha = 1e-6;
    public int agentSpikeMinCount = 20;
    /** Minutes of history an agent needs before its baseline is trusted. */
    public int agentWarmupMinutes = 20;

    /** After an alert, the same rule stays quiet for the same account for this long. */
    public Duration cooldown = Duration.ofMinutes(10);

    public static RuleConfig from(ParameterTool params) {
        RuleConfig c = new RuleConfig();
        c.velocityMaxTxns = params.getInt("velocityMaxTxns", c.velocityMaxTxns);
        c.velocityWindow = Duration.ofSeconds(params.getLong("velocityWindowSec", c.velocityWindow.toSeconds()));
        c.muleMinSenders = params.getInt("muleMinSenders", c.muleMinSenders);
        c.muleWindow = Duration.ofSeconds(params.getLong("muleWindowSec", c.muleWindow.toSeconds()));
        c.newDeviceMinPaisa = params.getLong("newDeviceMinPaisa", c.newDeviceMinPaisa);
        c.newDeviceWindow = Duration.ofSeconds(params.getLong("newDeviceWindowSec", c.newDeviceWindow.toSeconds()));
        c.agentSpikeAlpha = params.getDouble("agentSpikeAlpha", c.agentSpikeAlpha);
        c.agentSpikeMinCount = params.getInt("agentSpikeMinCount", c.agentSpikeMinCount);
        c.agentWarmupMinutes = params.getInt("agentWarmupMinutes", c.agentWarmupMinutes);
        c.cooldown = Duration.ofSeconds(params.getLong("cooldownSec", c.cooldown.toSeconds()));
        return c;
    }
}
