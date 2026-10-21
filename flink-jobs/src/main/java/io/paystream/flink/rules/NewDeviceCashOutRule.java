package io.paystream.flink.rules;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Payment;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.util.Collector;

/**
 * Keyed by sender: a large cash-out from a device the account has not used before (account takeover
 * after SIM swap or OTP phishing). Accounts with no device history are learned silently first, so a
 * cold start does not flag every account.
 */
public class NewDeviceCashOutRule extends FraudRule {
    private transient MapState<String, Long> devices;

    public NewDeviceCashOutRule(RuleConfig cfg) {
        super("NEW_DEVICE_CASH_OUT", cfg);
    }

    @Override
    public void open(OpenContext ctx) throws Exception {
        super.open(ctx);
        var desc = new MapStateDescriptor<>("deviceFirstSeen", String.class, Long.class);
        desc.enableTimeToLive(StateTtlConfig.newBuilder(java.time.Duration.ofDays(90)).build());
        devices = getRuntimeContext().getMapState(desc);
    }

    @Override
    protected void evaluate(Payment p, Context ctx, Collector<Alert> out) throws Exception {
        Long firstSeen = devices.get(p.deviceId);
        boolean hasOlderDevice = false;
        for (var e : devices.entries()) {
            if (!e.getKey().equals(p.deviceId) && e.getValue() < p.eventTime - cfg.newDeviceWindow.toMillis()) {
                hasOlderDevice = true;
                break;
            }
        }
        boolean deviceIsNew = firstSeen == null || p.eventTime - firstSeen < cfg.newDeviceWindow.toMillis();

        if ("CASH_OUT".equals(p.txnType) && p.amountPaisa >= cfg.newDeviceMinPaisa && deviceIsNew && hasOlderDevice) {
            alert(p, (double) p.amountPaisa / cfg.newDeviceMinPaisa,
                    "cash-out of " + p.amountPaisa / 100 + " BDT from new device " + p.deviceId, out);
        }
        if (firstSeen == null || p.eventTime < firstSeen) devices.put(p.deviceId, p.eventTime);
    }
}
