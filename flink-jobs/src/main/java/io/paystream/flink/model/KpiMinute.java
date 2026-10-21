package io.paystream.flink.model;

/** One-minute, event-time KPIs per transaction type. Fast and approximate: late events are excluded. */
public class KpiMinute {
    public long windowStart;
    public String txnType;
    public long txnCount;
    public long successCount;
    public long failedCount;
    public long successAmountPaisa;

    public KpiMinute() {}

    @Override
    public String toString() {
        return "KpiMinute{" + windowStart + " " + txnType + " n=" + txnCount + " ok=" + successCount + " amt=" + successAmountPaisa + "}";
    }
}
