package io.paystream.flink.pipeline;

import io.paystream.flink.model.KpiMinute;
import io.paystream.flink.model.Payment;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** Incremental per-minute aggregation: the window keeps one small accumulator, not every event. */
public class KpiAggregate implements AggregateFunction<Payment, KpiMinute, KpiMinute> {
    @Override
    public KpiMinute createAccumulator() {
        return new KpiMinute();
    }

    @Override
    public KpiMinute add(Payment p, KpiMinute acc) {
        acc.txnType = p.txnType;
        acc.txnCount++;
        if (p.isSuccess()) {
            acc.successCount++;
            acc.successAmountPaisa += p.amountPaisa;
        } else {
            acc.failedCount++;
        }
        return acc;
    }

    @Override
    public KpiMinute getResult(KpiMinute acc) {
        return acc;
    }

    @Override
    public KpiMinute merge(KpiMinute a, KpiMinute b) {
        a.txnCount += b.txnCount;
        a.successCount += b.successCount;
        a.failedCount += b.failedCount;
        a.successAmountPaisa += b.successAmountPaisa;
        if (a.txnType == null) a.txnType = b.txnType;
        return a;
    }

    /** Stamps the window start onto the aggregate. */
    public static class StampWindow extends ProcessWindowFunction<KpiMinute, KpiMinute, String, TimeWindow> {
        @Override
        public void process(String txnType, Context ctx, Iterable<KpiMinute> in, Collector<KpiMinute> out) {
            KpiMinute k = in.iterator().next();
            k.windowStart = ctx.window().getStart();
            k.txnType = txnType;
            out.collect(k);
        }
    }
}
