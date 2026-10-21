package io.paystream.flink.rules.dynamic;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The stream of rule publications (JSON, one per version) and how it joins the payment stream. */
public final class RuleUpdates {
    public static final OutputTag<String> REJECTED = new OutputTag<>("rejected-rules") {};
    private static final Logger LOG = LoggerFactory.getLogger(RuleUpdates.class);

    private RuleUpdates() {}

    /**
     * Rules carry no event time. Their watermark is pinned to the maximum so that joining them never
     * holds back event time on the payment side: a two-input operator takes the minimum of both.
     */
    public static WatermarkStrategy<String> watermarks() {
        return WatermarkStrategy.forGenerator(ctx -> new WatermarkGenerator<>() {
            @Override
            public void onEvent(String event, long ts, WatermarkOutput out) {}

            @Override
            public void onPeriodicEmit(WatermarkOutput out) {
                out.emitWatermark(new Watermark(Long.MAX_VALUE));
            }
        });
    }

    /**
     * Drops anything that does not parse, so one bad publication cannot fail the job. The rules
     * service validates before publishing; this is the second line of defence.
     */
    public static BroadcastStream<String> broadcast(DataStream<String> updates) {
        SingleOutputStreamOperator<String> valid = updates
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String json, Context ctx, Collector<String> out) {
                        try {
                            RuleDef r = RuleDef.parse(json);
                            LOG.info("rule {} v{} -> {}", r.ruleId, r.version, r.state);
                            out.collect(json);
                        } catch (IllegalArgumentException e) {
                            LOG.warn("rejected rule update: {}", e.getMessage());
                            ctx.output(REJECTED, json);
                        }
                    }
                })
                .name("validate-rules").uid("validate-rules");
        return valid.broadcast(RuleBook.STATE);
    }
}
