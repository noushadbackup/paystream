package io.paystream.flink;

import io.paystream.flink.model.Alert;
import io.paystream.flink.model.Envelope;
import io.paystream.flink.pipeline.PaymentsPipeline;
import io.paystream.flink.rules.RuleConfig;
import io.paystream.flink.serde.AlertSerializer;
import io.paystream.flink.serde.DeadLetterSerializer;
import io.paystream.flink.serde.HttpSchemaRegistry;
import io.paystream.flink.serde.PaymentEventDeserializer;
import io.paystream.flink.sink.IcebergTables;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Entry point: Kafka in, Iceberg and Kafka out.
 *
 * <p>Delivery guarantees differ by output, on purpose:
 * <ul>
 *   <li>Iceberg tables (money): exactly-once, committed on checkpoint.</li>
 *   <li>Fraud alerts: at-least-once with deterministic alert ids. A transactional Kafka sink would hold
 *       every alert until the next checkpoint (10 s), which breaks the 2 s alert target; consumers
 *       dedupe on alert_id instead.</li>
 * </ul>
 */
public final class PaymentsJob {
    private PaymentsJob() {}

    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        String bootstrap = p.get("bootstrap", "kafka:9092");
        HttpSchemaRegistry registry = new HttpSchemaRegistry(p.get("registry", "http://schema-registry:8081"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(p);
        env.enableCheckpointing(p.getLong("checkpointMs", 10_000), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cc = env.getCheckpointConfig();
        cc.setMinPauseBetweenCheckpoints(p.getLong("minPauseMs", 2_000));
        cc.setCheckpointTimeout(Duration.ofMinutes(5).toMillis());
        cc.setTolerableCheckpointFailureNumber(3);
        cc.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        KafkaSource<Envelope> source = KafkaSource.<Envelope>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(p.get("topic", "payments.events"))
                .setGroupId(p.get("groupId", "paystream-flink"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new PaymentEventDeserializer(registry))
                .setProperty("partition.discovery.interval.ms", "60000")
                .build();

        DataStream<Envelope> envelopes = env
                .fromSource(source, PaymentsPipeline.watermarks(), "kafka-payments")
                .uid("kafka-payments");

        PaymentsPipeline.Outputs out = PaymentsPipeline.build(envelopes, RuleConfig.from(p),
                Duration.ofMinutes(p.getLong("dedupTtlMin", 120)));

        out.alerts().sinkTo(KafkaSink.<Alert>builder()
                        .setBootstrapServers(bootstrap)
                        .setRecordSerializer(new AlertSerializer(registry, p.get("alertsTopic", "fraud.alerts")))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build())
                .name("kafka-alerts").uid("kafka-alerts");

        out.dlq().sinkTo(KafkaSink.<Envelope>builder()
                        .setBootstrapServers(bootstrap)
                        .setRecordSerializer(new DeadLetterSerializer(p.get("dlqTopic", "payments.dlq")))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build())
                .name("kafka-dlq").uid("kafka-dlq");

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        IcebergTables.createCatalog(tEnv, Map.of(
                "catalog-type", "rest",
                "uri", p.get("catalogUri", "http://iceberg-rest:8181"),
                "warehouse", p.get("warehouse", "s3://warehouse/"),
                "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                "s3.endpoint", p.get("s3Endpoint", "http://minio:9000"),
                "s3.path-style-access", "true",
                "client.region", "us-east-1"));
        StreamStatementSet set = tEnv.createStatementSet();
        IcebergTables.insertAll(tEnv, set, out);
        set.attachAsDataStream();

        env.execute("paystream-payments");
    }
}
