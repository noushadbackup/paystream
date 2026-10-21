package io.paystream.flink.sink;

import io.paystream.flink.model.DecisionRecord;
import io.paystream.flink.model.Label;
import io.paystream.flink.pipeline.PaymentsPipeline;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Lakehouse sinks. Tables are declared in SQL so the layout is readable in one place.
 *
 * <p>Exactly-once: the Iceberg sink commits one snapshot per Flink checkpoint, and a snapshot is
 * either fully visible or not at all. After a failure, Flink rewinds Kafka offsets to the last
 * checkpoint and the uncommitted files are never referenced, so no row is lost or written twice.
 */
public final class IcebergTables {
    public static final String CATALOG = "lake";

    private IcebergTables() {}

    public static void createCatalog(StreamTableEnvironment tEnv, Map<String, String> props) {
        String with = props.entrySet().stream()
                .map(e -> "'" + e.getKey() + "'='" + e.getValue() + "'")
                .collect(Collectors.joining(", "));
        tEnv.executeSql("CREATE CATALOG " + CATALOG + " WITH ('type'='iceberg', " + with + ")");
        for (String db : new String[] {"bronze", "silver", "gold"}) {
            tEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + CATALOG + "." + db);
        }
        String v2 = " WITH ('format-version'='2', 'write.parquet.compression-codec'='zstd',"
                + " 'write.target-file-size-bytes'='134217728')";

        tEnv.executeSql("CREATE TABLE IF NOT EXISTS lake.bronze.payment_events ("
                + PAYMENT_COLUMNS + ", kafka_partition INT, kafka_offset BIGINT, event_date DATE"
                + ") PARTITIONED BY (event_date)" + v2);
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS lake.silver.payments ("
                + PAYMENT_COLUMNS + ", event_date DATE) PARTITIONED BY (event_date)" + v2);
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS lake.silver.late_payments ("
                + PAYMENT_COLUMNS + ", event_date DATE) PARTITIONED BY (event_date)" + v2);
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS lake.silver.fraud_alerts ("
                + "alert_id STRING, rule STRING, account_id STRING, trigger_txn_id STRING,"
                + " event_time TIMESTAMP_LTZ(3), ingest_time TIMESTAMP_LTZ(3), detected_at TIMESTAMP_LTZ(3),"
                + " score DOUBLE, details STRING, rule_id STRING, rule_version INT, shadow BOOLEAN, action STRING,"
                + " event_date DATE) PARTITIONED BY (event_date)" + v2);
        addMissingColumns(tEnv, "silver", "fraud_alerts", ALERT_COLUMNS_V2);
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS lake.silver.decisions ("
                + "decision_id STRING, txn_id STRING, decided_at TIMESTAMP_LTZ(3), event_time TIMESTAMP_LTZ(3),"
                + " outcome STRING, reasons STRING, risk_score DOUBLE, policy_version STRING, degraded BOOLEAN,"
                + " latency_us BIGINT, txn_type STRING, sender_id STRING, receiver_id STRING, device_id STRING,"
                + " amount_paisa BIGINT, features STRING, flags STRING, decision_date DATE"
                + ") PARTITIONED BY (decision_date)" + v2);
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS lake.silver.fraud_labels ("
                + "txn_id STRING, scenario STRING, account_id STRING, labelled_at TIMESTAMP_LTZ(3), label_date DATE"
                + ") PARTITIONED BY (label_date)" + v2);
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS lake.gold.kpi_minute ("
                + "window_start TIMESTAMP_LTZ(3), txn_type STRING, txn_count BIGINT, success_count BIGINT,"
                + " failed_count BIGINT, success_amount_paisa BIGINT, event_date DATE)"
                + " PARTITIONED BY (event_date)" + v2);
    }

    private static final String PAYMENT_COLUMNS = "txn_id STRING, event_time TIMESTAMP_LTZ(3), txn_type STRING,"
            + " sender_id STRING, receiver_id STRING, amount_paisa BIGINT, currency STRING, device_id STRING,"
            + " district STRING, status STRING, channel STRING, ingest_time TIMESTAMP_LTZ(3)";

    private static final String PAYMENT_SELECT = "txnId, TO_TIMESTAMP_LTZ(eventTime, 3), txnType, senderId,"
            + " receiverId, amountPaisa, currency, deviceId, district, status, channel, TO_TIMESTAMP_LTZ(ingestTime, 3)";

    private static final String EVENT_DATE = "CAST(TO_TIMESTAMP_LTZ(eventTime, 3) AS DATE)";

    /** Adds one INSERT per table to the statement set; call {@code attachAsDataStream()} after. */
    public static void insertAll(StreamTableEnvironment tEnv, StreamStatementSet set, PaymentsPipeline.Outputs out) {
        tEnv.createTemporaryView("v_bronze", out.bronze());
        tEnv.createTemporaryView("v_silver", out.silver());
        tEnv.createTemporaryView("v_late", out.late());
        tEnv.createTemporaryView("v_alerts", out.alerts());
        tEnv.createTemporaryView("v_kpis", out.kpis());

        set.addInsertSql("INSERT INTO lake.bronze.payment_events SELECT " + PAYMENT_SELECT
                + ", kafkaPartition, kafkaOffset, " + EVENT_DATE + " FROM v_bronze");
        set.addInsertSql("INSERT INTO lake.silver.payments SELECT " + PAYMENT_SELECT + ", " + EVENT_DATE + " FROM v_silver");
        set.addInsertSql("INSERT INTO lake.silver.late_payments SELECT " + PAYMENT_SELECT + ", " + EVENT_DATE + " FROM v_late");
        set.addInsertSql("INSERT INTO lake.silver.fraud_alerts (alert_id, rule, account_id, trigger_txn_id,"
                + " event_time, ingest_time, detected_at, score, details, rule_id, rule_version, shadow, action, event_date)"
                + " SELECT alertId, rule, accountId, triggerTxnId,"
                + " TO_TIMESTAMP_LTZ(eventTime, 3), TO_TIMESTAMP_LTZ(ingestTime, 3), TO_TIMESTAMP_LTZ(detectedAt, 3),"
                + " score, details, ruleId, ruleVersion, shadow, action, " + EVENT_DATE + " FROM v_alerts");
        set.addInsertSql("INSERT INTO lake.gold.kpi_minute SELECT TO_TIMESTAMP_LTZ(windowStart, 3), txnType, txnCount,"
                + " successCount, failedCount, successAmountPaisa, CAST(TO_TIMESTAMP_LTZ(windowStart, 3) AS DATE) FROM v_kpis");
    }

    /** Labels go in through the same statement set, so they commit on the same checkpoints. */
    public static void insertLabels(StreamTableEnvironment tEnv, StreamStatementSet set, DataStream<Label> labels) {
        tEnv.createTemporaryView("v_labels", labels);
        set.addInsertSql("INSERT INTO lake.silver.fraud_labels SELECT txnId, scenario, accountId,"
                + " TO_TIMESTAMP_LTZ(labelledAt, 3), CAST(TO_TIMESTAMP_LTZ(labelledAt, 3) AS DATE) FROM v_labels");
    }

    public static void insertDecisions(StreamTableEnvironment tEnv, StreamStatementSet set,
                                       DataStream<DecisionRecord> decisions) {
        tEnv.createTemporaryView("v_decisions", decisions);
        set.addInsertSql("INSERT INTO lake.silver.decisions SELECT decisionId, txnId, TO_TIMESTAMP_LTZ(decidedAt, 3),"
                + " TO_TIMESTAMP_LTZ(eventTime, 3), outcome, reasons, riskScore, policyVersion, degraded, latencyUs,"
                + " txnType, senderId, receiverId, deviceId, amountPaisa, features, flags,"
                + " CAST(TO_TIMESTAMP_LTZ(decidedAt, 3) AS DATE) FROM v_decisions");
    }

    /** Columns added to silver.fraud_alerts in v2 (analyst rules), for tables created before them. */
    private static final Map<String, Type> ALERT_COLUMNS_V2 = new LinkedHashMap<>();

    static {
        ALERT_COLUMNS_V2.put("rule_id", Types.StringType.get());
        ALERT_COLUMNS_V2.put("rule_version", Types.IntegerType.get());
        ALERT_COLUMNS_V2.put("shadow", Types.BooleanType.get());
        ALERT_COLUMNS_V2.put("action", Types.StringType.get());
    }

    /**
     * Additive schema migration. Iceberg tracks columns by id, so adding optional columns is a
     * metadata-only commit: existing data files are not rewritten and old rows read the new columns as null.
     */
    static void addMissingColumns(StreamTableEnvironment tEnv, String db, String table, Map<String, Type> columns) {
        Catalog catalog = ((FlinkCatalog) tEnv.getCatalog(CATALOG).orElseThrow()).catalog();
        Table t = catalog.loadTable(TableIdentifier.of(db, table));
        UpdateSchema update = t.updateSchema();
        boolean changed = false;
        for (Map.Entry<String, Type> c : columns.entrySet()) {
            if (t.schema().findField(c.getKey()) == null) {
                update.addColumn(c.getKey(), c.getValue());
                changed = true;
            }
        }
        if (changed) update.commit();
    }
}
