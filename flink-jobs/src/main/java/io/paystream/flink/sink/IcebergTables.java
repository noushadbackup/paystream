package io.paystream.flink.sink;

import io.paystream.flink.pipeline.PaymentsPipeline;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
                + " score DOUBLE, details STRING, event_date DATE) PARTITIONED BY (event_date)" + v2);
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
        set.addInsertSql("INSERT INTO lake.silver.fraud_alerts SELECT alertId, rule, accountId, triggerTxnId,"
                + " TO_TIMESTAMP_LTZ(eventTime, 3), TO_TIMESTAMP_LTZ(ingestTime, 3), TO_TIMESTAMP_LTZ(detectedAt, 3),"
                + " score, details, " + EVENT_DATE + " FROM v_alerts");
        set.addInsertSql("INSERT INTO lake.gold.kpi_minute SELECT TO_TIMESTAMP_LTZ(windowStart, 3), txnType, txnCount,"
                + " successCount, failedCount, successAmountPaisa, CAST(TO_TIMESTAMP_LTZ(windowStart, 3) AS DATE) FROM v_kpis");
    }
}
