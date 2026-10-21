package io.paystream.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A lakehouse created by the previous release gains the v2 alert columns in place, keeping its rows. */
class IcebergMigrationTest {
    @TempDir
    Path warehouse;

    @Test
    void addsAlertColumnsToATableFromThePreviousRelease() throws Exception {
        TableEnvironment old = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        old.executeSql("CREATE CATALOG lake WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
                + warehouse.toUri() + "')");
        old.executeSql("CREATE DATABASE lake.silver");
        old.executeSql("CREATE TABLE lake.silver.fraud_alerts (alert_id STRING, rule STRING, account_id STRING,"
                + " trigger_txn_id STRING, event_time TIMESTAMP_LTZ(3), ingest_time TIMESTAMP_LTZ(3),"
                + " detected_at TIMESTAMP_LTZ(3), score DOUBLE, details STRING, event_date DATE)"
                + " PARTITIONED BY (event_date) WITH ('format-version'='2')");
        old.executeSql("INSERT INTO lake.silver.fraud_alerts VALUES ('VELOCITY:U1:T1', 'VELOCITY', 'U1', 'T1',"
                + " TO_TIMESTAMP_LTZ(0, 3), TO_TIMESTAMP_LTZ(0, 3), TO_TIMESTAMP_LTZ(5, 3), 1.2, 'x', DATE '1970-01-01')")
                .await();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        IcebergTables.createCatalog(tEnv, Map.of("catalog-type", "hadoop", "warehouse", warehouse.toUri().toString()));

        TableEnvironment after = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        after.executeSql("CREATE CATALOG lake WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
                + warehouse.toUri() + "')");
        try (CloseableIterator<Row> it = after.executeSql(
                "SELECT alert_id, rule_id, rule_version, shadow, action FROM lake.silver.fraud_alerts").collect()) {
            assertThat(it.next().toString()).isEqualTo("+I[VELOCITY:U1:T1, null, null, null, null]");
        }
    }
}
