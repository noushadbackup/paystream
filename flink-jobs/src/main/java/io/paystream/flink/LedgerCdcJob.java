package io.paystream.flink;

import io.paystream.flink.sink.IcebergTables;
import java.time.ZoneOffset;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Mirrors the Postgres ledger into the lakehouse from Debezium's change stream.
 *
 * <p>The ledger is the system of record; the gateway stream is what the app said happened. Having
 * both in Iceberg is what makes the daily reconciliation possible. Tables are upserted on their
 * primary key, so an update or a replayed change lands as the latest row, not a second one.
 */
public final class LedgerCdcJob {
    private LedgerCdcJob() {}

    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        String bootstrap = p.get("bootstrap", "kafka:9092");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(p.getLong("checkpointMs", 30_000), CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);

        IcebergTables.createCatalog(tEnv, ClientConfig.lakehouse(p));

        String kafka = " WITH ('connector'='kafka', 'properties.bootstrap.servers'='" + bootstrap + "', "
                + ClientConfig.kafkaSqlOptions(ClientConfig.kafka())
                + " 'properties.group.id'='paystream-ledger-cdc', 'scan.startup.mode'='earliest-offset',"
                + " 'format'='debezium-json', 'debezium-json.timestamp-format.standard'='ISO-8601', 'topic'=";

        tEnv.executeSql("CREATE TEMPORARY TABLE cdc_transfers (txn_id STRING, txn_type STRING, amount_paisa BIGINT,"
                + " event_time TIMESTAMP_LTZ(6), posted_at TIMESTAMP_LTZ(6))" + kafka + "'ledger.ledger.transfers')");
        tEnv.executeSql("CREATE TEMPORARY TABLE cdc_entries (txn_id STRING, leg STRING, account_id STRING,"
                + " amount_paisa BIGINT, posted_at TIMESTAMP_LTZ(6))" + kafka + "'ledger.ledger.entries')");

        String upsert = " WITH ('format-version'='2', 'write.upsert.enabled'='true')";
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS lake.silver.ledger_transfers (txn_id STRING, txn_type STRING,"
                + " amount_paisa BIGINT, event_time TIMESTAMP_LTZ(6), posted_at TIMESTAMP_LTZ(6),"
                + " PRIMARY KEY (txn_id) NOT ENFORCED)" + upsert);
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS lake.silver.ledger_entries (txn_id STRING, leg STRING,"
                + " account_id STRING, amount_paisa BIGINT, posted_at TIMESTAMP_LTZ(6),"
                + " PRIMARY KEY (txn_id, leg) NOT ENFORCED)" + upsert);

        StatementSet set = tEnv.createStatementSet();
        set.addInsertSql("INSERT INTO lake.silver.ledger_transfers SELECT * FROM cdc_transfers");
        set.addInsertSql("INSERT INTO lake.silver.ledger_entries SELECT * FROM cdc_entries");
        set.execute();
    }
}
