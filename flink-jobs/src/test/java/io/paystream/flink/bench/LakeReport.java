package io.paystream.flink.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

/**
 * Reads a benchmark's lakehouse (a Hadoop catalog in a directory) after the job has stopped: row
 * counts for the exactly-once checks, every alert for scoring, and how stale silver was.
 *
 * <pre>
 * --warehouse file:///.../warehouse --work DIR [--freshness]
 * </pre>
 * Writes {@code DIR/lake.json} and {@code DIR/alerts.jsonl}.
 */
public final class LakeReport {
    private LakeReport() {}

    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        String warehouse = p.getRequired("warehouse");
        Path work = Path.of(p.getRequired("work"));
        ObjectMapper json = new ObjectMapper();
        ObjectNode report = json.createObjectNode();
        report.set("counts", counts(warehouse, json));
        if (p.has("freshness")) report.set("silver_freshness", freshness(warehouse, "silver", "payments", json));
        exportAlerts(warehouse, work.resolve("alerts.jsonl"), json, true);
        json.writerWithDefaultPrettyPrinter().writeValue(work.resolve("lake.json").toFile(), report);
        System.out.println(json.writerWithDefaultPrettyPrinter().writeValueAsString(report));
    }

    static TableEnvironment batch(String warehouse) {
        TableEnvironment batch = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        batch.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        batch.executeSql("CREATE CATALOG lake WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
                + warehouse + "')");
        return batch;
    }

    static ObjectNode counts(String warehouse, ObjectMapper json) throws Exception {
        TableEnvironment batch = batch(warehouse);
        Map<String, String> counts = new LinkedHashMap<>();
        counts.put("bronze_rows", "SELECT count(*) FROM lake.bronze.payment_events");
        counts.put("silver_rows", "SELECT count(*) FROM lake.silver.payments");
        counts.put("silver_distinct_txn", "SELECT count(DISTINCT txn_id) FROM lake.silver.payments");
        counts.put("late_rows", "SELECT count(*) FROM lake.silver.late_payments");
        counts.put("alert_rows", "SELECT count(*) FROM lake.silver.fraud_alerts");
        counts.put("kpi_txn_sum", "SELECT sum(txn_count) FROM lake.gold.kpi_minute");
        ObjectNode c = json.createObjectNode();
        for (var e : counts.entrySet()) {
            try (CloseableIterator<Row> it = batch.executeSql(e.getValue()).collect()) {
                Object v = it.next().getField(0);
                c.put(e.getKey(), v == null ? 0 : ((Number) v).longValue());
            }
        }
        return c;
    }

    /**
     * One line per alert row; with {@code latency}, detection latency from the trigger's arrival (its
     * Kafka timestamp). A file replay has no arrival time, so it leaves latency out.
     */
    static void exportAlerts(String warehouse, Path out, ObjectMapper json, boolean latency) throws Exception {
        TableEnvironment batch = batch(warehouse);
        try (BufferedWriter w = Files.newBufferedWriter(out);
             CloseableIterator<Row> it = batch.executeSql("SELECT alert_id, rule, rule_id, account_id, shadow,"
                     + " ingest_time, detected_at FROM lake.silver.fraud_alerts").collect()) {
            while (it.hasNext()) {
                Row r = it.next();
                ObjectNode a = json.createObjectNode();
                a.put("alert_id", (String) r.getField(0));
                a.put("rule", (String) r.getField(1));
                a.put("rule_id", (String) r.getField(2));
                a.put("account_id", (String) r.getField(3));
                a.put("shadow", Boolean.TRUE.equals(r.getField(4)));
                Instant ingest = (Instant) r.getField(5);
                Instant detected = (Instant) r.getField(6);
                if (latency && ingest != null && detected != null) a.put("latency_ms", detected.toEpochMilli() - ingest.toEpochMilli());
                w.write(json.writeValueAsString(a));
                w.newLine();
            }
        }
    }

    /**
     * How long a row waits between arriving in Kafka and being readable in the table, from Iceberg's own
     * metadata: each commit makes its rows visible at its commit time, and the oldest of them arrived at
     * the lower bound of ingest_time in the files it added. Column bounds in the manifests give that
     * without scanning data.
     */
    static ObjectNode freshness(String warehouse, String ns, String name, ObjectMapper json) {
        HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehouse);
        Table table = catalog.loadTable(TableIdentifier.of(ns, name));
        int field = table.schema().findField("ingest_time").fieldId();
        Types.TimestampType type = (Types.TimestampType) table.schema().findType(field);
        List<Long> waits = new ArrayList<>();
        int commits = 0;
        for (Snapshot s : table.snapshots()) {
            commits++;
            long oldest = Long.MAX_VALUE;
            for (DataFile f : s.addedDataFiles(table.io())) {
                ByteBuffer lower = f.lowerBounds() == null ? null : f.lowerBounds().get(field);
                if (lower != null) oldest = Math.min(oldest, (Long) Conversions.fromByteBuffer(type, lower) / 1000);
            }
            if (oldest != Long.MAX_VALUE) waits.add(s.timestampMillis() - oldest);
        }
        ObjectNode o = json.createObjectNode();
        o.put("commits", commits);
        waits.sort(null);
        if (!waits.isEmpty()) {
            o.put("p50_ms", waits.get(waits.size() / 2));
            o.put("p95_ms", waits.get(Math.min(waits.size() - 1, (int) Math.ceil(waits.size() * 0.95) - 1)));
            o.put("max_ms", waits.get(waits.size() - 1));
        }
        o.put("definition", "per commit: commit time minus the earliest ingest_time among the rows it made visible");
        return o;
    }
}
