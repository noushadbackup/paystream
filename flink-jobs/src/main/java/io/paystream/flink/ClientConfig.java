package io.paystream.flink;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Connection settings shared by the jobs, the same on a laptop and in a cluster.
 *
 * <p>Kafka security comes from {@code KAFKA_CFG_*} environment variables, the convention every
 * PayStream service uses: {@code KAFKA_CFG_SECURITY_PROTOCOL=SASL_SSL} becomes
 * {@code security.protocol=SASL_SSL}. Secrets are read from the environment (a Kubernetes secret)
 * rather than job arguments, which the Flink UI and the job graph would show.
 */
public final class ClientConfig {
    static final String PREFIX = "KAFKA_CFG_";

    private ClientConfig() {}

    public static Properties kafka(Map<String, String> env) {
        Properties props = new Properties();
        env.forEach((k, v) -> {
            if (k.startsWith(PREFIX) && k.length() > PREFIX.length()) {
                props.setProperty(k.substring(PREFIX.length()).toLowerCase(Locale.ROOT).replace('_', '.'), v);
            }
        });
        return props;
    }

    public static Properties kafka() {
        return kafka(System.getenv());
    }

    /** The same properties as Flink SQL connector options ({@code 'properties.x'='y'}). */
    public static String kafkaSqlOptions(Properties props) {
        return props.stringPropertyNames().stream().sorted()
                .map(k -> "'properties." + k + "'='" + props.getProperty(k).replace("'", "''") + "', ")
                .collect(Collectors.joining());
    }

    /**
     * Iceberg REST catalog on S3. MinIO in compose needs an endpoint and path-style access; on AWS
     * both are left out and credentials come from the pod's IAM role.
     */
    public static Map<String, String> lakehouse(ParameterTool p) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("catalog-type", "rest");
        m.put("uri", p.get("catalogUri", "http://iceberg-rest:8181"));
        m.put("warehouse", p.get("warehouse", "s3://warehouse/"));
        m.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        String endpoint = p.get("s3Endpoint", "http://minio:9000");
        if (!endpoint.isEmpty()) {
            m.put("s3.endpoint", endpoint);
            m.put("s3.path-style-access", "true");
        }
        m.put("client.region", p.get("s3Region", "us-east-1"));
        return m;
    }
}
