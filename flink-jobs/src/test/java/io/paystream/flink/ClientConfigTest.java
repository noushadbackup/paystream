package io.paystream.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;

class ClientConfigTest {
    @Test
    void kafkaSecurityComesFromTheEnvironment() {
        Properties p = ClientConfig.kafka(Map.of(
                "KAFKA_CFG_SECURITY_PROTOCOL", "SASL_SSL",
                "KAFKA_CFG_SASL_MECHANISM", "SCRAM-SHA-512",
                "KAFKA_CFG_SASL_JAAS_CONFIG", "x required username='flink' password='s3cret';",
                "KAFKA_CFG_", "ignored",
                "PATH", "/usr/bin"));
        assertThat(p).containsOnlyKeys("security.protocol", "sasl.mechanism", "sasl.jaas.config");
        assertThat(ClientConfig.kafkaSqlOptions(p))
                .startsWith("'properties.sasl.jaas.config'='x required username=''flink'' password=''s3cret'';', ")
                .contains("'properties.security.protocol'='SASL_SSL', ");
    }

    @Test
    void awsNeedsNoEndpointMinioDoes() {
        assertThat(ClientConfig.lakehouse(ParameterTool.fromArgs(new String[0])))
                .containsEntry("s3.endpoint", "http://minio:9000").containsEntry("s3.path-style-access", "true");
        Map<String, String> aws = ClientConfig.lakehouse(ParameterTool.fromArgs(new String[] {
            "--s3Endpoint", "", "--s3Region", "ap-south-1", "--warehouse", "s3://paystream-lake/"}));
        assertThat(aws).doesNotContainKeys("s3.endpoint", "s3.path-style-access")
                .containsEntry("client.region", "ap-south-1").containsEntry("warehouse", "s3://paystream-lake/");
    }

    @Test
    void hadoopCatalogNeedsOnlyAWarehouse() {
        assertThat(ClientConfig.lakehouse(ParameterTool.fromArgs(new String[] {
            "--catalogType", "hadoop", "--warehouse", "file:///data/warehouse"})))
                .containsExactlyEntriesOf(Map.of("catalog-type", "hadoop", "warehouse", "file:///data/warehouse"));
    }
}
