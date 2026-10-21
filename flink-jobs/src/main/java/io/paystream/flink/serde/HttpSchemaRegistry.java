package io.paystream.flink.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;

/** Schema Registry REST client with a per-id cache: each schema id is fetched once per task. */
public class HttpSchemaRegistry implements SchemaRegistry {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String baseUrl;
    private transient HttpClient http;
    private transient Map<Integer, Schema> cache;

    public HttpSchemaRegistry(String baseUrl) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
    }

    @Override
    public Schema byId(int id) {
        return cache().computeIfAbsent(id, i -> {
            JsonNode body = call(HttpRequest.newBuilder(URI.create(baseUrl + "/schemas/ids/" + i)).GET());
            return new Schema.Parser().parse(body.get("schema").asText());
        });
    }

    @Override
    public int register(String subject, Schema schema) {
        String payload = JsonNodeFactory.instance.objectNode().put("schema", schema.toString()).toString();
        JsonNode body = call(HttpRequest.newBuilder(URI.create(baseUrl + "/subjects/" + subject + "/versions"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(payload)));
        int id = body.get("id").asInt();
        cache().put(id, schema);
        return id;
    }

    private JsonNode call(HttpRequest.Builder request) {
        try {
            HttpResponse<String> resp = client().send(request.timeout(Duration.ofSeconds(10)).build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("schema registry " + resp.statusCode() + ": " + resp.body());
            }
            return JSON.readTree(resp.body());
        } catch (IOException e) {
            throw new IllegalStateException("schema registry unreachable at " + baseUrl, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private HttpClient client() {
        if (http == null) http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        return http;
    }

    private Map<Integer, Schema> cache() {
        if (cache == null) cache = new ConcurrentHashMap<>();
        return cache;
    }
}
