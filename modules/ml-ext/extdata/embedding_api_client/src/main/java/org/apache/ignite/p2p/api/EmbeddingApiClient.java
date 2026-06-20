package org.apache.ignite.p2p.api;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.DeserializationFeature;

public class EmbeddingApiClient {

    private final HttpClient httpClient;
    private final static ObjectMapper objectMapper = new ObjectMapper();

    static{
        objectMapper.configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true);
    }
    private final String baseUrl;

    public EmbeddingApiClient(String baseUrl) {
        this.httpClient = HttpClient.newHttpClient();
        this.baseUrl = baseUrl;
    }

    // 使用 EmbeddingRequest 对象调用接口
    public EmbeddingResponse getPassageEmbeddings(EmbeddingRequest request) throws IOException,InterruptedException {
        String jsonBody = objectMapper.writeValueAsString(request.toMap());
        byte[] bodyBytes = jsonBody.getBytes(StandardCharsets.UTF_8);

        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/v1/embedding/passages/"))
                .header("Content-Type", "application/json; charset=UTF-8")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("HTTP error: " + response.statusCode() + ", body: " + response.body());
        }

        EmbeddingResponse root = objectMapper.readValue(response.body(),EmbeddingResponse.class);

        return root;
    }

    // 调用 Reranking 接口
    public RerankingResponse computeRerankingScores(RerankingRequest request) throws IOException,InterruptedException  {
        String jsonBody = objectMapper.writeValueAsString(request.toMap());

        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/v1/reranking/compute"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody,StandardCharsets.UTF_8))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("HTTP error: " + response.statusCode() + ", body: " + response.body());
        }

        JsonNode root = objectMapper.readTree(response.body());

        List<Double> scores = new ArrayList<>();
        if (root.has("scores") && root.get("scores").isArray()) {
            for (JsonNode scoreNode : root.get("scores")) {
                scores.add(scoreNode.asDouble());
            }
        }

        return new RerankingResponse(scores);
    }

    // 简便方法：直接传入 query 和 passages
    public RerankingResponse computeRerankingScores(String query, List<String> passages) throws IOException,InterruptedException {
        RerankingRequest request = RerankingRequest.builder()
                .query(query)
                .passages(passages)
                .build();
        return computeRerankingScores(request);
    }


    // 示例调用
    public static void main(String[] args) throws Exception {
        EmbeddingApiClient client = new EmbeddingApiClient("http://127.0.0.1:7777");

        List<String> corpus = List.of(
                "今天狂风暴雨",
                "明天股市大跌"
        );

        // 方式1：使用 Builder 模式
        EmbeddingRequest request1 = EmbeddingRequest.builder()
                .corpus(corpus)
                .batchSize(128)
                .maxLength(1024)
                .dense(true)
                .sparse(true)
                .colbert(true)
                .build();

        EmbeddingResponse response = client.getPassageEmbeddings(request1);

        System.out.println(response);
    }
}