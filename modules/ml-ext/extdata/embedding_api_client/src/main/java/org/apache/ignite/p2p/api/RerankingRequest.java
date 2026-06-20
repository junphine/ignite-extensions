package org.apache.ignite.p2p.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
// Reranking 请求参数封装类
public class RerankingRequest {
    private String query;
    private List<String> passages;
    private int batchSize = 128;
    private int maxLength = 1024;
    private boolean normalize = true;
    private List<Float> weightsForDifferentModes;

    public RerankingRequest() {
        this.passages = new ArrayList<>();
    }

    public RerankingRequest(String query, List<String> passages) {
        this.query = query;
        this.passages = passages;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String query;
        private List<String> passages;
        private int batchSize = 128;
        private int maxLength = 1024;
        private boolean normalize = true;
        private List<Float> weightsForDifferentModes;

        public Builder query(String query) {
            this.query = query;
            return this;
        }

        public Builder passages(List<String> passages) {
            this.passages = passages;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder maxLength(int maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        public Builder normalize(boolean normalize) {
            this.normalize = normalize;
            return this;
        }

        public Builder weightsForDifferentModes(float dense,float sparse,float colbert){
            this.weightsForDifferentModes = List.of(dense,sparse,colbert);
            return this;
        }

        public RerankingRequest build() {
            RerankingRequest request = new RerankingRequest();
            request.query = this.query;
            request.passages = this.passages;
            request.batchSize = this.batchSize;
            request.maxLength = this.maxLength;
            request.normalize = this.normalize;
            request.weightsForDifferentModes = this.weightsForDifferentModes;
            return request;
        }
    }

    // Getters and Setters
    public String getQuery() { return query; }
    public void setQuery(String query) { this.query = query; }
    public List<String> getPassages() { return passages; }
    public void setPassages(List<String> passages) { this.passages = passages; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    public int getMaxLength() { return maxLength; }
    public void setMaxLength(int maxLength) { this.maxLength = maxLength; }
    public boolean isNormalize() { return normalize; }
    public void setNormalize(boolean normalize) { this.normalize = normalize; }

    public Map<String, Object> toMap() {
        return Map.of(
                "query", query,
                "passages", passages,
                "batch_size", batchSize,
                "max_length", maxLength,
                "normalize", normalize
        );
    }
}