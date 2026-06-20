package org.apache.ignite.p2p.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// 请求参数封装类
public class EmbeddingRequest {
    private List<String> corpus;
    private int batchSize = 128;      // 默认值
    private int maxLength = 1024;     // 默认值
    private boolean dense = true;     // 默认值
    private boolean sparse = true;    // 默认值
    private boolean colbert = false; // 默认值

    // 构造函数
    public EmbeddingRequest() {
        this.corpus = new ArrayList<>();
    }

    public EmbeddingRequest(List<String> corpus) {
        this.corpus = corpus;
    }

    // Builder 模式（可选，提供更友好的构建方式）
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<String> corpus;
        private int batchSize = 128;
        private int maxLength = 1024;
        private boolean dense = true;
        private boolean sparse = true;
        private boolean colbert = false;

        public Builder corpus(List<String> corpus) {
            this.corpus = corpus;
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

        public Builder dense(boolean dense) {
            this.dense = dense;
            return this;
        }

        public Builder sparse(boolean sparse) {
            this.sparse = sparse;
            return this;
        }

        public Builder colbert(boolean colbert) {
            this.colbert = colbert;
            return this;
        }

        public EmbeddingRequest build() {
            EmbeddingRequest request = new EmbeddingRequest();
            request.corpus = this.corpus;
            request.batchSize = this.batchSize;
            request.maxLength = this.maxLength;
            request.dense = this.dense;
            request.sparse = this.sparse;
            request.colbert = this.colbert;
            return request;
        }
    }

    // Getters and Setters
    public List<String> getCorpus() {
        return corpus;
    }

    public void setCorpus(List<String> corpus) {
        this.corpus = corpus;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    public boolean isDense() {
        return dense;
    }

    public void setDense(boolean dense) {
        this.dense = dense;
    }

    public boolean isSparse() {
        return sparse;
    }

    public void setSparse(boolean sparse) {
        this.sparse = sparse;
    }

    public boolean isColbert() {
        return colbert;
    }

    public void setColbert(boolean colbert) {
        this.colbert = colbert;
    }

    // 转换为 Map，用于 JSON 序列化
    public Map<String, Object> toMap() {
        return Map.of(
                "corpus", corpus,
                "batch_size", batchSize,
                "max_length", maxLength,
                "dense", dense,
                "sparse", sparse,
                "colbert", colbert
        );
    }
}
