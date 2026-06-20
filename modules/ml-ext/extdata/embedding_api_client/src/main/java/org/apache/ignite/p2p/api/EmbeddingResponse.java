package org.apache.ignite.p2p.api;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


// 响应封装类
public class EmbeddingResponse {
    private List<float[]> dense;   // 可以使用更具体的类型，如 List<List<Double>>
    private List<Map<Integer, Float>> sparse;  // 可以使用更具体的类型，如 List<Map<String, Double>>
    private List<List<float[]>> colbert;    // 可以使用更具体的类型



    // Getters and Setters
    public List<float[]> getDense() {
        return dense;
    }

    public void setDense(List<float[]> dense) {
        this.dense = dense;
    }

    public List<Map<Integer, Float>> getSparse() {
        return sparse;
    }

    public void setSparse(List<Map<Integer, Float>> sparse) {
        this.sparse = sparse;
    }

    public List<List<float[]>> getColbert() {
        return colbert;
    }

    public void setColbert(List<List<float[]>> colbert) {
        this.colbert = colbert;
    }


    @Override
    public String toString() {
        return "EmbeddingResponse{" +
                "dense=" + dense +
                ", sparse=" + sparse +
                ", colbert=" + colbert +
                '}';
    }

}
