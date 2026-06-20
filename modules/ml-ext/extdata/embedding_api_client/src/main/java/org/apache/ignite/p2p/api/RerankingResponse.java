package org.apache.ignite.p2p.api;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Reranking 响应封装类
public class RerankingResponse {
    private List<Double> scores;

    private List<Integer> sortedIndexs;

    public List<Integer> getSortedIndexs() {
        if(sortedIndexs==null)
            sortedIndexs = new ArrayList<>(scores.size());

        // 创建索引列表
        List<Integer> indexes = sortedIndexs;
        for (int i = 0; i < scores.size(); i++) {
            indexes.add(i);
        }
        // 根据分数从高到低排序索引
        indexes.sort((i1, i2) -> Double.compare(scores.get(i2), scores.get(i1)));

        return sortedIndexs;
    }

    public RerankingResponse(List<Double> scores) {
        this.scores = scores;
    }

    public List<Double> getScores() { return scores; }
    public void setScores(List<Double> scores) { this.scores = scores; }

    // 获取指定位置的分数
    public Double getScore(int index) {
        if (scores != null && index >= 0 && index < scores.size()) {
            return scores.get(index);
        }
        return null;
    }

    // 获取最高分数
    public double getMaxScore() {
        return scores.stream().max(Double::compareTo).orElse(0.0);
    }

    // 获取最高分数的索引
    public int getMaxScoreIndex() {
        int maxIndex = 0;
        for (int i = 1; i < scores.size(); i++) {
            if (scores.get(i) > scores.get(maxIndex)) {
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    @Override
    public String toString() {
        return "RerankingResponse{scores=" + scores + "}";
    }
}
