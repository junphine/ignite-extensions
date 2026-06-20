package org.apache.ignite.ml.dataset.feature.extractor;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

import java.util.List;
import java.util.Map;

public interface DocumentVectorizerFactory<V extends Vector> {

    default boolean canHandle(boolean sparse,String modelUrl){
        return false;
    }

    FeatureLabelExtractor<String, Map<String,Object>, V> create(String modelUrl, int dimensions);

    FeatureLabelExtractor<String, Map<String,Object>, V> create(String modelUrl, int dimensions, String[] fields);
}
