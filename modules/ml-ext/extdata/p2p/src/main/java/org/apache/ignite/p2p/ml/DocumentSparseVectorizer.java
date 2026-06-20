/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.p2p.ml;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import org.apache.commons.io.FileUtils;
import org.apache.ignite.ml.dataset.feature.extractor.DocumentVectorizerFactory;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

/**
 * 将文档向量化，元素是Vector，最终生成的是LabelVector<Vector>
 */
public class DocumentSparseVectorizer extends Vectorizer<String, Map<String,Object>, String, SparseVector> {

	private static final long serialVersionUID = 882779686951425867L;

    public static class Factory implements DocumentVectorizerFactory<SparseVector> {
        @Override
        public boolean canHandle(boolean sparse, String modelUrl) {
            return sparse && FileUtils.isDirectory(new File(modelUrl));
        }

        @Override
        public FeatureLabelExtractor<String, Map<String, Object>, SparseVector> create(String modelUrl, int dimensions) {
            return new DocumentSparseVectorizer(modelUrl,dimensions);
        }
        @Override
        public FeatureLabelExtractor<String, Map<String, Object>, SparseVector> create(String modelUrl, int dimensions,String[] fields) {
            return new DocumentSparseVectorizer(modelUrl,dimensions,fields);
        }
    }
	
	final String modelId;
	final int dimensions;

    private List<String> allCoordinates;
	
	public DocumentSparseVectorizer(String modelId, int dimensions) {
		super(new DenseVector(1));
		this.modelId = modelId;
		this.dimensions = dimensions;
	}

    public DocumentSparseVectorizer(String modelId, int dimensions,String[] fields) {
        super(new DenseVector(1),fields);
        this.modelId = modelId;
        this.dimensions = dimensions;
    }
	
    /** {@inheritDoc} */
    @Override protected Serializable feature(String coord, String key, Map<String,Object> value) {
    	String fieldValue = value!=null?(String)value.get(key):key;
    	SparseVector vec = EmbeddingUtil.textTwoGramVec(fieldValue, modelId, dimensions);
        return vec;
    }

    /** {@inheritDoc} */
    @Override protected SparseVector label(String coord, String key, Map<String,Object> value) {
        String fieldValue = value!=null?(String)value.get(key):key;
        SparseVector vec = EmbeddingUtil.textTwoGramVec(fieldValue, modelId,dimensions);
        return vec;
        
    }

    /** {@inheritDoc} */
    @Override protected SparseVector zero() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected List<String> allCoords(String key, Map<String,Object> value) {
        if(allCoordinates==null)
            allCoordinates = new ArrayList<>(value.keySet());
        return allCoordinates;
    }
}
