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

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;

/**
 * 将文本列表向量化，元素是Vector，最终生成的是LabelVector<Vector>
 */
public class SentencesSparseVectorizer extends Vectorizer<Object, List<String>, Integer, SparseVector> {

	private static final long serialVersionUID = 882779686951425867L;
	
	final String modelId;
	
	public SentencesSparseVectorizer(String modelId) {
		super(new DenseVector(1));
		this.modelId = modelId;
	}
	
    /** {@inheritDoc} */
    @Override protected Serializable feature(Integer coord, Object key, List<String> value) {
    	StringBuilder sb = new StringBuilder();
    	for(int i=0;i<value.size();i++) {
    		if(labelCoord != null && i==labelCoord) {
    			continue;
    		}
    		sb.append(value.get(i));
    		sb.append("\n");
    	}
    	SparseVector vec = EmbeddingUtil.textTwoGramVec(sb.toString(), modelId);
        return vec;
    }

    /** {@inheritDoc} */
    @Override protected SparseVector label(Integer coord, Object key, List<String> value) {
        String text = value.get(coord);
        SparseVector vec = EmbeddingUtil.textTwoGramVec(text, modelId);
        return vec;
        
    }

    /** {@inheritDoc} */
    @Override protected SparseVector zero() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected List<Integer> allCoords(Object key, List<String> value) {
    	if(labelCoord != null) {
    		if(labelCoord!=0)
    			return List.of(0,labelCoord);
    		return List.of(labelCoord,1);
    	}
    	if(lbCoordinateShortcut != null) {
   		 	return List.of(0,1);
    	}
        return List.of(0);
    }
}
