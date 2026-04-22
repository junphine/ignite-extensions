package de.bwaldvogel.mongo.backend.ignite.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.apache.ignite.ml.math.primitives.vector.storage.DenseDoubleVectorStorage;
import org.apache.ignite.ml.structures.LabeledVector;


public class EmbeddingUtil {
	
	private static HashMap<String,Method> embeddingCache = new HashMap<>();
	
	public static SparseVector textTwoGramVec(String sentence,String modelId,int dimensions) {

		Method vectorizer = embeddingCache.get(modelId);
		
		try {
			if(vectorizer==null) {
				//SentencesSparseVectorizer sentencesVectorizer = new SentencesSparseVectorizer(modelId);
				//vectorizer = sentencesVectorizer;
				
				Class cls = Class.forName("org.apache.ignite.p2p.ml.EmbeddingUtil");
				vectorizer = cls.getMethod("textTwoGramVec",String.class,String.class, int.class);
				embeddingCache.put(modelId, vectorizer);
			}
			
			SparseVector labelVec = (SparseVector)vectorizer.invoke(null,sentence,modelId,dimensions);
			
			return labelVec;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static DenseVector textXlmVec(String sentence,String modelId,int dimensions) {

		Method vectorizer = embeddingCache.get(modelId);
		
		try {
			if(vectorizer==null) {
				//SentencesVectorizer sentencesVectorizer = new SentencesVectorizer(modelId);
				//vectorizer = sentencesVectorizer;

				Class cls = Class.forName("org.apache.ignite.p2p.ml.EmbeddingUtil");
				vectorizer = cls.getMethod("textXlmVec",String.class,String.class,int.class);
				embeddingCache.put(modelId, vectorizer);
			}

			DenseVector labelVec = (DenseVector)vectorizer.invoke(null,sentence,modelId,dimensions);

			return labelVec;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DenseVector v =  new DenseVector(new DenseDoubleVectorStorage(dimensions));
		return (DenseVector)v.plus(1.0d);
		//return null;
	}
}
