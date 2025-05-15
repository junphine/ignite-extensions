package de.bwaldvogel.mongo.backend.ignite.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.apache.ignite.ml.structures.LabeledVector;


public class EmbeddingUtil {
	
	private static HashMap<String,Vectorizer> embeddingCache = new HashMap<>();	
	
	public static SparseVector textTwoGramVec(Object id, List<String> sentence,String modelId) {	
		
		Vectorizer<Object, List<String>, Integer, SparseVector> vectorizer = embeddingCache.get(modelId);
		
		try {
			if(vectorizer==null) {
				//SentencesSparseVectorizer sentencesVectorizer = new SentencesSparseVectorizer(modelId);
				//vectorizer = sentencesVectorizer;
				
				Class cls = Class.forName("org.apache.ignite.p2p.ml.SentencesSparseVectorizer");
				Constructor<Vectorizer> cs = cls.getDeclaredConstructor(String.class);
				vectorizer = cs.newInstance(modelId);
				embeddingCache.put(modelId, vectorizer);
			}
			
			SparseVector labelVec = vectorizer.extractFeatures(id, sentence).getRaw(0);			
			
			return labelVec;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static Vector textXlmVec(Object id, List<String> sentence,String modelId) {
		
		Vectorizer<Object, List<String>, Integer, Vector> vectorizer = embeddingCache.get(modelId);
		
		try {
			if(vectorizer==null) {
				//SentencesVectorizer sentencesVectorizer = new SentencesVectorizer(modelId);
				//vectorizer = sentencesVectorizer;
				
				Class cls = Class.forName("org.apache.ignite.p2p.ml.SentencesVectorizer");
				Constructor<Vectorizer> cs = cls.getDeclaredConstructor(String.class);				
				vectorizer = cs.newInstance(modelId);
				embeddingCache.put(modelId, vectorizer);
			}
			
			Vector labelVec = vectorizer.extractFeatures(id, sentence).getRaw(0);			
			
			return labelVec;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}

}
