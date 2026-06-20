package de.bwaldvogel.mongo.backend.ignite.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.ServiceLoader;

import de.bwaldvogel.mongo.bson.Document;
import org.apache.ignite.ml.dataset.feature.extractor.DocumentVectorizerFactory;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.apache.ignite.ml.math.primitives.vector.storage.DenseDoubleVectorStorage;
import org.apache.ignite.ml.structures.LabeledVector;


public class EmbeddingUtil {
	
	private static HashMap<String, DocumentVectorizerFactory> embeddingCache = new HashMap<>();
	
	public static SparseVector textSparseVec(String key,Document sentence, String modelId, int dimensions) {

		DocumentVectorizerFactory vectorizer = embeddingCache.get(modelId);

		try {
			if(vectorizer==null) {
				//vectorizer = sentencesVectorizer;
				ServiceLoader<DocumentVectorizerFactory> serviceLoader = ServiceLoader.load(DocumentVectorizerFactory.class);

				// 遍历并执行
				for (DocumentVectorizerFactory service : serviceLoader) {
					if(service.canHandle(true,modelId)){
						embeddingCache.put(modelId,service);
						vectorizer = service;
						break;
					}
				}
			}
			if(vectorizer!=null) {
				if(sentence==null) { // for query text
					SparseVector labelVec = (SparseVector) vectorizer.create(modelId, dimensions).extractLabel(key, sentence);

					return labelVec;
				}

				Vector docVec = (Vector) vectorizer.create(modelId, dimensions, key.split(",")).extractFeatures(key, sentence);

				return docVec.getRaw(0);

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static DenseVector textXlmVec(String key, Document sentence,String modelId,int dimensions) {

		DocumentVectorizerFactory<?> vectorizer = embeddingCache.get(modelId);
		
		try {
			if(vectorizer==null) {
				//vectorizer = sentencesVectorizer;
				ServiceLoader<DocumentVectorizerFactory> serviceLoader = ServiceLoader.load(DocumentVectorizerFactory.class);

				// 遍历并执行
				for (DocumentVectorizerFactory service : serviceLoader) {
					if(service.canHandle(false,modelId)){
						embeddingCache.put(modelId,service);
						vectorizer = service;
						break;
					}
				}
			}
			if(vectorizer!=null) {
				if(sentence==null){ // for query text
					DenseVector labelVec = (DenseVector) vectorizer.create(modelId, dimensions).extractLabel(key, sentence);
					return labelVec;
				}
				// for colpus doc
				Vector docVec = (Vector) vectorizer.create(modelId, dimensions, key.split(",")).extractFeatures(key, sentence);

				return docVec.getRaw(0);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}



	public static float[] computeValueEmbedding(Object val,String modelUrl,int dimensions) {

		float[] vec = null;
		if(val instanceof float[]) {
			float[] fdata = (float[])val;
			vec = fdata;
		}
		else if(val instanceof double[]) {
			double[] fdata = (double[])val;
			float[] data = new float[fdata.length];
			for(int i=0;i<fdata.length;i++) {
				data[i] = (float)fdata[i];
			}
			vec = data;
		}
		else if(val instanceof Number[]) {
			Number[] fdata = (Number[])val;
			float[] data = new float[fdata.length];
			for(int i=0;i<fdata.length;i++) {
				data[i] = fdata[i].floatValue();
			}
			vec = data;
		}
		else if(val instanceof List) {
			List<Number> fdata = (List<Number>)val;
			float[] data = new float[fdata.size()];
			for(int i=0;i<fdata.size();i++) {
				data[i] = fdata.get(i).floatValue();
			}
			vec = data;
		}
		else if(val instanceof CharSequence) {
			Vector fdata = EmbeddingUtil.textXlmVec(val.toString(),null,modelUrl,dimensions);
			if(fdata!=null) {
				float[] data = new float[fdata.size()];
				for(int i=0;i<fdata.size();i++) {
					data[i] = (float)fdata.get(i);
				}
				vec = data;
			}
		}
		return vec;
	}

	public static byte[] computeInt8ValueEmbedding(Object val,String modelUrl,int dimensions) {

		byte[] vec = null;
		if(val instanceof byte[]) {
			byte[] fdata = (byte[])val;
			vec = fdata;
		}
		else if(val instanceof float[]) {
			float[] fdata = (float[])val;
			byte[] data = new byte[fdata.length];
			for(int i=0;i<fdata.length;i++) {
				data[i] = (byte)fdata[i];
			}
			vec = data;
		}
		else if(val instanceof double[]) {
			double[] fdata = (double[])val;
			byte[] data = new byte[fdata.length];
			for(int i=0;i<fdata.length;i++) {
				data[i] = (byte)fdata[i];
			}
			vec = data;
		}
		else if(val instanceof Number[]) {
			Number[] fdata = (Number[])val;
			byte[] data = new byte[fdata.length];
			for(int i=0;i<fdata.length;i++) {
				data[i] = fdata[i].byteValue();
			}
			vec = data;
		}
		else if(val instanceof List) {
			List<Number> fdata = (List<Number>)val;
			byte[] data = new byte[fdata.size()];
			for(int i=0;i<fdata.size();i++) {
				data[i] = fdata.get(i).byteValue();
			}
			vec = data;
		}
		else if(val instanceof CharSequence) {
			Vector fdata = EmbeddingUtil.textXlmVec(val.toString(),null,modelUrl,dimensions);
			if(fdata!=null) {
				byte[] data = new byte[fdata.size()];
				for(int i=0;i<fdata.size();i++) {
					data[i] = (byte)fdata.get(i);
				}
				vec = data;
			}
		}
		return vec;
	}

}
