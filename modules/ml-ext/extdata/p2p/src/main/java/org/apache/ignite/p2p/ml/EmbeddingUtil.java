package org.apache.ignite.p2p.ml;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import me.aias.torch.utils.ParaphraseSentenceEncoder;
import me.aias.torch.utils.SentenceEncoder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;


import ai.djl.MalformedModelException;
import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.inference.Predictor;
import ai.djl.modality.nlp.preprocess.Tokenizer;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.sentencepiece.SpTextEmbedding;
import ai.djl.sentencepiece.SpTokenizer;
import ai.djl.translate.TranslateException;


public class EmbeddingUtil {	
	
	
	private static HashMap<String,Tokenizer> tokenizerCache = new HashMap<>();
	
	private static HashMap<String,Predictor> predictorCache = new HashMap<>(); 
	
	public static Tokenizer tokenizer(Path name) {		
		Tokenizer tokenizer = tokenizerCache.get(name.toString());
		if(tokenizer!=null) {
			return tokenizer;
		}
		
		if(name.toString().endsWith(".model")) { // is sentencepiece model	
			try {				
				tokenizer = new SpTokenizer(name);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		else {
			try {
				tokenizer = HuggingFaceTokenizer.newInstance(name);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		    
		
		tokenizerCache.put(name.toString(), tokenizer);
		return tokenizer;
	}
	
	public static Predictor<String, float[]> predictor(Path name) {
		
		Predictor<String, float[]> predictor = predictorCache.get(name.toString());
		if(predictor!=null) {
			return predictor;
		}

		Path modelPath = name;		

		if(modelPath.toFile().isDirectory()) {
			SentenceEncoder sentenceEncoder = new SentenceEncoder();
			try {		
				
				ZooModel<String, float[]> model = ModelZoo.loadModel(sentenceEncoder.criteria(name));
			            		  
				Predictor<String, float[]> predictorNew = model.newPredictor();
				
				predictor = predictorNew;
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ModelNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedModelException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		    
		
		predictorCache.put(name.toString(), predictor);
		return predictor;
	}

	public static Predictor<String, float[]> sentencepiecePredictor(Path modelPath) {

		Predictor<String, float[]> predictor = predictorCache.get(modelPath.toString());
		if(predictor!=null) {
			return predictor;
		}
		Tokenizer spTokenizer = null;
		Path sentencepieceModelFile = modelPath.resolve("sentencepiece.bpe.model");
		if(sentencepieceModelFile.toFile().exists()) {
			spTokenizer = tokenizer(sentencepieceModelFile);
		}

		if(spTokenizer!=null && spTokenizer instanceof SpTokenizer) {
			SentenceEncoder sentenceEncoder = new SentenceEncoder();
			try {

				SpTextEmbedding processor = SpTextEmbedding.from((SpTokenizer)spTokenizer);

				ZooModel<String, float[]> model = ModelZoo.loadModel(sentenceEncoder.criteria(processor,modelPath.toString()));

				Predictor<String, float[]> predictorNew = model.newPredictor();

				predictor = predictorNew;

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ModelNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedModelException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		predictorCache.put(modelPath.toString(), predictor);
		return predictor;
	}
	
	/**
	 *  remote or local zip model	
	 * @param name
	 * @return
	 */
	public static Predictor<String, float[]> predictor(String name) {
		
		Predictor<String, float[]> predictor = predictorCache.get(name.toString());
		if(predictor!=null) {
			return predictor;
		}
		
		if(name.toString().endsWith(".zip")) {
			ParaphraseSentenceEncoder sentenceEncoder = new ParaphraseSentenceEncoder();
			try {		
				
				ZooModel<String, float[]> model = ModelZoo.loadModel(sentenceEncoder.criteria(name));
			            		  
				Predictor<String, float[]> predictorNew = model.newPredictor();
				
				predictor = predictorNew;
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ModelNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedModelException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		else {
			predictor = null;
		}		    
		
		predictorCache.put(name, predictor);
		return predictor;
	}
	
	public static SparseVector textTwoGramVec(String sentence,String modelId, int dimensions) {
		Path file = Paths.get(modelId);
		HuggingFaceTokenizer tokenizer = (HuggingFaceTokenizer)tokenizer(file);
		// perceive-xlm-large
		int vocbSize = dimensions>0 ? dimensions:65000;
		SparseVector vec = new SparseVector(vocbSize);

		long[] sepsTokens = tokenizer.encode(",.;:\"'?\n，。、？；：“'",false,false).getIds();
		Arrays.sort(sepsTokens);

		Encoding tokens = tokenizer.encode(sentence,false, false);
		int last = 0;
		for(long id: tokens.getIds()) {
			double w = vec.get((int)id);
			double delta = (vocbSize*0.25+id*0.75)/vocbSize/(1+w*4);
			vec.set((int)id, w+delta);
			if(last>0) {
				long hashed = (last*37+id) % vocbSize;
				double w2 = vec.get((int)hashed);
				double delta2 = 0.25/(1+w2*4);
				vec.set((int)(hashed),w2+delta2);
			}	
			// 是标点符号，特殊字符
			if(Arrays.binarySearch(sepsTokens,id)>=0) {
				last = 0;
			}
			else {
				last =(int)id;
			}
		}		
		
		return vec;
		
	}
	
	public static synchronized DenseVector textXlmVec(String sentence,String modelId,int dimensions) {
		Predictor<String,float[]> predictor;
		if(modelId.indexOf("://")>1) {
			predictor = predictor(modelId);
		}
		else {
			Path modelPath = Paths.get(modelId);
			predictor = predictor(modelPath);
		}
		if(predictor==null){
			Path modelPath = Paths.get(modelId);
			predictor = sentencepiecePredictor(modelPath);
		}
		
		try {
			float[] embedding = predictor.predict(sentence);
			DenseVector vec = new DenseVector(embedding);
			return vec;
		} catch (TranslateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}

}
