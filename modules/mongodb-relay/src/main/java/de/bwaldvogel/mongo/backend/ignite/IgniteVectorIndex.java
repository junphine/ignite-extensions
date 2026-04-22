package de.bwaldvogel.mongo.backend.ignite;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneIndex;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDataset;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.knn.KNNPartitionDataBuilder;
import org.apache.ignite.ml.knn.ann.ANNClassificationModel;
import org.apache.ignite.ml.knn.ann.ANNClassificationTrainer;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.utils.PointWithDistance;
import org.apache.ignite.ml.knn.utils.PointWithDistanceUtil;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndex;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndexType;
import org.apache.ignite.ml.math.distances.CosineSimilarity;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.DotProductSimilarity;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.backend.QueryOperator;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.backend.ignite.util.DocumentUtil;
import de.bwaldvogel.mongo.backend.ignite.util.EmbeddingUtil;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.KeyConstraintError;


public class IgniteVectorIndex extends Index<Object> {
	
	/** Learning environment builder. */
    protected LearningEnvironmentBuilder envBuilder = LearningEnvironmentBuilder.defaultBuilder();

	private final String cacheName;	
	
	private IgniteCache<Object, Vector> vecIndex;	
	
	private final GridKernalContext ctx;	
	
	/** knn */
    private KNNClassificationModel<Object> knnModel;
    /** ann */
    private ANNClassificationModel<Object> annModel;

    /** Distance measure. */
    protected DistanceMeasure distanceMeasure;
    
    private String indexType = SpatialIndexType.BALL_TREE.name();
	
	private String idField = "_id";
	
	private int K = GridLuceneIndex.DEAULT_LIMIT;
	
	private int dimensions = 1024;
	
	private boolean defaultANN = false;
	
	private String tokenizerUrl = "tokenizers/chinese-xlnet-large";
	
	private String modelUrl = null;

	private Set<Object> updatedDocs = new TreeSet<>();

	private long updatedTime = System.currentTimeMillis();
	
	static class EmbeddingIntCoordObjectLabelVectorizer implements FeatureLabelExtractor<Object,Vector,Object>{
		
		private static final long serialVersionUID = 1L;

		@Override
		public LabeledVector<Object> apply(Object k, Vector v) {			
			return extract(k, v);
		}

		@Override
		public LabeledVector<Object> extract(Object k, Vector v) {
			LabeledVector<Object> f = new  LabeledVector<>(v,k);
			return f;
		}		
		
	}
	

	public IgniteVectorIndex(GridKernalContext ctx, IgniteBinaryCollection collection, String name, List<IndexKey> keys, boolean sparse) {
		super(name, keys, sparse);
		this.ctx = ctx;
		this.cacheName = collection.getCollectionName();
		this.idField = collection.idField;
		this.distanceMeasure = new CosineSimilarity();
		
		if(sparse) {
			indexType = SpatialIndexType.ARRAY.name();
		}

		String embeddingModelName = "text2vec-base-chinese-paraphrase";
		Document options = keys.get(0).textOptions();
		if(options!=null) {
			
			this.dimensions = Integer.parseInt(options.getOrDefault("dimensions", dimensions).toString());
			
			String similarity = options.getOrDefault("similarity", "cosine").toString();
			if(similarity.equals("euclidean ")) {
				this.distanceMeasure = new EuclideanDistance();
			}
			else if(similarity.equals("cosine")) {
				this.distanceMeasure = new CosineSimilarity();
			}
			else if(similarity.equals("dotProduct")) {
				this.distanceMeasure = new DotProductSimilarity();
			}
			else if(!similarity.isBlank()) {
				try {
					this.distanceMeasure = DistanceMeasure.of(similarity);
				} catch (InstantiationException e) {
					e.printStackTrace();
					throw new IgniteException("KnnVectorIndex similarity not supported");
				}
			}			
			
			String indexType = options.getOrDefault("indexType", "").toString().toUpperCase();
			if(!indexType.isBlank()) {
				if(indexType.startsWith("ANN")) {
					defaultANN =  true;
				}
				else {
					this.indexType = indexType;
				}
			}
			
			// 句向量模型
			embeddingModelName = (String)options.getOrDefault("modelUrl", "chinese");
			if(embeddingModelName.equals("chinese")) {
				embeddingModelName = "text2vec-base-chinese-paraphrase";
			}
			else if(embeddingModelName.equals("multilingual")) {
				embeddingModelName = "paraphrase-xlm-r-multilingual";
			}	
			// IDF词典模型
			tokenizerUrl= (String)options.getOrDefault("tokenizerUrl", tokenizerUrl);
		}
		
		String igniteHome = ctx.grid().configuration().getIgniteHome();

		// is openai api
		String lowerUrl = embeddingModelName.toLowerCase();
		if(lowerUrl.startsWith("http://") || lowerUrl.startsWith("https://")){
			modelUrl = embeddingModelName;
		}
		else {  // is local model

			try {
				IgniteFileSystem fs = ctx.grid().fileSystem("models");
				if (fs.exists(new IgfsPath(embeddingModelName))) {
					modelUrl = "s3://models/" + (embeddingModelName);
				} else {
					modelUrl = igniteHome + "/models/" + (embeddingModelName);
				}
			} catch (IllegalArgumentException e) {
				modelUrl = igniteHome + "/models/" + (embeddingModelName);
			}
		}
		
		CacheConfiguration<Object, Vector> cfg = new CacheConfiguration<>();        	
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName(IgniteDatabase.getIndexCacheName(collection.getDatabaseName(),this.cacheName,this.getName()));
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC); 
        cfg.setBackups(0);
        
        vecIndex = ctx.grid().getOrCreateCache(cfg);
        
        init(collection);
	}
	

	public void init(IgniteBinaryCollection collection) {
				
	}
	
	public KNNClassificationModel<Object> knnModel(){
		if(this.knnModel!=null){
			if(updatedDocs.size()>1000 || updatedDocs.size()>0 && System.currentTimeMillis()-updatedTime>1000*60*60){
				knnModel.close();
				knnModel = null;
			}
		}
		if(this.knnModel==null) {
			synchronized(this){
				if(this.knnModel==null) {

					/** Index type. */
				    SpatialIndexType idxType = SpatialIndexType.BALL_TREE;
				    try {
						idxType = SpatialIndexType.valueOf(indexType.toUpperCase());
				    }
				    catch(Exception e) {
				    	
				    }
					LearningEnvironment environment = envBuilder.buildForTrainer();
					EmbeddingIntCoordObjectLabelVectorizer vectorizer = new EmbeddingIntCoordObjectLabelVectorizer();
					CacheBasedDatasetBuilder<Object, Vector> datasetBuilder = new CacheBasedDatasetBuilder<>(ctx.grid(), vecIndex);
					
					CacheBasedDataset<Object, Vector, EmptyContext, SpatialIndex<Object>> knnDataset = datasetBuilder.build(
				            envBuilder,
				            new EmptyContextBuilder<>(),
				            new KNNPartitionDataBuilder<>(vectorizer, idxType, distanceMeasure),
				            environment
				        );
					this.knnModel = new KNNClassificationModel<Object>(knnDataset,distanceMeasure,this.K, false);

					this.updatedDocs.clear();
					updatedTime = System.currentTimeMillis();
				}
			}			
		}
		return knnModel;
	}
	
	public ANNClassificationModel<Object> annModel() {
		if(this.annModel!=null){
			if(updatedDocs.size()>1000 || updatedDocs.size()>0 && System.currentTimeMillis()-updatedTime>1000*60*60){
				annModel.close();
				annModel = null;
			}
		}
		if(this.annModel==null) {
			synchronized(this){
				if(this.annModel==null) {

					EmbeddingIntCoordObjectLabelVectorizer vectorizer = new EmbeddingIntCoordObjectLabelVectorizer();
					CacheBasedDatasetBuilder<Object, Vector> datasetBuilder = new CacheBasedDatasetBuilder<>(ctx.grid(), vecIndex);
					int K = 2+(int)Math.sqrt(dimensions*2);
					ANNClassificationTrainer<Object> annTrainer = new ANNClassificationTrainer<Object>()
							.withEnvironmentBuilder(envBuilder)
							.withMaxIterations(2+K/5)
							.withDistance(distanceMeasure)
							.withK(K);
					
					annModel = annTrainer.update(annModel, datasetBuilder, vectorizer);
					annModel.withWeighted(true);
					annModel.withDistanceMeasure(distanceMeasure);

					updatedDocs.clear();
					updatedTime = System.currentTimeMillis();
				}
			}		
		}
		return annModel;
	}
	
	public Vector computeEmbedding(Document document,Object position) {
		Vector vec = null;
		List<String> sentences = new ArrayList<>(); // maybe is resource text path

		for(String field : keys()) {
			Object val = document.get(field);
			if(val instanceof CharSequence) {
				sentences.add(val.toString());
			}
			else if(val!=null){
				vec = computeValueEmbedding(val);
				if(vec!=null) {
					return vec;
				}
			}			
		}
		
		if(!sentences.isEmpty()) {
			if(this.isSparse()) {
				vec = EmbeddingUtil.textTwoGramVec(String.join("\n",sentences),modelUrl,dimensions);
			}
			else {			
				vec = EmbeddingUtil.textXlmVec(String.join("\n",sentences),modelUrl,dimensions);
			}
		}
		return vec;
	}	

	public Vector computeValueEmbedding(Object val) {
		Vector vec = null;
		if(val instanceof float[]) {
			float[] fdata = (float[])val;			
			vec = new DenseVector(fdata);
		}
		else if(val instanceof double[]) {
			vec = new DenseVector((double[])val);
		}
		else if(val instanceof Number[]) {
			Number[] fdata = (Number[])val;
			float[] data = new float[fdata.length];
			for(int i=0;i<fdata.length;i++) {
				data[i] = fdata[i].floatValue();
			}
			vec = new DenseVector(data);
		}
		else if(val instanceof List) {
			List<Number> fdata = (List<Number>)val;
			float[] data = new float[fdata.size()];
			for(int i=0;i<fdata.size();i++) {
				data[i] = fdata.get(i).floatValue();
			}
			vec = new DenseVector(data);
		}
		else if(val instanceof CharSequence) {
			if(this.isSparse()) {
				vec = EmbeddingUtil.textTwoGramVec(val.toString(),this.tokenizerUrl,dimensions);
			}
			else {			
				vec = EmbeddingUtil.textXlmVec(val.toString(),this.modelUrl,dimensions);
			}
		}
		return vec;
	}
	
	  /** {@inheritDoc} */
    public List<LabeledVector<Object>> findKClosest(int k, Vector pnt, boolean ann) {
    	if(ann) {
			// no vector index
    		List<LabeledVector<Object>> list = this.annModel().findKClosestLabels(k, pnt, this.vecIndex);
    		return list;
    	}
    	
    	Collection<PointWithDistance<Object>> res = this.knnModel().findKClosest(k, pnt);

        return res == null ? Collections.emptyList() : PointWithDistanceUtil.transformToListOrdered(res);
    }
    
	@Override
	public Object getPosition(Document document) {		
		Object key = document.get(idField);
		if (key != null) {
			return DocumentUtil.toBinaryKey(key);
		}
		return null;
	}

	@Override
	public void checkAdd(Document document, MongoCollection<Object> collection) {
	}


	@Override
	public void add(Document document, Object position, MongoCollection<Object> collection) {
		
		// build doc body
		try {
			Vector vec = this.computeEmbedding(document,position);
			if(vec==null) {
				vecIndex.removeAsync(position);
				return ;
			}
			
			vecIndex.putAsync(position, vec);
			updatedDocs.add(position);

		} catch (Exception e) {			
			e.printStackTrace();
		}
	}

	@Override
	public Object remove(Document document, MongoCollection<Object> collection) {		
		Object key = getPosition(document);
		try {
			vecIndex.removeAsync(key);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return key;
	}

	@Override
	public boolean canHandle(Document query) {
		if (query.containsKey("$text")) {
			if(query.getDocument("$text").containsKey("$knnVector")) {
				return true;
			}
			if(query.getDocument("$text").containsKey("$vectorSearch")) {
				return true;
			}
		}
		
		if (!CollectionUtils.containsAny(query.keySet(), keySet())) {
			return false;
		}

		if (isSparse() && query.values().stream().allMatch(Objects::isNull)) {
			return false;
		}

		for (String key : keys()) {
			Object queryValue = query.get(key);
			if (queryValue instanceof Document) {				
				Document queryDoc = (Document) queryValue;
				for (String queriedKey : queryDoc.keySet()) {
					if (isInQuery(queriedKey)) {
						// okay
						return true;
					} 
					else if (queriedKey.equals("$vectorSearch")) {
						return true;
					}
					else if (queriedKey.equals("$knnVector") || queriedKey.equals("$annVector")) {
						return true;
					}
					else if (queriedKey.startsWith("$")) {
						// continue check
					}
				}
			}
		}
		return false;
	}

	@Override
	public Iterable<Object> getPositions(Document query) {
		final KeyValue queriedKeys = getQueriedKeys(query);
		KeyValue searchKey = queriedKeys;		
		int n = 0;
		List<Object> all = new ArrayList<>(20);

		Document textDoc = null;
		if (query.containsKey("$text")) {
			textDoc = query.getDocument("$text");
			if(isVectorSearchExpression(textDoc)) {
				query.remove("$text");
			}
			else{
				textDoc = null;
			}
		}
		
		for (Object queriedKey : queriedKeys.iterator()) {
			IndexKey indexKey = this.getKeys().get(n);

			// for $text value  { $text : { $vectorSearch: 'keyword' } }
			if (textDoc!=null) {
				List<IdWithMeta> positions = getVectorTextList(indexKey,textDoc);
				return (List)positions;
			}

			// for { embedding : { $search: 'keyword' } } || { embedding : { $knnVector: [0.1,0.4,0.6] } }
			if (queriedKey instanceof Document) {				
				Document keyObj = (Document) queriedKey;
				if (Utils.containsQueryExpression(keyObj)) {
					Set<String> expression = keyObj.keySet();
					
					if (expression.contains(QueryOperator.VECTOR_SEARCH.getValue())) {
						searchKey = searchKey.copyFrom(n, keyObj);						
						query.remove(indexKey.getKey());
					}
					else if (expression.contains(QueryOperator.KNN_VECTOR.getValue())) {						
						searchKey = searchKey.copyFrom(n, keyObj);
						query.remove(indexKey.getKey());
					}
					else if (expression.contains(QueryOperator.ANN_VECTOR.getValue())) {						
						searchKey = searchKey.copyFrom(n, keyObj);
						query.remove(indexKey.getKey());
					}
					else if (expression.contains(QueryOperator.IN.getValue())) {
						List<IdWithMeta> positions = getPositionsForInExpression(indexKey,keyObj.get(QueryOperator.IN.getValue()), QueryOperator.IN.getValue());
						query.remove(indexKey.getKey());						
						all.addAll(positions);
						searchKey = searchKey.copyFrom(n, null);
					}
				}
			}
			n++;
		}
		for (IndexKey idxKey : this.getKeys()) {
			Object obj = searchKey.get(n);
			if(obj != null) {
				List<IdWithMeta> positions = getVectorTextList(idxKey,obj);
				if (positions != null) {
					all.addAll(positions);
				}
			}
		}
		return all;
	}

	@Override
	public long getCount() {		
		return vecIndex.sizeLong(CachePeekMode.ALL);
	}

	@Override
	public long getDataSize() {		
		return vecIndex.localMetrics().getCacheSize();
	}

	@Override
	public void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<Object> collection) {

	}

	@Override
	public void updateInPlace(Document oldDocument, Document newDocument, Object position,
			MongoCollection<Object> collection) throws KeyConstraintError {
		if (!nullAwareEqualsKeys(oldDocument, newDocument)) {
			Object removedPosition = remove(oldDocument, collection);
			if (removedPosition != null) {
				Assert.equals(removedPosition, position);
			}
			add(newDocument, position, collection);
		}
	}

	@Override
	public void drop() {		
		close();
		this.vecIndex.destroy();		
	}
	
	void close() {
		try {
			if (this.knnModel != null) {
				this.knnModel.close();
				this.knnModel = null;
			}			
			if (this.annModel != null) {
				this.annModel.close();
				this.annModel = null;
			}
			
		} catch (Exception e) {			
			e.printStackTrace();
		}
	}

	private List<IdWithMeta> getPositionsForInExpression(IndexKey indexKey, Object value, String operator) {
		if (isInQuery(operator)) {
			@SuppressWarnings("unchecked")
			Collection<Object> objects = (Collection<Object>) value;
			Collection<Object> queriedObjects = new TreeSet<>(ValueComparator.asc());
			queriedObjects.addAll(objects);

			List<IdWithMeta> allKeys = new ArrayList<>();
			for (Object object : queriedObjects) {
				
				List<IdWithMeta> keys = getVectorTextList(indexKey,object);
				if (keys != null) {
					allKeys.addAll(keys);
				} else {
					return null;
				}

			}
			return allKeys;
		}
		else {
			throw new UnsupportedOperationException("unsupported query expression: " + operator);
		}
	}

	/**
	 * 对字符串字段进行向量搜索查询，支持模糊匹配
	 * 
	 * @param indexKey
	 * @param exp maybe string or document
	 * @return
	 */
	protected List<IdWithMeta> getVectorTextList(IndexKey indexKey, Object exp) {
		List<IdWithMeta> result = new ArrayList<>();
		try {
			int limit = 0;			
			Object obj = exp;
			float scoreThreshold = 0.0f;
			boolean useAnn = defaultANN;
			if (exp instanceof Map) {
				Map<String, Object> opt = (Map) obj;
				
				if(opt.containsKey("$knnVector")) {
					obj = opt.get("$knnVector");
					useAnn = false;
				}
				else if(opt.containsKey("$annVector")) {
					obj = opt.get("$annVector");
					useAnn = true;
				}
				else if(opt.containsKey("$vectorSearch")) {
					obj = opt.get("$vectorSearch");
				}
				
				if(opt.containsKey("$limit")) {
					limit = Integer.parseInt(opt.get("$limit").toString());
				}
				
				if(opt.containsKey("$scoreThreshold")) {
					scoreThreshold = Float.parseFloat(opt.get("$scoreThreshold").toString());
				}
				
				if(opt.containsKey("$indexType")) {					
					String indexType = opt.get("$indexType").toString().toUpperCase();
					if(!indexType.equals(this.indexType)) {
						this.close();
						this.indexType = indexType;
					}
				}
			}

			Vector vec = this.computeValueEmbedding(obj);
			
			int maxResults = limit;
			if(limit<=0) {
				maxResults = K;
			}
			List<LabeledVector<Object>> docs = this.findKClosest(maxResults, vec, useAnn);
			limit = docs.size();
			result = new ArrayList<>(limit);
			for (int i = 0; i < limit; i++) {
				LabeledVector<Object> doc = docs.get(i);
				float score = convertDistanceToSimilarity(doc.weight());
				if(score>=scoreThreshold) { // 越大越好
					Object k = doc.label();
					Vector v = doc.features();
					
					result.add(new IdWithMeta(k,new Document("vectorSearchScore",score)));
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;

	}	
	
	public boolean isTextIndex() {
		return true;
	}
	
	private static boolean isInQuery(String key) {
		return key.equals(QueryOperator.IN.getValue());
	}
	
    public int getPriority() {
    	return Math.min(9, 1 + (int)getDataSize()/10000);
    }    
    
    public float convertDistanceToSimilarity(float distance) {
    	if(distanceMeasure.getClass()==CosineSimilarity.class) {
    		// 将余弦距离[0,2] 转换为 [-1, 1] 范围内的相似度
    		return (1 - distance);
    	}
    	// 使用高斯核函数将欧氏距离转换为相似度
    	double sigma = 1;
        double sim = Math.exp(-Math.pow(distance, 2) / (2 * Math.pow(sigma, 2)));
        return (float)sim;
    }

	public static boolean isVectorSearchExpression(Object object) {
		if (object instanceof Document) {
			Document textDoc = (Document) object;
			return textDoc.containsKey("$vectorSearch") || textDoc.containsKey("$knnVector") || textDoc.containsKey("$annVector");
		} else {
			return object instanceof float[] || object instanceof double[];
		}
	}
}
