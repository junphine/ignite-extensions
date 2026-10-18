package de.bwaldvogel.mongo.backend.ignite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.bson.Document;

import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.VectorQueryIndex;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.HybridTextQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.ScoredCacheEntry;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

import de.bwaldvogel.mongo.backend.ignite.util.EmbeddingUtil;

/**
 * knn向量索引：mongo的 {@code {"field": "knnVector"}} 索引。
 *
 * <p>注册阶段把向量字段声明进Ignite类型的 {@link VectorQueryIndex}（索引数据由Ignite的
 * {@code GridLuceneIndex} 以 Hnsw/flat 向量字段维护，与MongoDB共用）；查询阶段通过
 * {@link HybridTextQuery}（文本部分为空时即纯KNN检索）执行。
 *
 * <p>查询语法：
 * <ul>
 *   <li>{@code { $text: { $knnVector: [0.1, ...] } }} → 首个向量字段的KNN检索；</li>
 *   <li>{@code { field: { $knnVector: [0.1, ...] } }} → 指定字段的KNN检索。</li>
 * </ul>
 *
 * <p>首版限制：与文本索引相同，向量字段无法在类型注册后动态追加；后建的向量索引若字段未被注册，
 * 查询退化为全表扫描。混合（文本+向量）检索暂不支持。
 */
public class IgniteKnnVectorIndex extends IgniteH2Index {

	/** 未指定 $k/$limit 时的默认返回条数（与旧实现保持一致）。 */
	private static final int DEFAULT_K = 1200;

	protected int dimensions = 1024;
	protected VectorSimilarityFunction vectorSimilarityFunction = VectorSimilarityFunction.COSINE;
	protected String modelUrl = null;

	public IgniteKnnVectorIndex(GridKernalContext ctx, IgniteBinaryCollection collection, String name,
			List<IndexKey> keys, boolean sparse) {
		super(ctx, collection, name, keys, sparse);
		for (IndexKey ik : keys) {
			if (ik.isText()) {
				initVectorOptions(ik.textOptions());
				break; // 首版以首个向量字段的配置为准
			}
		}
		register();
	}

	/** 解析mongo建索引时的向量选项（numDimensions/dimensions、similarity、modelUrl）。 */
	protected void initVectorOptions(Document options) {
		if (options == null) {
			log.error("KnnVectorIndex must have dimensions and indexType options");
			return;
		}
		Object numDimensions = options.getOrDefault("numDimensions", this.dimensions);
		try {
			this.dimensions = Integer.parseInt(options.getOrDefault("dimensions", numDimensions).toString());
		} catch (NumberFormatException e) {
			log.warn("Invalid vector dimensions '" + numDimensions + "', falling back to " + this.dimensions);
		}
		String similarity = options.getOrDefault("similarity", "cosine").toString();
		if (similarity.equalsIgnoreCase("euclidean")) {
			vectorSimilarityFunction = VectorSimilarityFunction.EUCLIDEAN;
		} else if (similarity.equalsIgnoreCase("cosine")) {
			vectorSimilarityFunction = VectorSimilarityFunction.COSINE;
		} else if (similarity.equalsIgnoreCase("dotProduct")) {
			vectorSimilarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
		} else if (similarity.equalsIgnoreCase("innerProduct")) {
			vectorSimilarityFunction = VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
		} else {
			try {
				vectorSimilarityFunction = VectorSimilarityFunction.valueOf(similarity);
			} catch (IllegalArgumentException e) {
				log.warn("Unknown vector similarity '" + similarity + "', falling back to COSINE");
			}
		}

		String embeddingModelName = (String) options.getOrDefault("modelUrl", "BAAI/bge-m3");
		if ("chinese".equals(embeddingModelName)) {
			embeddingModelName = "BAAI/bge-m3";
		}
		this.modelUrl = resolveModelUrl(embeddingModelName);
	}

	/** http(s)地址直接使用；本地模型优先从IGFS models文件系统加载，否则取IGNITE_HOME/models。 */
	protected String resolveModelUrl(String embeddingModelName) {
		String lowerUrl = embeddingModelName.toLowerCase();
		if (lowerUrl.startsWith("http://") || lowerUrl.startsWith("https://")) {
			return embeddingModelName;
		}
		String igniteHome = ctx.grid().configuration().getIgniteHome();
		try {
			IgniteFileSystem fs = ctx.grid().fileSystem("models");
			if (fs.exists(new IgfsPath(embeddingModelName))) {
				return "s3://models/" + embeddingModelName;
			}
		} catch (IllegalArgumentException ignore) {
			// IGFS未配置，使用本地路径
		}
		return igniteHome + "/models/" + embeddingModelName;
	}

	@Override
	protected int priorityBase() {
		return 100;
	}

	@Override
	protected void doRegister() throws Exception {
		if (findTable() != null) {
			// Ignite不支持在已注册类型上动态追加向量字段（dynamicIndexCreate只支持SORTED）
			log.warn("Ignite type '" + typeName() + "' is already registered. Vector fields " + indexFields()
					+ " cannot be added to its Lucene index dynamically. Queries using mongo index '" + getName()
					+ "' will fall back to full scan.");
			return;
		}
		List<String> fields = indexFields();
		VectorQueryIndex idx = new VectorQueryIndex(fields.get(0), vectorSimilarityFunction, dimensions,
				VectorEncoding.FLOAT32);
		if (fields.size() > 1) {
			idx.setFieldNames(fields, true); // 复合向量索引：每个字段各自独立索引
		}
		QueryEntity entity = new QueryEntity();
		entity.setValueType(typeName());
		entity.setTableName(tableName());
		entity.setFields(buildFields(allKnownFields()));
		entity.getIndexes().add(idx);
		addQueryEntity(entity);
	}

	@Override
	protected boolean verifyRegistration() {
		TableDescriptor tbl = findTable();
		if (tbl == null) {
			return false;
		}
		GridQueryIndexDescriptor textIdx = tbl.type().textIndex();
		if (textIdx == null) {
			return false;
		}
		Collection<String> textFields = textIdx.fields();
		return textFields != null && textFields.containsAll(indexFields());
	}

	@Override
	public boolean canHandle(Document query) {
		if (!isRegistered()) {
			return false;
		}

		// { $text : { $knnVector: [0.1,...] } } || { $text : { $vectorSearch: [0.1,...] } }
		Object text = query.get("$text");
		if (text instanceof Document && IgniteVectorIndex.isVectorSearchExpression((Document) text)) {
			return true;
		}

		if (!CollectionUtils.containsAny(query.keySet(), queryableKeySet())) {
			return false;
		}

		if (isSparse()) { // always sparse
			boolean allIndexFieldsNull = queryableKeySet().stream().allMatch(k -> query.get(k) == null);
			if (allIndexFieldsNull) {
				return false;
			}
		}

		for (String key : queryableKeys()) {
			Object value = query.get(key);
			if (value != null && IgniteVectorIndex.isVectorSearchExpression(value)) {
				return true; // { field: { $knnVector: [0.1,...] } }
			}
		}
		return false;
	}

	@Override
	public Iterable<Object> getPositions(Document query) {
		List<IdWithMeta> results = null;

		// { $text : { $knnVector: [0.1,...], $k: 10, $scoreThreshold: 0.5 } }
		if (query.containsKey("$text")) {
			Object text = query.get("$text");
			if (text instanceof Document && IgniteVectorIndex.isVectorSearchExpression((Document) text)) {
				Document textDoc = (Document) text;
				Object vector = textDoc.get("$knnVector");
				if (vector == null) {
					vector = textDoc.get("$vectorSearch");
				}
				if (vector == null) {
					vector = textDoc.get("$annVector");
				}
				if (vector != null && !vectorFields().isEmpty()) {
					String field = vectorFields().get(0);
					int k = kOf(textDoc, limitOf(textDoc, 0));
					List<IdWithMeta> found = executeVectorQuery(field, vector, k, scoreThresholdOf(textDoc));
					results = results == null ? found : intersect(results, found);
				}
				query.remove("$text"); // 向量部分已被消费（文本部分由文本索引自行处理）
			}
		}

		for (String key : queryableKeys()) {
			Object value = query.get(key);
			if (value == null || !IgniteVectorIndex.isVectorSearchExpression(value)) {
				continue;
			}
			Object vector = value;
			int k = DEFAULT_K;
			float scoreThreshold = 0;
			if (value instanceof Document) {
				Document doc = (Document) value;
				vector = doc.get("$knnVector");
				if (vector == null) {
					vector = doc.get("$vectorSearch");
				}
				if (vector == null) {
					vector = doc.get("$annVector");
				}
				k = kOf(doc, limitOf(doc, 0));
				scoreThreshold = scoreThresholdOf(doc);
			}
			if (vector == null) {
				continue;
			}
			List<IdWithMeta> found = executeVectorQuery(key, vector, k, scoreThreshold);
			query.remove(key);
			results = results == null ? found : intersect(results, found);
		}

		return (Iterable) (results == null ? new ArrayList<IdWithMeta>() : results);
	}

	/** 本索引中可检索的向量字段（仅支持顶层与二层字段，排除{@code _id}）。 */
	protected List<String> vectorFields() {
		return queryableKeys();
	}

	private static int kOf(Map<String, Object> opts, int limit) {
		Object k = opts.get("$k");
		if (k instanceof Number) {
			return ((Number) k).intValue();
		}
		return limit > 0 ? limit : DEFAULT_K;
	}

	/** 执行纯KNN检索（HybridTextQuery文本部分为空时即向量检索）。 */
	protected List<IdWithMeta> executeVectorQuery(String field, Object vectorValue, int k, float scoreThreshold) {
		List<IdWithMeta> result = new ArrayList<>();
		try {
			float[] vector = EmbeddingUtil.computeValueEmbedding(vectorValue, modelUrl, dimensions);
			if (vector == null || vector.length == 0) {
				return result;
			}
			if (k <= 0) {
				k = DEFAULT_K;
			}
			HybridTextQuery<Object, Object> qry = new HybridTextQuery<>(typeName(), "", field, vector, k);
			QueryCursor<javax.cache.Cache.Entry<Object, Object>> cursor = null;
			try {
				cursor = collection.dataMap.query(qry);
				for (javax.cache.Cache.Entry<Object, Object> entry : cursor) {
					float score = entry instanceof ScoredCacheEntry
							? ((ScoredCacheEntry<Object, Object>) entry).score()
							: 0f;
					if (score < scoreThreshold) {
						continue;
					}
					Document meta = new Document("vectorSearchScore", score);
					result.add(new IdWithMeta(entry.getKey(), meta));
				}
			} finally {
				if (cursor != null) {
					cursor.close();
				}
			}
		} catch (Exception e) {
			log.error("Failed to execute vector query on field '" + field + "' of type '" + typeName() + "'", e);
		}
		return result;
	}
}
