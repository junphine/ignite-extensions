package de.bwaldvogel.mongo.backend.ignite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.ScoredCacheEntry;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;

/**
 * 文本索引：mongo的 {@code {"field": "text"}} 索引。
 *
 * <p>注册阶段把字段声明进Ignite类型的 {@code FullTextQueryIndex}（由Ignite的 {@code GridLuceneIndex}
 * 维护索引数据，写入路径与MongoDB共用）；查询阶段通过 {@link TextQuery} 走
 * {@code IgniteH2Indexing.queryLocalText(...)} 检索。
 *
 * <p>查询语法（由 {@code GridLuceneIndex} 的StandardQueryParser解析，形如 {@code field:(text)}）：
 * <ul>
 *   <li>{@code { $text: { $search: 'keyword' } }} → 所有本索引字段的OR检索；</li>
 *   <li>{@code { field: { $text: 'keyword' } }} → 单字段检索；</li>
 *   <li>{@code { field: 'keyword' } } → 单字段检索（与旧版SimpleQueryParser语义近似）；</li>
 * </ul>
 *
 * <p>初始化参数（命名对齐MongoDB Search索引配置，取自mongo建索引时传入的 {@code textOptions}）：
 * <ul>
 *   <li>{@code analyzer} —— 索引期分析器；</li>
 *   <li>{@code searchAnalyzer}（或 {@code queryAnalyzer}/{@code defaultSearchAnalyzer}）—— 检索期分析器；</li>
 * </ul>
 * 取值为 {@code $IGNITE_HOME/config/lucene.xml} 中注册的Spring bean名，由Ignite的
 * {@code LuceneIndexAccess} 解析；自定义分词器/tokenFilter等也统一由该配置提供。
 * 后建的文本索引追加到已有类型时，若类型上已存在FULLTEXT配置则共享既有配置、忽略本索引的分析器参数。
 *
 * <p>首版限制：
 * <ul>
 *   <li>{@code $regex} 不使用索引，退化为 {@code matchDocuments} 全表扫描过滤；</li>
 *   <li>{@code $sort} 不生效（Ignite的TextQuery仅支持double docValues字段排序）。</li>
 * </ul>
 */
public class IgniteTextIndex extends IgniteH2Index {

	/** 索引期分析器（对应MongoDB Search的 {@code analyzer}，lucene.xml bean名）。 */
	protected String analyzer;

	/** 检索期分析器（对应MongoDB Search的 {@code searchAnalyzer}，lucene.xml bean名）。 */
	protected String queryAnalyzer;

	protected Boolean storedSource;

	protected Boolean dynamic;

	public IgniteTextIndex(GridKernalContext ctx, IgniteBinaryCollection collection, String name, List<IndexKey> keys,
			boolean sparse) {
		super(ctx, collection, name, keys, sparse);
		initTextOptions();
	}

	@Override
	protected int priorityBase() {
		return 100;
	}

	@Override
	protected void doRegister() throws Exception {
		TableDescriptor tbl = findTable();
		if (tbl == null) {
			QueryEntity entity = new QueryEntity();
			entity.setValueType(typeName());
			entity.setTableName(tableName());
			entity.setFields(buildFields(allKnownFields()));
			List<QueryIndex> indexs = new ArrayList<>(entity.getIndexes());
			indexs.add(buildFullTextQueryIndex());
			entity.setIndexes(indexs);
			addQueryEntity(entity);
			// 针对持久化缓存：registerCache0的重建走树索引分支，不经过Lucene store，这里统一回填
			backfillLuceneIndex();
			return;
		}
		// 类型已注册：WARN后把文本字段动态追加进类型已有的Lucene文本索引
		log.warn("Ignite type '" + typeName() + "' is already registered. Appending text fields " + keys()
				+ " of mongo index '" + getName() + "' into its Lucene text index.");
		addColumns(keys());
		QueryIndexDescriptorImpl desc = appendTextIndexFields(tbl);
		if (desc.getQueryIndex() == null) {
			desc.setQueryIndex(buildFullTextQueryIndex());
		} else {
			log.info("Ignite type '" + typeName() + "' already has FULLTEXT configuration "
					+ desc.getQueryIndex().getClass().getSimpleName() + "; analyzer options of mongo index '"
					+ getName() + "' (analyzer=" + analyzer + ", searchAnalyzer=" + queryAnalyzer
					+ ") are not applied, the existing configuration is shared.");
		}

		try{
			LuceneIndexAccess access = LuceneIndexAccess.getIndexAccess(ctx, this.collection.getCollectionName());
			access.config.setStoreTextFieldValue(storedSource);
			access.init(tbl.type());
		} catch (IOException e) {
			log.error("Get LuceneIndexAccess of cache failed",e);
		}
		refreshLuceneIndex();
		backfillLuceneIndex();
	}

	/**
	 * 解析mongo建索引时的文本选项（命名对齐MongoDB Search索引配置）。
	 * 取第一个非空配置生效；未指定的项回落到Ignite的默认分析器。
	 */
	protected void initTextOptions() {
		for (IndexKey key : getKeys()) {
			if (!key.isText()) {
				continue;
			}
			Document options = key.textOptions();
			if (options == null) {
				continue;
			}
			if (analyzer == null) {
				analyzer = stringOption(options, "analyzer");
			}
			if (queryAnalyzer == null) {
				queryAnalyzer = stringOption(options, "searchAnalyzer");
				if (queryAnalyzer == null) {
					queryAnalyzer = stringOption(options, "queryAnalyzer");
				}
			}
			if (storedSource == null) {
				storedSource = "true".equalsIgnoreCase(stringOption(options, "storedSource"));
			}

			if (dynamic == null) {
				dynamic = "true".equalsIgnoreCase(stringOption(options, "dynamic"));
			}
		}
		if (analyzer != null || queryAnalyzer != null) {
			log.info("Text index '" + getName() + "' analyzer config: analyzer=" + analyzer + ", searchAnalyzer="
					+ queryAnalyzer);
		}
    }

	protected static String stringOption(Document options, String name) {
		Object value = options.get(name);
		return value == null ? null : value.toString();
	}

	/** 构建带分析器配置的FullTextQueryIndex（分析器命名对齐MongoDB Search）。 */
	protected FullTextQueryIndex buildFullTextQueryIndex() {
		FullTextQueryIndex idx = new FullTextQueryIndex(keys());
		if (analyzer != null) {
			idx.setAnalyzer(analyzer);
		}
		if (queryAnalyzer != null) {
			idx.setQueryAnalyzer(queryAnalyzer);
		} else if (analyzer != null) {
			idx.setQueryAnalyzer(analyzer); // 未单独指定时检索分析器与索引分析器一致
		}
		idx.setName(this.getName());
		return idx;
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
		if (textFields == null || !textFields.containsAll(keys())) {
			return false;
		}
		QueryIndexDescriptorImpl desc = textIndexDescriptor(tbl);
		QueryIndex qidx = desc == null ? null : desc.getQueryIndex();
		if (qidx == null) {
			return false; // 无QueryIndex对象时LuceneIndexAccess不会为这些字段建索引
		}
		if (qidx instanceof VectorQueryIndex) {
			// 字段被注册为向量字段（数值数组）时无法做文本检索，交集非空则放弃本索引
			return Collections.disjoint(((VectorQueryIndex) qidx).getFieldNames(), keys());
		}
		return true;
	}

	@Override
	public boolean canHandle(Document query) {
		if (!isRegistered()) {
			return false;
		}

		// { $text : { $search: 'keyword' } }
		if (query.containsKey("$text") && BsonRegularExpression.isTextSearchExpression(query.get("$text"))) {
			return true;
		}

		if (!CollectionUtils.containsAny(query.keySet(), keySet())) {
			return false;
		}

		if (isSparse()) { // always sparse
			boolean allIndexFieldsNull = keys().stream().allMatch(k -> query.get(k) == null);
			if (allIndexFieldsNull) {
				return false;
			}
		}

		for (String key : queryableKeys()) {
			Object value = query.get(key);
			if (value == null) {
				continue;
			}
			if (value instanceof CharSequence) {
				return true; // { field: 'keyword' }
			}
			if (value instanceof Document && ((Document) value).size() == 1
					&& BsonRegularExpression.isTextSearchExpression(value)) {
				return true; // { field: { $text: 'keyword' } }
			}
			// $regex等首版不走索引
		}
		return false;
	}

	@Override
	public Iterable<Object> getPositions(Document query) {
		List<IdWithMeta> results = null;

		// { $text : { $search: 'keyword', $limit: 10, $scoreThreshold: 0.5 } }
		if (query.containsKey("$text")) {
			Object text = query.get("$text");
			if (text instanceof Document && BsonRegularExpression.isTextSearchExpression(text)) {
				Document textDoc = (Document) text;
				Object search = textDoc.containsKey("$search") ? textDoc.get("$search") : textDoc.get("$text");
				if (search != null) {
					List<IdWithMeta> found = executeTextQuery(buildMultiFieldQuery(search.toString()),
							limitOf(textDoc, 0), scoreThresholdOf(textDoc), "textScore");
					results = results == null ? found : intersect(results, found);
				}
				if (!IgniteVectorIndex.isVectorSearchExpression(textDoc)) {
					query.remove("$text"); // 没有向量部分时整个$text已被消费
				}
			}
		}

		for (String key : queryableKeys()) {
			Object value = query.get(key);
			if (value == null) {
				continue;
			}
			List<IdWithMeta> found = null;
			if (value instanceof Document) {
				Document doc = (Document) value;
				if (doc.size() == 1 && BsonRegularExpression.isTextSearchExpression(doc)) {
					Object text = doc.containsKey("$text") ? doc.get("$text") : doc.get("$search");
					if (text != null) {
						found = executeTextQuery(buildFieldQuery(key, text.toString()), limitOf(doc, 0),
								scoreThresholdOf(doc), "searchScore");
					}
				}
			} else if (value instanceof CharSequence) {
				found = executeTextQuery(buildFieldQuery(key, value.toString()), 0, 0, "searchScore");
			}
			if (found != null) {
				query.remove(key);
				results = results == null ? found : intersect(results, found);
			}
		}

		return (Iterable) (results == null ? new ArrayList<IdWithMeta>() : results);
	}

	/** 所有本索引字段的OR检索：{@code (f1:(kw) OR f2:(kw) ...)}。 */
	protected String buildMultiFieldQuery(String search) {
		List<String> parts = new ArrayList<>();
		for (String field : queryableKeys()) {
			parts.add(buildFieldQuery(field, search));
		}
		if (parts.isEmpty()) {
			return search;
		}
		return parts.size() == 1 ? parts.get(0) : "(" + String.join(" OR ", parts) + ")";
	}

	/** 单字段限定检索：{@code field:(text)}。 */
	protected static String buildFieldQuery(String field, String text) {
		return quoteField(field) + ":(" + text + ")";
	}

	private static String quoteField(String field) {
		return field.matches("[A-Za-z0-9_]+") ? field : '"' + field.replace("\"", "\\\"") + '"';
	}

	/** 通过Ignite的TextQuery检索（底层是Ignite自己的GridLuceneIndex，与Mongo共用同一份索引数据）。 */
	protected List<IdWithMeta> executeTextQuery(String text, int limit, float scoreThreshold, String scoreField) {
		List<IdWithMeta> result = new ArrayList<>();
		TextQuery<Object, Object> qry = limit > 0 ? new TextQuery<>(typeName(), text, limit)
				: new TextQuery<>(typeName(), text);
		QueryCursor<javax.cache.Cache.Entry<Object, Object>> cursor = null;
		try {
			cursor = collection.dataMap.query(qry);
			for (javax.cache.Cache.Entry<Object, Object> entry : cursor) {
				float score = entry instanceof ScoredCacheEntry ? ((ScoredCacheEntry<Object, Object>) entry).score()
						: Float.MAX_VALUE;
				if (score < scoreThreshold) {
					continue;
				}
				Document meta = new Document(scoreField, score);
				result.add(new IdWithMeta(entry.getKey(), meta));
			}
		} catch (Exception e) {
			log.error("Failed to execute text query [" + text + "] on type '" + typeName() + "'", e);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
		return result;
	}
}
