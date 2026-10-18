package de.bwaldvogel.mongo.backend.ignite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;

import org.apache.ignite.cache.FullTextQueryIndex;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.ScoredCacheEntry;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
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
 * <p>首版限制：
 * <ul>
 *   <li>{@code $regex} 不使用索引，退化为 {@code matchDocuments} 全表扫描过滤；</li>
 *   <li>FULLTEXT字段无法在类型注册后动态追加；后建的文本索引若字段未被注册，查询退化为全表扫描；</li>
 *   <li>{@code $sort} 不生效（Ignite的TextQuery仅支持double docValues字段排序）。</li>
 * </ul>
 */
public class IgniteTextIndex extends IgniteH2Index {

	public IgniteTextIndex(GridKernalContext ctx, IgniteBinaryCollection collection, String name, List<IndexKey> keys,
			boolean sparse) {
		super(ctx, collection, name, keys, sparse);
		register();
	}

	@Override
	protected int priorityBase() {
		return 100;
	}

	@Override
	protected void doRegister() throws Exception {
		if (findTable() != null) {
			// Ignite不支持在已注册类型上动态追加FULLTEXT字段（dynamicIndexCreate只支持SORTED）
			log.warn("Ignite type '" + typeName() + "' is already registered. Text fields " + indexFields()
					+ " cannot be added to its Lucene text index dynamically. Queries using mongo index '" + getName()
					+ "' will fall back to full scan.");
			return;
		}
		QueryEntity entity = new QueryEntity();
		entity.setValueType(typeName());
		entity.setTableName(tableName());
		entity.setFields(buildFields(allKnownFields()));
		entity.getIndexes().add(new FullTextQueryIndex(indexFields()));
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

		// { $text : { $search: 'keyword' } }
		if (query.containsKey("$text") && BsonRegularExpression.isTextSearchExpression(query.get("$text"))) {
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
						: 0f;
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
