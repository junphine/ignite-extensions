package de.bwaldvogel.mongo.backend.ignite;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;

/**
 * B+Tree（SORTED）索引：把mongo的普通（1/-1）索引注册为Ignite的SORTED索引，
 * 等值/IN/范围查询通过参数化SQL（{@code SELECT _KEY FROM ... WHERE ...}）走Ignite的H2 B+Tree索引。
 *
 * <p>类型首次注册时通过 {@code dynamicAddQueryEntity} 建表并建索引；类型已存在时通过
 * {@code dynamicColumnAdd}（补齐缺失列）+ {@code dynamicIndexCreate}（ifNotExists幂等）追加索引。
 *
 * <p>已知限制（首版）：
 * <ul>
 *   <li>字段声明类型取自Binary元数据/文档采样；列类型与实际存储值不一致的文档会导致该字段不走SQL路径；</li>
 *   <li>{@code _id} 是cache key本身，不通过SQL列检索；</li>
 *   <li>{@code $regex} 等未支持的谓词由 {@code matchDocuments} 兜底过滤（本索引只提供超集）。</li>
 * </ul>
 */
public class IgniteSortedIndex extends IgniteH2Index {

	public IgniteSortedIndex(GridKernalContext ctx, IgniteBinaryCollection collection, String name,
			List<IndexKey> keys, boolean sparse) {
		super(ctx, collection, name, keys, sparse);
		register();
	}

	@Override
	protected void doRegister() throws Exception {
		TableDescriptor tbl = findTable();
		if (tbl == null) {
			QueryEntity entity = new QueryEntity();
			entity.setValueType(typeName());
			entity.setTableName(tableName());
			entity.setFields(buildFields(allKnownFields()));
			entity.getIndexes().add(buildQueryIndex());
			addQueryEntity(entity);
		} else {
			addColumns(indexFields());
			createSqlIndex(buildQueryIndex());
		}
	}

	@Override
	protected boolean verifyRegistration() {
		TableDescriptor tbl = findTable();
		if (tbl == null) {
			return false;
		}
		if (!tbl.type().indexes().containsKey(sqlIndexName())) {
			return false;
		}
		for (String field : indexFields()) {
			if (!tbl.type().fields().containsKey(field)) {
				return false;
			}
		}
		return true;
	}

	protected QueryIndex buildQueryIndex() {
		LinkedHashMap<String, Boolean> fields = new LinkedHashMap<>();
		for (IndexKey key : getKeys()) {
			if (isRegistrableField(key.getKey()) && !isIdField(key.getKey())) {
				fields.put(key.getKey(), key.isAscending());
			}
		}
		QueryIndex idx = new QueryIndex(fields, QueryIndexType.SORTED);
		idx.setName(sqlIndexName());
		return idx;
	}

	@Override
	public boolean canHandle(Document query) {
		if (!isRegistered()) {
			return false;
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

		Document $expr = query.getDocument("$expr");
		if ($expr != null && $expr.containsKey("$eq")) {
			List<?> args = (List<?>) $expr.get("$eq");
			for (String key : queryableKeys()) {
				String ekey = "$" + key;
				if (args.contains(ekey)) {
					((List<Object>) args).remove(ekey);
					query.remove("$expr");
					query.put(key, args.get(0));
					return true;
				}
			}
			return false;
		}

		for (String key : queryableKeys()) {
			Object queryValue = query.get(key);
			if (queryValue == null) {
				continue;
			}
			if (queryValue instanceof Document) {
				Document queryDoc = (Document) queryValue;
				if (!Utils.containsQueryExpression(queryDoc)) {
					continue; // 精确文档值相等：不走索引，由matchDocuments过滤
				}
				for (String queriedKey : queryDoc.keySet()) {
					if (isIndexableOperator(queriedKey)) {
						continue;
					}
					if (queryDoc.size() == 1) {
						return false; // 唯一谓词不受索引支持，退化为全表扫描
					}
					// 其它谓词仍可利用索引缩小范围，未支持部分由matchDocuments过滤
				}
			}
			// 标量值与集合值($in语义)均可走索引
		}
		return true;
	}

	/** 本索引可以在SQL中表达的mongo操作符；哈希索引会收窄此集合。 */
	protected boolean isIndexableOperator(String op) {
		return op.equals("=") || op.equals("$eq") || op.equals("$ne") || op.equals("$in") || op.equals("$nin")
				|| op.equals("$gt") || op.equals("$gte") || op.equals("$lt") || op.equals("$lte");
	}

	@Override
	public Iterable<Object> getPositions(Document query) {
		List<String> clauses = new ArrayList<>();
		List<Object> args = new ArrayList<>();

		for (String field : queryableKeys()) {
			Object value = query.get(field);
			if (value == null) {
				continue;
			}
			if (appendClauses(field, value, clauses, args)) {
				// 整个键值都已转成SQL谓词，从查询中移除（剩余谓词由matchDocuments过滤）
				query.remove(field);
			}
			// 部分未消费的键保留在查询中，matchDocuments会重新过滤（冗余但正确）
		}

		StringBuilder sql = new StringBuilder("SELECT _KEY FROM ").append(quote(schemaName())).append('.')
				.append(quote(tableName()));
		if (!clauses.isEmpty()) {
			sql.append(" WHERE ").append(String.join(" AND ", clauses));
		}
		return executeKeyQuery(sql.toString(), args);
	}

	/**
	 * 把某个字段的查询值转换成SQL谓词。
	 *
	 * @return true表示整个值都被消费（可从查询中移除该键），false表示只消费了部分或无法消费。
	 */
	private boolean appendClauses(String field, Object value, List<String> clauses, List<Object> args) {
		if (value instanceof Document) {
			Document expr = (Document) value;
			if (!Utils.containsQueryExpression(expr)) {
				return false; // 精确文档值：不可索引
			}
			boolean allConsumed = true;
			for (Map.Entry<String, Object> entry : expr.entrySet()) {
				if (!appendOperatorClause(field, entry.getKey(), entry.getValue(), clauses, args)) {
					allConsumed = false;
				}
			}
			return allConsumed;
		}
		if (value instanceof Collection) {
			if (((Collection<?>) value).isEmpty()) {
				return false; // {f: []} 的语义由matchDocuments兜底，索引不产生谓词
			}
			return appendOperatorClause(field, "$in", value, clauses, args);
		}
		return appendOperatorClause(field, "=", value, clauses, args);
	}

	/** @return 该谓词是否成功写入SQL。 */
	private boolean appendOperatorClause(String field, String operator, Object value, List<String> clauses,
			List<Object> args) {
		String col = quote(field);
		if (operator.equals("$in") || operator.equals("$nin")) {
			if (!(value instanceof Collection)) {
				return false;
			}
			List<Object> converted = new ArrayList<>();
			for (Object item : (Collection<?>) value) {
				Object arg = toSqlValue(field, item);
				if (arg == null) {
					return false;
				}
				converted.add(arg);
			}
			if (converted.isEmpty()) {
				if (operator.equals("$in")) {
					clauses.add("1 = 0"); // 空$in不匹配任何文档
				}
				return true;
			}
			String placeholders = String.join(",", Collections.nCopies(converted.size(), "?"));
			clauses.add(col + (operator.equals("$in") ? " IN (" : " NOT IN (") + placeholders + ")");
			args.addAll(converted);
			return true;
		}
		String sqlOp;
		switch (operator) {
		case "=":
		case "$eq":
			sqlOp = "=";
			break;
		case "$ne":
			sqlOp = "<>";
			break;
		case "$gt":
			sqlOp = ">";
			break;
		case "$gte":
			sqlOp = ">=";
			break;
		case "$lt":
			sqlOp = "<";
			break;
		case "$lte":
			sqlOp = "<=";
			break;
		default:
			return false;
		}
		Object arg = toSqlValue(field, value);
		if (arg == null) {
			return false;
		}
		clauses.add(col + " " + sqlOp + " ?");
		args.add(arg);
		return true;
	}

	/**
	 * 把mongo查询值转换为与列声明类型一致的SQL参数。
	 *
	 * @return 转换失败返回null（该谓词不走索引）。
	 */
	protected Object toSqlValue(String field, Object value) {
		if (value == null) {
			return null;
		}
		Class<?> cls = declaredFieldClass(field);
		if (cls == String.class) {
			return value instanceof CharSequence ? value.toString() : null;
		}
		if (cls == Long.class) {
			return value instanceof Number ? ((Number) value).longValue() : null;
		}
		if (cls == Integer.class) {
			return value instanceof Number ? ((Number) value).intValue() : null;
		}
		if (cls == Double.class) {
			return value instanceof Number ? ((Number) value).doubleValue() : null;
		}
		if (cls == Float.class) {
			return value instanceof Number ? ((Number) value).floatValue() : null;
		}
		if (cls == Boolean.class) {
			return value instanceof Boolean ? value : null;
		}
		if (cls == BigDecimal.class) {
			try {
				return value instanceof Number ? new BigDecimal(value.toString()) : null;
			} catch (NumberFormatException e) {
				return null;
			}
		}
		if (cls == Date.class) {
			return value instanceof Date ? value : null;
		}
		if (cls == UUID.class) {
			return value instanceof UUID ? value : null;
		}
		// java.lang.Object（JAVA_OBJECT）等类型不做SQL转换，退化为全表扫描过滤
		return null;
	}

	/** 执行取主键的SQL；失败时兜底返回全量主键（超集），保证matchDocuments过滤后结果正确。 */
	protected List<Object> executeKeyQuery(String sql, List<Object> args) {
		List<Object> keys = new ArrayList<>();
		try {
			runKeyQuery(sql, args, keys);
		} catch (Exception e) {
			log.error("Failed to execute index query [" + sql + "] on type '" + typeName()
					+ "', falling back to full scan", e);
			keys.clear();
			try {
				runKeyQuery("SELECT _KEY FROM " + quote(schemaName()) + '.' + quote(tableName()),
						Collections.emptyList(), keys);
			} catch (Exception e2) {
				log.error("Failed to scan keys of type '" + typeName() + "'", e2);
				keys.clear();
			}
		}
		return keys;
	}

	private void runKeyQuery(String sql, List<Object> args, List<Object> keys) {
		SqlFieldsQuery qry = new SqlFieldsQuery(sql);
		if (!args.isEmpty()) {
			qry.setArgs(args.toArray());
		}
		QueryCursor<List<?>> cursor = collection.dataMap.query(qry);
		try {
			for (List<?> row : cursor) {
				if (!row.isEmpty()) {
					keys.add(row.get(0));
				}
			}
		} finally {
			cursor.close();
		}
	}
}
