package de.bwaldvogel.mongo.backend.ignite;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.ignite.internal.util.typedef.T2;

/**
 * B+Tree（SORTED）索引：把mongo的普通（1/-1）索引注册为Ignite的SORTED索引，
 * 等值/IN/范围查询通过参数化SQL（{@code SELECT _KEY FROM ... WHERE ...}）走Ignite的H2 B+Tree索引。
 *
 * <p>类型首次注册时通过 {@code dynamicAddQueryEntity} 建表并建索引；类型已存在时通过
 * {@code dynamicColumnAdd}（补齐缺失列）+ {@code dynamicIndexCreate}（ifNotExists幂等）追加索引。
 *
 * <p>当 {@code orderBy} 能被本索引的键前缀满足时，还会追加 {@code ORDER BY} 让B+Tree直接按序返回主键，
 * 由 {@link #getPositions(Document, Document)} 的第二个返回值告知调用方结果已有序，
 * 从而免去 {@code matchDocumentsFromStream} 的内存全量排序（内存占用从 O(命中文档) 降为 O(skip+limit)）。
 *
 * <p>已知限制（首版）：
 * <ul>
 *   <li>字段声明类型取自Binary元数据/文档采样；列类型与实际存储值不一致的文档会导致该字段不走SQL路径；</li>
 *   <li>{@code _id} 是cache key本身，不通过SQL列检索（按 {@code _id} 排序不走下推）；</li>
 *   <li>{@code $regex} 等未支持的谓词由 {@code matchDocuments} 兜底过滤（本索引只提供超集）；</li>
 *   <li>ORDER BY 下推仅当排序键是索引键前缀、方向全同/全反、且列类型为可比标量时启用；
 *       字符串序按H2的UTF-16码元序（与mongo的码点序在非BMP字符上有理论差异），null/缺失字段
 *       按H2语义（ASC在前）近似mongo的null语义。</li>
 * </ul>
 */
public class IgniteSortedIndex extends IgniteH2Index {
	/**
	 * 可以安全用于 ORDER BY 下推的列类型：与 mongo ValueComparator 的标量排序语义一致。
	 * <p>不含 UUID（Java UUID.compareTo 按有符号 msb 比较，与 mongo 按无符号字节序不一致）；
	 * {@code Object/JAVA_OBJECT}（混合类型/数组）由不在白名单内自动排除。
	 */
	private static final Set<Class<?>> SORTABLE_FIELD_TYPES = new LinkedHashSet<>(Arrays.asList(
			String.class, Integer.class, Long.class, Float.class, Double.class,
			Boolean.class, BigDecimal.class, Date.class));

	public IgniteSortedIndex(GridKernalContext ctx, IgniteBinaryCollection collection, String name,
			List<IndexKey> keys, boolean sparse) {
		super(ctx, collection, name, keys, sparse);
	}

	/**
	 * 判断本索引能否直接产出 {@code orderBy} 指定的顺序（B+Tree 前缀规则）。
	 *
	 * <p>全部满足才返回 true：
	 * <ul>
	 *   <li>索引已注册，且排序键序列是 {@link #queryableKeys()} 的前缀（{@code $natural} 等在此被拒绝）；</li>
	 *   <li>方向值是 {@code Number}（1=升序，-1=降序；{@code $meta} 等返回 false）；</li>
	 *   <li>列类型属于 {@link #SORTABLE_FIELD_TYPES}（可比标量）；</li>
	 *   <li>所有键的方向相对索引注册方向要么全相同，要么全相反（全反→B+Tree 反向扫描）。</li>
	 * </ul>
	 *
	 * <p>任何一条不满足即返回 false，调用方退回内存排序，行为与优化前完全一致。
	 */
	public boolean canProvideSortOrder(Document orderBy) {
		if (!isRegistered() || orderBy == null || orderBy.isEmpty()) {
			return false;
		}

		List<String> sortKeys = queryableKeys();
		List<Boolean> indexDirections = queryableKeyDirections();
		boolean firstKey = true;
		boolean reversed = false;
		int i = 0;
		for (Map.Entry<String, Object> entry : orderBy.entrySet()) {
			if (i >= sortKeys.size() || !entry.getKey().equals(sortKeys.get(i))) {
				return false; // 排序键必须是索引键的前缀（$natural、$meta 字段名也在此被拒绝）
			}
			Object direction = entry.getValue();
			if (!(direction instanceof Number)) {
				return false;
			}
			if (!SORTABLE_FIELD_TYPES.contains(declaredFieldClass(entry.getKey()))) {
				return false; // JAVA_OBJECT/混合类型列不保证与 mongo 序一致
			}
			boolean thisReversed = (((Number) direction).intValue() > 0) != indexDirections.get(i);
			if (firstKey) {
				reversed = thisReversed;
				firstKey = false;
			} else if (reversed != thisReversed) {
				return false; // 混合方向无法由同一次索引扫描产出
			}
			i++;
		}
		return !firstKey;
	}

	/** 与 {@link #queryableKeys()} 一一对应的索引注册方向（true=升序）。 */
	private List<Boolean> queryableKeyDirections() {
		List<Boolean> directions = new ArrayList<>();
		for (IndexKey key : getKeys()) {
			directions.add(key.isAscending());
		}
		return directions;
	}

	@Override
	protected void doRegister() throws Exception {
		TableDescriptor tbl = findTable();
		if (tbl == null) {
			QueryEntity entity = new QueryEntity();
			entity.setValueType(typeName());
			entity.setTableName(tableName());
			entity.setFields(buildFields(allKnownFields()));
			// QueryEntity.getIndexes() 在未初始化时返回不可变的空集合，必须通过 setIndexes 设置
			entity.setIndexes(new LinkedHashSet<>(Collections.singletonList(buildQueryIndex())));
			addQueryEntity(entity);
		} else {
			addColumns(keySet());
			createSqlIndex(buildQueryIndex());
		}
	}

	@Override
	protected boolean verifyRegistration() {
		TableDescriptor tbl = findTable();
		if (tbl == null) {
			return false;
		}
		if (!tbl.type().indexes().containsKey(getName())) {
			return false;
		}
		for (String field : keys()) {
			if (!tbl.type().fields().containsKey(field)) {
				return false;
			}
		}
		return true;
	}

	protected QueryIndex buildQueryIndex() {
		LinkedHashMap<String, Boolean> fields = new LinkedHashMap<>();
		for (IndexKey key : getKeys()) {
			fields.put(key.getKey(), key.isAscending());
		}
		QueryIndex idx = new QueryIndex(fields, QueryIndexType.SORTED);
		idx.setName(getName());
		return idx;
	}

	@Override
	public boolean canHandle(Document query) {
		if (!isRegistered()) {
			return false;
		}

		if (!CollectionUtils.containsAny(query.keySet(), keySet())) {
			return false;
		}

		if (isSparse()) { // always sparse
			boolean allIndexFieldsNull = queryableKeys().stream().allMatch(k -> query.get(k) == null);
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
		return getPositions(query, null).get1();
	}

	/**
	 * 取索引命中的主键（超集，剩余谓词由 matchDocuments 过滤）。
	 *
	 * <p>当 {@code orderBy} 非空且 {@link #canProvideSortOrder} 成立时，SQL 追加 {@code ORDER BY}，
	 * 由 Ignite 的 B+Tree（正向或反向扫描）直接按序返回主键。
	 *
	 * @return first = 命中主键；second = 结果是否已按 orderBy 有序。
	 *         仅当 ORDER BY 实际执行成功时为 true；SQL 失败走全量兜底扫描时为 false，
	 *         调用方据此保留内存排序，保证正确性。
	 */
	public T2<List<Object>, Boolean> getPositions(Document query, Document orderBy) {
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
		boolean ordered = orderBy != null && canProvideSortOrder(orderBy);
		if (ordered) {
			appendOrderBy(sql, orderBy);
		}
		List<Object> keys = new ArrayList<>();
		boolean succeeded = executeKeyQuery(sql.toString(), args, keys);
		return new T2<>(keys, Boolean.valueOf(ordered && succeeded));
	}

	/** 在SQL末尾追加 {@code ORDER BY "col" ASC/DESC, ...}（方向取自mongo的 orderBy 值）。 */
	private void appendOrderBy(StringBuilder sql, Document orderBy) {
		sql.append(" ORDER BY ");
		boolean firstKey = true;
		for (Map.Entry<String, Object> entry : orderBy.entrySet()) {
			if (!firstKey) {
				sql.append(", ");
			}
			firstKey = false;
			sql.append(quote(entry.getKey()))
					.append(((Number) entry.getValue()).intValue() > 0 ? " ASC" : " DESC");
		}
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

	/**
	 * 执行取主键的SQL；失败时兜底返回全量主键（超集），保证matchDocuments过滤后结果正确。
	 *
	 * @param keys 输出参数，填充命中的主键
	 * @return true=请求SQL执行成功；false=走了兜底全量扫描（结果相对请求的排序是无序的）
	 */
	protected boolean executeKeyQuery(String sql, List<Object> args, List<Object> keys) {
		try {
			runKeyQuery(sql, args, keys);
			return true;
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
			return false;
		}
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
