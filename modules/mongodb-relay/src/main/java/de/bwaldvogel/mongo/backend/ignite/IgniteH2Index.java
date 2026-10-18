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
import java.util.concurrent.TimeUnit;

import javax.cache.Cache;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.ignite.util.DocumentUtil;
import de.bwaldvogel.mongo.bson.Document;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.LuceneIndexAccess;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneIndex;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * mongo二级索引基类：不再在mongodb-relay中维护一份独立的Lucene索引数据，
 * 而是通过 {@code IgniteH2Indexing} 的动态schema操作把索引定义注册进Ignite自身的索引体系
 * （H2 B+Tree / GridLuceneIndex），Ignite与MongoDB共用同一份索引。
 *
 * <p>索引维护（add/remove/update）全部委托给Ignite在cache store/remove时的索引钩子
 * （{@code IgniteH2Indexing.store(...)} 会驱动 H2 表与 Lucene 索引的更新），
 * 因此 {@link #add}/{@link #remove}/{@link #updateInPlace} 均为空实现。
 *
 * <p>注册是按 (cache, typeName) 幂等的：
 * <ul>
 *   <li>类型首次注册：{@code dynamicAddQueryEntity}（携带QueryEntity字段与索引定义）；</li>
 *   <li>类型已存在时追加SORTED索引：{@code dynamicColumnAdd} + {@code dynamicIndexCreate}；</li>
 *   <li>类型已注册后追加的text/knnVector字段通过 {@code addFieldToTextIndex} 动态并入类型的FULLTEXT索引
 *       （补建/重建QueryIndex与GridLuceneIndex实例），并回填存量文档到Lucene索引；</li>
 * </ul>
 *
 * <p>仅支持顶层与二层（一个点号）字段；更深层的点号路径退化为 {@code matchDocuments} 全表扫描。
 */
public abstract class IgniteH2Index extends Index<Object> {

	protected static final Logger log = LoggerFactory.getLogger(IgniteH2Index.class);

	/** 分布式schema操作（注册实体/索引/列）的等待超时。 */
	private static final long SCHEMA_OP_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10);

	/** 推断字段类型时最多采样的文档数。 */
	private static final int FIELD_TYPE_SAMPLE_SIZE = 64;

	/** 可以直接映射为H2列类型的采样类型。 */
	private static final Set<Class<?>> SQL_FIELD_TYPES = new LinkedHashSet<>(Arrays.asList(
			String.class, Integer.class, Long.class, Float.class, Double.class,
			Boolean.class, BigDecimal.class, Date.class, UUID.class));

	/** index key textOptions里的type取值。 */
	static final String TYPE_TEXT = "text";

	/** index key textOptions里的type取值。 */
	static final String TYPE_HASHED = "hashed";

	/** index key textOptions里的type取值。 */
	static final String[] TYPE_VECTOR = { "knnVector", "vector", "annVector" };

	protected final IgniteBinaryCollection collection;
	protected final GridKernalContext ctx;
	protected final IgniteH2Indexing igniteH2Indexing;


	/** 索引定义是否已成功注册进Ignite。 */
	private volatile boolean registered = false;

	protected IgniteH2Index(GridKernalContext ctx, IgniteBinaryCollection collection, String name, List<IndexKey> keys,
			boolean sparse) {
		super(name, indexFields(keys,collection), true); // mongo侧的索引语义上总是sparse（缺失字段的文档不参与匹配）
		this.ctx = ctx;
		this.collection = collection;

		this.igniteH2Indexing = (IgniteH2Indexing) ctx.query().getIndexing();
		if (igniteH2Indexing == null) {
			throw new UnsupportedOperationException("Use IgniteH2Index must dependecy ignite-indexed.jar");
		}
	}

	public boolean isRegistered() {
		return registered;
	}

	/** 重新尝试注册（幂等），用于collection的 "reIndex" 操作。 */
	public void init() {
		register();
	}

	protected final void register() {
		if (getKeys().isEmpty()) {
			log.warn("Mongo index '" + getName() + "' has no registrable field (only _id or deeper fields); "
					+ "it will not be registered in Ignite, queries fall back to full scan");
		} else {
			try {
				doRegister();
			} catch (Exception e) {
				log.error("Failed to register mongo index '" + getName() + "' of type '" + typeName()
						+ "' into Ignite indexing, queries will fall back to full scan", e);
			}
		}
		registered = verifyRegistration();
		if (!registered) {
			log.warn("Mongo index '" + getName() + "' is not registered in Ignite indexing for type '" + typeName()
					+ "', queries will fall back to full scan");
		}
	}

	/**
	 * 把索引定义注册进Ignite的索引体系。必须可重复调用（幂等）。
	 */
	protected abstract void doRegister() throws Exception;

	/**
	 * 校验索引定义是否已经存在于Ignite索引体系中。
	 */
	protected abstract boolean verifyRegistration();

	/** Ignite计算出的schema名（与SchemaManager.schemaName一致）。 */
	protected String schemaName() {
		return collection.getSchema();
	}

	public final String typeName() {
		return collection.getTypeName();
	}

	/** 表名；类型未注册时按Ignite默认规则推导。 */
	protected String tableName() {
		String tbl = collection.getTableName();
		return tbl != null ? tbl : QueryUtils.normalizeObjectName(typeName(), true);
	}

	protected TableDescriptor findTable() {
		Collection<TableDescriptor> tables = igniteH2Indexing.schemaManager().tablesForCache(collection.getCollectionName());
		if (tables != null) {
			for (TableDescriptor table : tables) {
				if (typeName().equalsIgnoreCase(table.type().name())) {
					return table;
				}
			}
		}
		return null;
	}

	/** 本mongo索引中可以注册进Ignite的字段（仅支持顶层和二层字段；{@code _id} 是cache key本身，不声明列）。 */
	protected static List<IndexKey> indexFields(List<IndexKey> keys,IgniteBinaryCollection collection) {
		List<IndexKey> fields = new ArrayList<>();
		for (IndexKey key : keys) {
			if (isIdField(key.getKey(),collection)) {
				log.debug("Field '" + key.getKey() + "' of mongo index '" + collection.getCollectionName()
						+ "' is the cache key itself and will not be declared as an Ignite column");
			} else if (isRegistrableField(key.getKey())) {
				fields.add(key);
			} else {
				log.warn("Field '" + key.getKey() + "' of mongo index '" + collection.getCollectionName()
						+ "' is deeper than the second level and will not be indexed in Ignite");
			}
		}
		return fields;
	}

	/**
	 * 本索引中实际参与Ignite检索的键：仅注册过的（顶层/二层）字段，且排除 {@code _id}。
	 * canHandle/getPositions只允许触碰这些键，避免对未注册字段检索得到错误的空结果。
	 */
	protected List<String> queryableKeys() {
		return this.keys();
	}


	/** Ignite的QueryEntity field支持顶层字段与带一个点号的二层字段。 */
	protected static boolean isRegistrableField(String field) {
		return field.indexOf('.') == field.lastIndexOf('.');
	}

	/** 本索引字段 + 集合中已知的其它索引的字段（用于首次注册时一次性声明列）。 */
	protected Collection<String> allKnownFields() {
		Set<String> fields = new LinkedHashSet<>(keySet());
		for (Index<Object> idx : collection.getIndexes()) {
			if (idx instanceof IgniteH2Index && idx != this) {
				fields.addAll(((IgniteH2Index) idx).keySet());
			}
		}
		return fields;
	}

	/** 判断某个index key是否属于指定的textOptions type。 */
	protected boolean isKeyOfType(String field, String... types) {
		for (IndexKey key : getKeys()) {
			if (!key.getKey().equals(field)) {
				continue;
			}
			String type = key.getType();
			if (type == null) {
				continue;
			}
			for (String t : types) {
				if (t.equalsIgnoreCase(type)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * 解析字段在QueryEntity中声明的Java类型。
	 * <p>
	 * 对现有文档采样（{@code documentToBinaryObject} 会按首个文档写入时的Binary元数据归一化数值类型，
	 * 因此同名字段的类型是稳定的）；采样失败或类型混杂时退化为 {@code java.lang.Object}
	 * （JAVA_OBJECT列，写入安全但不支持SQL查询）。
	 */
	protected Class<?> resolveFieldClass(String field) {
		if (isKeyOfType(field, TYPE_TEXT)) {
			return String.class; // 文本字段只经由Lucene检索，列类型仅用于兼容
		}
		if (isKeyOfType(field, TYPE_VECTOR)) {
			return float[].class; // 向量字段只经由Lucene检索
		}
		return sampleFieldClass(field);
	}

	private Class<?> sampleFieldClass(String field) {
		Set<Class<?>> classes = new LinkedHashSet<>();
		int scanned = 0;
		QueryCursor<Cache.Entry<Object, Object>> cursor = collection.dataMap.query(new ScanQuery<>());
		try {
			for (Cache.Entry<Object, Object> entry : cursor) {
				if (++scanned > FIELD_TYPE_SAMPLE_SIZE) {
					break;
				}
				Object value = rawFieldValue(entry.getValue(), field);
				if (value != null) {
					classes.add(value.getClass());
				}
			}
		} finally {
			cursor.close();
		}
		if (classes.size() == 1) {
			Class<?> cls = classes.iterator().next();
			if (SQL_FIELD_TYPES.contains(cls)) {
				return cls;
			}
		}
		return Object.class;
	}

	private static Object rawFieldValue(Object value, String field) {
		if (!(value instanceof org.apache.ignite.binary.BinaryObject)) {
			return null;
		}
		Object current = value;
		for (String part : field.split("\\.")) {
			if (!(current instanceof org.apache.ignite.binary.BinaryObject)) {
				return null;
			}
			current = ((org.apache.ignite.binary.BinaryObject) current).field(part);
			if (current == null) {
				return null;
			}
		}
		return current;
	}

	/** QueryEntity的fields映射：字段名 → Java类名。 */
	protected LinkedHashMap<String, String> buildFields(Collection<String> fields) {
		LinkedHashMap<String, String> res = new LinkedHashMap<>();
		for (String field : fields) {
			res.put(field, resolveFieldClass(field).getName());
		}
		return res;
	}

	/** 查询时用于参数转换的列类型；以Ignite中已注册的声明为准。 */
	protected Class<?> declaredFieldClass(String field) {
		TableDescriptor tbl = findTable();
		if (tbl != null) {
			GridQueryTypeDescriptor type = tbl.type();
			Class<?> cls = type.fields().get(field);
			if (cls != null) {
				return cls;
			}
		}
		return resolveFieldClass(field);
	}

	protected static String quote(String ident) {
		if(ident.startsWith("\"") && ident.endsWith("\"")) return ident;
		return '"' + ident + '"';
	}

	protected void addQueryEntity(org.apache.ignite.cache.QueryEntity entity) throws Exception {
		ctx.query().dynamicAddQueryEntity(collection.getCollectionName(), schemaName(), entity, null, true)
				.get(SCHEMA_OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
	}

	protected void createSqlIndex(org.apache.ignite.cache.QueryIndex idx) throws Exception {
		ctx.query().dynamicIndexCreate(collection.getCollectionName(), schemaName(), tableName(), idx, true, 1)
				.get(SCHEMA_OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
	}

	protected void addColumns(Collection<String> fields) throws Exception {
		TableDescriptor tbl = findTable();
		if (tbl == null) {
			return;
		}
		List<QueryField> cols = new ArrayList<>();
		for (String field : fields) {
			if (tbl.type().fields().containsKey(field)) {
				continue;
			}
			cols.add(new QueryField(field,
					resolveFieldClass(field).getName(), true));
		}
		if (!cols.isEmpty()) {
			ctx.query().dynamicColumnAdd(collection.getCollectionName(), schemaName(), tableName(), cols, true, false)
					.get(SCHEMA_OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		}
	}

	/** 类型文本索引描述符（{@code QueryIndexDescriptorImpl}，可能为null）。 */
	protected QueryIndexDescriptorImpl textIndexDescriptor(TableDescriptor tbl) {
		GridQueryIndexDescriptor desc = tbl.type().textIndex();
		return desc instanceof QueryIndexDescriptorImpl ? (QueryIndexDescriptorImpl) desc : null;
	}

	/**
	 * 把本索引的字段追加进类型已有的文本索引描述符（走 {@link QueryTypeDescriptorImpl#addFieldToTextIndex}，
	 * 描述符不存在时会自动创建）。调用前必须先通过 {@link #addColumns} 注册字段属性，
	 * 因为 addField 要求字段已存在于类型上。
	 *
	 * @param tbl 已注册的表描述符
	 * @return 追加后的文本索引描述符
	 * @throws IgniteCheckedException 若类型上不存在该字段
	 */
	protected QueryIndexDescriptorImpl appendTextIndexFields(TableDescriptor tbl) throws IgniteCheckedException {
		QueryTypeDescriptorImpl type = (QueryTypeDescriptorImpl) tbl.type();
		QueryIndexDescriptorImpl desc = textIndexDescriptor(tbl);
		Collection<String> existing = desc != null ? desc.fields() : Collections.emptyList();
		for (String field : keySet()) {
			if (!existing.contains(field)) {
				type.addFieldToTextIndex(field);
			}
		}
		return textIndexDescriptor(tbl);
	}

	/**
	 * 动态追加字段后无条件重建 {@code GridLuceneIndex} 实例：
	 * <ul>
	 *   <li>{@code store()} 的字段变化检查对比的是 {@code indexAccess.init(type)} 时的快照，
	 *       不重建则新字段永远不会进入共享字段映射、也不会被索引；</li>
	 *   <li>同时为“类型创建时无FULLTEXT索引”的情况补建实例（此时H2表上根本没有GridLuceneIndex）。</li>
	 * </ul>
	 * 旧实例的 {@code close()} 是空操作，且两个实例共享同一个Lucene目录（{@code LuceneIndexAccess}
	 * 按缓存名全局复用），因此覆盖是安全的。
	 */
	protected void refreshLuceneIndex() {
		GridH2Table h2Tbl = igniteH2Indexing.schemaManager().dataTable(schemaName(), tableName());
		if (h2Tbl == null) {
			return;
		}
		H2TableDescriptor desc = h2Tbl.tableDescriptor();
		if (desc.table() != null) {
			desc.createTextIndex(desc.table());
			log.info("Recreated GridLuceneIndex of table '" + tableName() + "' after text index field append");
		}
	}

	/**
	 * 遍历本节点缓存数据，把存量文档补写进Lucene索引：追加字段前写入的文档在Lucene记录中
	 * 没有新字段，不回填会导致查询漏掉这些文档。{@code GridLuceneIndex.store} 按key先删后写，幂等。
	 */
	protected void backfillLuceneIndex() {
		String cacheName = collection.getCollectionName();
		long count = 0;
		try {
			GridH2Table h2Tbl = igniteH2Indexing.schemaManager().dataTable(schemaName(), tableName());
			if (h2Tbl == null || h2Tbl.tableDescriptor().luceneIndex() == null) {
				return;
			}
			GridCacheAdapter<?, ?> cache = ctx.cache().internalCache(cacheName);
			if (cache == null) {
				return;
			}
			GridLuceneIndex luceneIdx = h2Tbl.tableDescriptor().luceneIndex();
			for (IgniteCacheOffheapManager.CacheDataStore store : cache.context().offheap().cacheDataStores()) {
				if (store.isEmpty()) {
					continue;
				}
				try {
					GridCursor<? extends CacheDataRow> cursor = store.cursor();
					while (cursor.next()) {
						CacheDataRow row = cursor.get();
						long expireTime = row.expireTime() == 0 ? Long.MAX_VALUE : row.expireTime();
						try {
							luceneIdx.store(row.key(), row.value(), row.version(), expireTime);
							count++;
						} catch (Exception e) {
							log.warn("Failed to backfill a document into Lucene index of table '" + tableName()
									+ "' [key=" + row.key() + ']', e);
						}
					}
				} catch (Exception e) {
					// 单个分区迭代失败（如并发修改）不影响其它分区
					log.error("Failed to backfill partition " + store.partId() + " of cache '" + cacheName
							+ "' into Lucene index", e);
				}
			}
			log.info("Backfilled " + count + " documents into the Lucene index of table '" + tableName() + "'");
		} catch (Exception e) {
			log.error("Failed to backfill the Lucene index of table '" + tableName() + "'; queries on the "
					+ "appended fields may miss documents written before the append", e);
		}
	}

	protected static boolean isIdField(String field,IgniteBinaryCollection collection) {
		String idField = collection.getOptions().getIdField();
		return field.equals(idField) || field.equals("_id");
	}

	protected static int limitOf(Map<String, Object> opts, int defaultLimit) {
		Object limit = opts.get("$limit");
		if (limit instanceof Number) {
			return ((Number) limit).intValue();
		}
		return defaultLimit;
	}

	protected static float scoreThresholdOf(Map<String, Object> opts) {
		Object threshold = opts.get("$scoreThreshold");
		if (threshold instanceof Number) {
			return ((Number) threshold).floatValue();
		}
		return 0;
	}

	/** 按主键求交集（保留带meta的结果）。 */
	protected static List<IdWithMeta> intersect(List<IdWithMeta> a, List<IdWithMeta> b) {
		Set<Object> keys = new LinkedHashSet<>();
		for (IdWithMeta id : b) {
			keys.add(id.getKey());
		}
		List<IdWithMeta> res = new ArrayList<>();
		for (IdWithMeta id : a) {
			if (keys.contains(id.getKey())) {
				res.add(id);
			}
		}
		return res;
	}

	@Override
	public Object getPosition(Document document) {
		Object key = document.get(collection.getOptions().getIdField());
		if (key != null) {
			return DocumentUtil.toBinaryKey(key);
		}
		return null;
	}

	@Override
	public void checkAdd(Document document, MongoCollection<Object> collection) {
		// 索引维护由Ignite的store钩子完成
		if (!isRegistered()) {
			register();
		}
	}

	@Override
	public void add(Document document, Object position, MongoCollection<Object> collection) {
		// 索引维护由Ignite的store钩子完成（dataMap.put/remove会驱动IgniteH2Indexing.store/remove）
	}

	@Override
	public Object remove(Document document, MongoCollection<Object> collection) {
		return null;
	}

	@Override
	public void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<Object> collection) {
		// 索引维护由Ignite的store钩子完成
	}

	@Override
	public void updateInPlace(Document oldDocument, Document newDocument, Object position,
			MongoCollection<Object> collection) {
		// 索引维护由Ignite的store钩子完成（update通过dataMap.put覆盖）
	}

	@Override
	public long getCount() {
		return collection.dataMap.sizeLong(CachePeekMode.PRIMARY);
	}

	@Override
	public long getDataSize() {
		CacheMetrics metrics = collection.dataMap.metrics();
		long offHeap = metrics.getOffHeapAllocatedSize();
		return offHeap >= 0 ? offHeap : 0;
	}

	@Override
	public void drop() {
		try {
			ctx.query().dynamicIndexDrop(collection.getCollectionName(), schemaName(), getName(), true)
					.get(SCHEMA_OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			log.warn("Failed to drop SQL index '" + getName() + "' of type '" + typeName() + "'", e);
		}
		registered = false;
	}

	@Override
	public void close() {
		// 底层索引的生命周期由Ignite管理，这里无需释放任何资源
		super.close();
	}

	@Override
	public int getPriority() {
		return priorityBase() + getKeys().size();
	}

	/** 等值类索引优先于文本/向量索引参与查询规划。 */
	protected int priorityBase() {
		return 10;
	}
}
