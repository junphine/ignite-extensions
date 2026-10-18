package de.bwaldvogel.mongo.backend.ignite;

import java.util.List;

import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.bson.Document;

import org.apache.ignite.internal.GridKernalContext;

/**
 * 哈希索引：mongo的 {@code {"field": "hashed"}} 索引。
 *
 * <p>Ignite的QueryIndexType没有HASH类型，因此注册阶段与SORTED索引相同
 * （B+Tree索引对等值查询本身就是索引查找，语义等价）；查询阶段只接受等值与 {@code $in}
 * 谓词，范围查询退化为全表扫描，与mongo的哈希索引语义保持一致。
 */
public class IgniteHashIndex extends IgniteSortedIndex {

	public IgniteHashIndex(GridKernalContext ctx, IgniteBinaryCollection collection, String name, List<IndexKey> keys,
			boolean sparse) {
		super(ctx, collection, name, keys, sparse);
	}

	@Override
	protected boolean isIndexableOperator(String op) {
		// mongo的哈希索引只支持等值与$in
		return op.equals("=") || op.equals("$eq") || op.equals("$in");
	}

	@Override
	protected int priorityBase() {
		return 20;
	}

	@Override
	public boolean canHandle(Document query) {
		if (!isRegistered()) {
			return false;
		}
		if (!super.canHandle(query)) {
			return false;
		}
		// super.canHandle已完成表达式重写；再校验查询中的索引键只包含等值类谓词
		for (String key : queryableKeys()) {
			Object value = query.get(key);
			if (value instanceof Document) {
				for (String op : ((Document) value).keySet()) {
					if (!isIndexableOperator(op)) {
						return false;
					}
				}
			}
		}
		return true;
	}
}
