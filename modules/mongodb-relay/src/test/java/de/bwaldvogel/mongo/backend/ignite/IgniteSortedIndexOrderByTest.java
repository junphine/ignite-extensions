package de.bwaldvogel.mongo.backend.ignite;

import java.util.ArrayList;
import java.util.List;

import org.apache.ignite.internal.util.typedef.T2;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoCollection;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractTest;
import de.bwaldvogel.mongo.backend.Index;

/**
 * 验证 {@link IgniteSortedIndex} 的 ORDER BY 下推（改动点1~3）：
 * 排序键命中 sorted 索引的键前缀时，queryDocuments 通过索引按序返回主键，
 * 免去 matchDocumentsFromStream 的内存全量排序；不满足条件时退回内存排序且结果仍正确。
 */
class IgniteSortedIndexOrderByTest extends AbstractTest {

    @Override
    protected MongoBackend createBackend() throws Exception {
        return IgniteBackend.inMemory();
    }

    @Override
    protected long getNumberOfOpenCursors() {
        return 0;
    }

    /** 服务端语义的 Document（与驱动侧 org.bson.Document 区分）。 */
    private static de.bwaldvogel.mongo.bson.Document mdoc(Object... keyValues) {
        de.bwaldvogel.mongo.bson.Document doc = new de.bwaldvogel.mongo.bson.Document();
        for (int i = 0; i < keyValues.length; i += 2) {
            doc.put((String) keyValues[i], keyValues[i + 1]);
        }
        return doc;
    }

    private IgniteBinaryCollection serverCollection(String collectionName) {
        AbstractMongoDatabase<?> database =
                (AbstractMongoDatabase<?>) ((AbstractMongoBackend) backend).resolveDatabase(TEST_DATABASE_NAME);
        return (IgniteBinaryCollection) database.resolveCollection(collectionName, false);
    }

    private IgniteSortedIndex sortedIndex(String collectionName) {
        IgniteBinaryCollection collection = serverCollection(collectionName);
        assertThat(collection).isNotNull();
        for (Index<Object> index : collection.getIndexes()) {
            if (index instanceof IgniteSortedIndex) {
                IgniteSortedIndex sorted = (IgniteSortedIndex) index;
                // 注册是惰性的（由后续写入的 checkAdd 触发），这里显式幂等触发一次
                sorted.init();
                return sorted;
            }
        }
        return null;
    }

    // ------------------------------------------------------------------
    // 改动点 1：canProvideSortOrder 能力判定
    // ------------------------------------------------------------------

    @Test
    void testCanProvideSortOrderRules() {
        MongoCollection<Document> coll = db.getCollection("sort_rules");
        coll.insertOne(new Document("_id", 1).append("a", 1).append("b", 1));
        coll.insertOne(new Document("_id", 2).append("a", 2).append("b", 2));
        coll.createIndex(new Document("a", 1).append("b", 1));

        IgniteSortedIndex index = sortedIndex("sort_rules");
        assertThat(index).isNotNull();
        assertThat(index.isRegistered()).isTrue();

        // 可下推：索引键前缀 + 方向全同/全反
        assertThat(index.canProvideSortOrder(mdoc("a", 1))).isTrue();
        assertThat(index.canProvideSortOrder(mdoc("a", -1))).isTrue();
        assertThat(index.canProvideSortOrder(mdoc("a", 1, "b", 1))).isTrue();
        assertThat(index.canProvideSortOrder(mdoc("a", -1, "b", -1))).isTrue();

        // 不可下推
        assertThat(index.canProvideSortOrder(mdoc("b", 1))).isFalse();               // 非前缀
        assertThat(index.canProvideSortOrder(mdoc("a", 1, "b", -1))).isFalse();      // 混合方向
        assertThat(index.canProvideSortOrder(mdoc("a", 1, "b", 1, "c", 1))).isFalse(); // 超出索引键
        assertThat(index.canProvideSortOrder(mdoc("a", "asc"))).isFalse();           // 方向非数值
        assertThat(index.canProvideSortOrder(mdoc("$natural", 1))).isFalse();
        assertThat(index.canProvideSortOrder(mdoc("c", 1))).isFalse();               // 非索引字段
        assertThat(index.canProvideSortOrder(new de.bwaldvogel.mongo.bson.Document())).isFalse();
        assertThat(index.canProvideSortOrder(null)).isFalse();
    }

    // ------------------------------------------------------------------
    // 改动点 2：getPositions 追加 ORDER BY 并返回有序标记
    // ------------------------------------------------------------------

    @Test
    void testGetPositionsReturnsOrderedKeys() {
        MongoCollection<Document> coll = db.getCollection("sort_positions");
        for (int i = 0; i < 10; i++) {
            coll.insertOne(new Document("_id", i).append("a", (i * 7) % 10));
        }
        coll.createIndex(new Document("a", 1));

        IgniteSortedIndex index = sortedIndex("sort_positions");
        assertThat(index).isNotNull();
        IgniteBinaryCollection server = serverCollection("sort_positions");

        // 范围谓词 + 降序排序
        T2<List<Object>, Boolean> result =
                index.getPositions(mdoc("a", mdoc("$gte", 0)), mdoc("a", -1));
        assertThat(result.get2()).isTrue();

        List<Long> values = new ArrayList<>();
        for (Object key : result.get1()) {
            de.bwaldvogel.mongo.bson.Document doc = server.getDocument(key);
            assertThat(doc).isNotNull();
            values.add(((Number) doc.get("a")).longValue());
        }
        assertThat(values).hasSize(10);
        for (int i = 1; i < values.size(); i++) {
            assertThat(values.get(i)).isLessThanOrEqualTo(values.get(i - 1));
        }

        // 排序键不在索引前缀中：不追加 ORDER BY，返回无序标记
        T2<List<Object>, Boolean> unordered =
                index.getPositions(mdoc("a", mdoc("$gte", 0)), mdoc("c", 1));
        assertThat(unordered.get2()).isFalse();
        assertThat(unordered.get1()).hasSize(10);
    }

    /** 缺失/null 字段在 ASC 时排最前（H2 与 mongo 语义一致）。 */
    @Test
    void testOrderedKeysWithMissingFields() {
        MongoCollection<Document> coll = db.getCollection("sort_nulls");
        coll.insertOne(new Document("_id", 1).append("a", 4));
        coll.insertOne(new Document("_id", 2)); // 缺失 a
        coll.insertOne(new Document("_id", 3).append("a", 2));
        coll.insertOne(new Document("_id", 4).append("a", null));
        coll.insertOne(new Document("_id", 5)); // 缺失 a
        coll.createIndex(new Document("a", 1));

        IgniteSortedIndex index = sortedIndex("sort_nulls");
        assertThat(index).isNotNull();
        IgniteBinaryCollection server = serverCollection("sort_nulls");

        T2<List<Object>, Boolean> result = index.getPositions(new de.bwaldvogel.mongo.bson.Document(), mdoc("a", 1));
        assertThat(result.get2()).isTrue();

        List<Long> values = new ArrayList<>();
        for (Object key : result.get1()) {
            de.bwaldvogel.mongo.bson.Document doc = server.getDocument(key);
            assertThat(doc).isNotNull();
            Object raw = doc.get("a");
            values.add(raw == null ? null : Long.valueOf(((Number) raw).longValue()));
        }
        assertThat(values).hasSize(5);
        // null/缺失在前，然后才是数值升序
        assertThat(values.subList(0, 3)).containsExactly(null, null, null);
        assertThat(values.get(3)).isEqualTo(2L);
        assertThat(values.get(4)).isEqualTo(4L);
    }

    // ------------------------------------------------------------------
    // 改动点 3：queryDocuments 单索引路径集成（端到端）
    // ------------------------------------------------------------------

    @Test
    void testSortWithFilterEndToEnd() {
        MongoCollection<Document> coll = db.getCollection("sort_e2e");
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            docs.add(new Document("_id", i).append("a", (i * 13) % 20)
                    .append("tag", i % 2 == 0 ? "x" : "y"));
        }
        coll.insertMany(docs);
        coll.createIndex(new Document("a", 1));
        assertThat(sortedIndex("sort_e2e")).isNotNull();

        Document filter = new Document("a", new Document("$gte", 0));

        // 升序
        List<Document> asc = coll.find(filter).sort(new Document("a", 1)).into(new ArrayList<>());
        assertThat(asc).hasSize(20);
        assertMonotonic(asc, "a", true);

        // 降序
        List<Document> desc = coll.find(filter).sort(new Document("a", -1)).into(new ArrayList<>());
        assertThat(desc).hasSize(20);
        assertMonotonic(desc, "a", false);

        // skip + limit：应等于升序结果的切片
        List<Document> page = coll.find(filter).sort(new Document("a", 1)).skip(5).limit(7)
                .into(new ArrayList<>());
        assertThat(page).hasSize(7);
        assertThat(idsOf(page)).containsExactlyElementsOf(idsOf(asc.subList(5, 12)));

        // 残留谓词（tag 无索引）：先由索引按序取主键，再流式过滤，顺序保持
        Document residualFilter = new Document("a", new Document("$gte", 0)).append("tag", "x");
        List<Document> residual = coll.find(residualFilter).sort(new Document("a", 1))
                .into(new ArrayList<>());
        assertThat(residual).hasSize(10);
        assertMonotonic(residual, "a", true);
        for (Document doc : residual) {
            assertThat(doc.getString("tag")).isEqualTo("x");
        }
    }

    @Test
    void testCompoundSortEndToEnd() {
        MongoCollection<Document> coll = db.getCollection("sort_compound");
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            docs.add(new Document("_id", i).append("a", i % 5).append("b", (i * 2) % 3));
        }
        coll.insertMany(docs);
        coll.createIndex(new Document("a", 1).append("b", 1));

        IgniteSortedIndex index = sortedIndex("sort_compound");
        assertThat(index).isNotNull();

        Document filter = new Document("a", new Document("$gte", 0));

        // 全同方向：可下推
        assertThat(index.canProvideSortOrder(mdoc("a", 1, "b", 1))).isTrue();
        List<Document> asc = coll.find(filter).sort(new Document("a", 1).append("b", 1))
                .into(new ArrayList<>());
        assertThat(asc).hasSize(30);
        assertMonotonic2(asc, "a", "b");

        // 全反方向（B+Tree 反向扫描）：可下推
        assertThat(index.canProvideSortOrder(mdoc("a", -1, "b", -1))).isTrue();
        List<Document> desc = coll.find(filter).sort(new Document("a", -1).append("b", -1))
                .into(new ArrayList<>());
        assertThat(desc).hasSize(30);
        assertReverseMonotonic2(desc, "a", "b");

        // 键前缀：可下推
        assertThat(index.canProvideSortOrder(mdoc("a", 1))).isTrue();
        List<Document> prefix = coll.find(filter).sort(new Document("a", 1)).into(new ArrayList<>());
        assertThat(prefix).hasSize(30);
        assertMonotonic(prefix, "a", true);

        // 混合方向：退回内存排序，结果仍正确
        assertThat(index.canProvideSortOrder(mdoc("a", 1, "b", -1))).isFalse();
        List<Document> mixed = coll.find(filter).sort(new Document("a", 1).append("b", -1))
                .into(new ArrayList<>());
        assertThat(mixed).hasSize(30);
        assertMonotonic(mixed, "a", true);

        // 非前缀（仅按 b 排序）：退回内存排序，结果仍正确
        assertThat(index.canProvideSortOrder(mdoc("b", 1))).isFalse();
        List<Document> byB = coll.find(filter).sort(new Document("b", 1)).into(new ArrayList<>());
        assertThat(byB).hasSize(30);
        assertMonotonic(byB, "b", true);
    }

    /** 非索引字段排序：退回内存排序，结果仍正确。 */
    @Test
    void testSortByNonIndexedFieldFallsBack() {
        MongoCollection<Document> coll = db.getCollection("sort_nonindexed");
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            docs.add(new Document("_id", i).append("a", i).append("c", (i * 5) % 15));
        }
        coll.insertMany(docs);
        coll.createIndex(new Document("a", 1));

        Document filter = new Document("a", new Document("$gte", 0));
        List<Document> result = coll.find(filter).sort(new Document("c", 1)).into(new ArrayList<>());
        assertThat(result).hasSize(15);
        assertMonotonic(result, "c", true);
    }

    // ------------------------------------------------------------------
    // 断言辅助
    // ------------------------------------------------------------------

    private static void assertMonotonic(List<Document> docs, String field, boolean ascending) {
        for (int i = 1; i < docs.size(); i++) {
            Number prev = (Number) docs.get(i - 1).get(field);
            Number next = (Number) docs.get(i).get(field);
            if (ascending) {
                assertThat(next.longValue()).isGreaterThanOrEqualTo(prev.longValue());
            } else {
                assertThat(next.longValue()).isLessThanOrEqualTo(prev.longValue());
            }
        }
    }

    private static void assertMonotonic2(List<Document> docs, String first, String second) {
        for (int i = 1; i < docs.size(); i++) {
            Document prev = docs.get(i - 1);
            Document next = docs.get(i);
            long prevA = ((Number) prev.get(first)).longValue();
            long nextA = ((Number) next.get(first)).longValue();
            if (prevA == nextA) {
                assertThat(((Number) next.get(second)).longValue())
                        .isGreaterThanOrEqualTo(((Number) prev.get(second)).longValue());
            } else {
                assertThat(nextA).isGreaterThan(prevA);
            }
        }
    }

    private static void assertReverseMonotonic2(List<Document> docs, String first, String second) {
        for (int i = 1; i < docs.size(); i++) {
            Document prev = docs.get(i - 1);
            Document next = docs.get(i);
            long prevA = ((Number) prev.get(first)).longValue();
            long nextA = ((Number) next.get(first)).longValue();
            if (prevA == nextA) {
                assertThat(((Number) next.get(second)).longValue())
                        .isLessThanOrEqualTo(((Number) prev.get(second)).longValue());
            } else {
                assertThat(nextA).isLessThan(prevA);
            }
        }
    }

    private static List<Long> idsOf(List<Document> docs) {
        List<Long> ids = new ArrayList<>();
        for (Document doc : docs) {
            ids.add(((Number) doc.get("_id")).longValue());
        }
        return ids;
    }
}
