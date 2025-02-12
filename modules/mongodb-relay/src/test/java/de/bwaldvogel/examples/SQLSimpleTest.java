package de.bwaldvogel.examples;



import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.apache.ignite.internal.processors.mongo.MongoPluginConfiguration;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;

import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.AbstractTest;
import de.bwaldvogel.mongo.backend.ignite.IgniteBackend;
import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assumptions.assumeTrue;



@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SQLSimpleTest {

    private static MongoCollection<Document> collection;
    
    private static MongoClient client;
    private static MongoServer server;

    @BeforeEach
    public void setUp() {
    	if(server!=null) {
    		return ;
    	}
        server = new MongoServer(IgniteBackend.inMemory(new MongoPluginConfiguration()));

        // bind on a random local port
        String serverAddress = server.bindAndGetConnectionString();

        client = MongoClients.create(serverAddress);
        
        for(String name:client.getDatabase("testdb").listCollectionNames()) {
        	System.out.println(name);
        }
        collection = client.getDatabase("testdb").getCollection("testcoll");
        
    }

    @AfterEach
    public void tearDown() {
    	collection.deleteMany(json("{}"));  	
    }

    public FindIterable<Document> findSql(String sql) {
    	Document obj = new Document("$sql", sql);    	
    	FindIterable<Document> list = collection.find(obj);
    	for(Document doc:list) {
    		System.out.println(doc);
    	}
    	return list;
    }
    
    public AggregateIterable<Document> execSql(String sql) {
    	Document pipeline = new Document("$sql", new Document("query",sql));    	
    	AggregateIterable<Document> list = client.getDatabase("testdb").aggregate(List.of(pipeline));
    	for(Document doc:list) {
    		System.out.println(doc);
    	}
    	return list;
    }
    
    @Order(0)
    @Test
    public void testSimpleInsertQuery() throws Exception {
        assertEquals(0, collection.countDocuments());
        
        collection.createIndex(json("title: 1"), new IndexOptions().unique(false).sparse(true));        
        //collection.createIndex(json("title: 'text', tag: 'text'"), new IndexOptions().unique(false).sparse(true));

        // creates the database and collection in memory and insert the object
        Document obj = new Document("_id", 1).append("title", "title1").append("text", "good world!");
        execSql("insert into testcoll(_id,title,text) values(1,'title1','good world!')");
        
        execSql("upsert into testcoll(_id,title,text) values(1,'title_new','not good world!') ON DUPLICATE KEY UPDATE title=VALUES(title) ");
        
        execSql("upsert into testcoll(_id,title,text) values(2,'title_new_2','not good world too!') ON DUPLICATE KEY UPDATE title=VALUES(title) ");

        assertEquals(2, collection.countDocuments());
        assertEquals(obj, collection.find().first());
    }
    
    
    @Order(1)
    @Test
    public void testSimpleUpdate() throws Exception {
        
        collection.insertOne(json("_id: 2, text: 'def'"));
        collection.insertOne(json("_id: 3, title: '标题'"));
        collection.insertOne(json("_id: 4, version: 0"));
        collection.insertOne(json("_id: 5, text: 'def', version: 0"));
        
        
        //collection.deleteOne(json("_id: 5"));
        execSql("delete from testcoll where _id=5");

        //collection.updateOne(json("_id: 2"), new Document("$set", json("text: null")));
        //collection.updateOne(json("_id: 1"), new Document("$set", json("text: 'def'")));
        execSql("update testcoll set text=null,title=null where _id=2");
        execSql("update testcoll set text='def',title='' where _id=4");
        execSql("update testcoll set version=version+1 where _id>=4");

        FindIterable<Document>  ret = findSql("select * from testcoll where text like 'def'");
        
        
        collection.updateOne(json("_id: 2"), new Document("$set", json("text: 'def', title:'这个 是 标题'")));
        ret = findSql("select distinct title from testcoll where title is not null");
        
        System.out.println("finish full text");
    }    

    @Order(2)
    @Test
    public void testGroupBySQL() throws Exception {
        //collection.createIndex(json("title: 1"), new IndexOptions().unique(true).sparse(true));

        collection.insertOne(json("_id: 2, title: 'def'"));
        collection.insertOne(json("_id: 3, title: '标题'"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5, title: 'def'"));
        
        FindIterable<Document>  ret = findSql("select title,count(*) from testcoll group by title");
        
        findSql("select name,count(*) from information_schema.tables group by name");
    }
    
    
    @Order(4)
    @Test
    public void testJoinSQL() throws Exception {
        //collection.createIndex(json("title: 1"), new IndexOptions().unique(true).sparse(true));

        collection.insertOne(json("_id: 2, title: 'def'"));
        collection.insertOne(json("_id: 3, title: '标题'"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5, title: 'def'"));
        
        MongoCollection<Document> dicts = client.getDatabase("testdb").getCollection("dicts");
        
        dicts.insertOne(json("_id: 2, tag: 'def'"));
        dicts.insertOne(json("_id: 3, tag: '标题'"));

        findSql("select t._id, t.title, d.tag as tag from testcoll t inner join dicts d ON t._id=d._id ");
        
        findSql("select t._id, t.title, d.tag as tag from testcoll t inner join dicts d ON t.title=d.tag ");
    }
    
    

    protected static void assertMongoWriteException(ThrowingCallable callable, int expectedErrorCode, String expectedMessage) {
        assertMongoWriteException(callable, expectedErrorCode, "Location" + expectedErrorCode, expectedMessage);
    }

    protected static void assertMongoWriteException(ThrowingCallable callable, int expectedErrorCode, String expectedCodeName,
                                                    String expectedMessage) {
    	try {
    		callable.call();
    	}
    	catch(MongoWriteException e) {
    		e.printStackTrace();
    	}
    	catch (Throwable e1) {
    		e1.printStackTrace();
			
		}        
    }

}