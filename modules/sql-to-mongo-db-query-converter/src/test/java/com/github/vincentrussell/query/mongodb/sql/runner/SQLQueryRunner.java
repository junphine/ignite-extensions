package com.github.vincentrussell.query.mongodb.sql.runner;

import java.util.Arrays;
import java.util.List;

import org.bson.BsonValue;
import org.bson.Document;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;


import com.github.vincentrussell.query.mongodb.sql.converter.MongoDBQueryHolder;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryConverter;
import com.github.vincentrussell.query.mongodb.sql.converter.SQLCommandType;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.from.SQLCommandInfoHolder;
import com.google.common.collect.Iterables;




public class SQLQueryRunner {
	
	final QueryConverter queryConverter;
	
	final SQLCommandInfoHolder sqlCommandInfoHolder;
	
	private final Integer aggregationBatchSize;
    private final Boolean aggregationAllowDiskUse;
	

    public SQLQueryRunner(QueryConverter queryConverter) {
		super();
		this.queryConverter = queryConverter;
		this.sqlCommandInfoHolder = queryConverter.getSqlCommandInfoHolder();
		this.aggregationBatchSize = queryConverter.aggregationBatchSize;
		this.aggregationAllowDiskUse = queryConverter.aggregationAllowDiskUse;
	}


	/**
     * @param mongoDatabase the database to run the query against.
     * @param <T>           variable based on the type of query run.
     * @return When query does a find will return QueryResultIterator&lt;{@link org.bson.Document}&gt;
     * When query does a count will return a Long
     * When query does a distinct will return QueryResultIterator&lt;{@link java.lang.String}&gt;
     * @throws ParseException when the sql query cannot be parsed
     */
    @SuppressWarnings("unchecked")
    public <T> T run(final MongoDatabase mongoDatabase) throws ParseException {
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();

        MongoCollection mongoCollection = mongoDatabase.getCollection(mongoDBQueryHolder.getCollection());

        if (SQLCommandType.SELECT.equals(mongoDBQueryHolder.getSqlCommandType())) {

            if (mongoDBQueryHolder.isDistinct()) {
                return (T) new QueryResultIterator<>(mongoCollection.distinct(
                		getDistinctFieldName(mongoDBQueryHolder), mongoDBQueryHolder.getQuery(), String.class));
            } else if (sqlCommandInfoHolder.isCountAll() && !queryConverter.isAggregate(mongoDBQueryHolder)) {
                return (T) Long.valueOf(mongoCollection.countDocuments(mongoDBQueryHolder.getQuery()));
            } else if (queryConverter.isAggregate(mongoDBQueryHolder)) {

                AggregateIterable aggregate = mongoCollection.aggregate(
                		queryConverter.generateAggSteps(mongoDBQueryHolder, sqlCommandInfoHolder));

                if (aggregationAllowDiskUse != null) {
                    aggregate.allowDiskUse(aggregationAllowDiskUse);
                }

                if (aggregationBatchSize != null) {
                    aggregate.batchSize(aggregationBatchSize);
                }

                return (T) new QueryResultIterator<>(aggregate);
            } else {
                FindIterable findIterable = mongoCollection.find(mongoDBQueryHolder.getQuery())
                        .projection(mongoDBQueryHolder.getProjection());
                if (mongoDBQueryHolder.getSort() != null && mongoDBQueryHolder.getSort().size() > 0) {
                    findIterable.sort(mongoDBQueryHolder.getSort());
                }
                if (mongoDBQueryHolder.getOffset() != -1) {
                    findIterable.skip((int) mongoDBQueryHolder.getOffset());
                }
                if (mongoDBQueryHolder.getLimit() != -1) {
                    findIterable.limit((int) mongoDBQueryHolder.getLimit());
                }

                return (T) new QueryResultIterator<>(findIterable);
            }
        } else if (SQLCommandType.DELETE.equals(mongoDBQueryHolder.getSqlCommandType())) {
            DeleteResult deleteResult = mongoCollection.deleteMany(mongoDBQueryHolder.getQuery());
            return (T) ((Long) deleteResult.getDeletedCount());
        } else if (SQLCommandType.UPDATE.equals(mongoDBQueryHolder.getSqlCommandType())) {
            Document updateSet = mongoDBQueryHolder.getUpdateSet();
            List<String> fieldsToUnset = mongoDBQueryHolder.getFieldsToUnset();
            UpdateResult result = new EmptyUpdateResult();
            if ((updateSet != null && !updateSet.isEmpty()) && (fieldsToUnset != null && !fieldsToUnset.isEmpty())) {
                result = mongoCollection.updateMany(mongoDBQueryHolder.getQuery(),
                        Arrays.asList(new Document().append("$set", updateSet),
                                new Document().append("$unset", fieldsToUnset)));
            } else if (updateSet != null && !updateSet.isEmpty()) {
                result = mongoCollection.updateMany(mongoDBQueryHolder.getQuery(),
                        new Document().append("$set", updateSet));
            } else if (fieldsToUnset != null && !fieldsToUnset.isEmpty()) {
                result = mongoCollection.updateMany(mongoDBQueryHolder.getQuery(),
                        new Document().append("$unset", fieldsToUnset));
            }
            return (T) ((Long) result.getModifiedCount());
        } else {
            throw new UnsupportedOperationException("SQL command type not supported");
        }
    }
    
    private String getDistinctFieldName(final MongoDBQueryHolder mongoDBQueryHolder) {
        return Iterables.get(mongoDBQueryHolder.getProjection().keySet(), 0);
    }

	
	private static class EmptyUpdateResult extends UpdateResult {
	    @Override
	    public boolean wasAcknowledged() {
	        return false;
	    }

	    @Override
	    public long getMatchedCount() {
	        return 0;
	    }

	    @Override
	    public long getModifiedCount() {
	        return 0;
	    }

	    @Override
	    public BsonValue getUpsertedId() {
	        return null;
	    }
	}
}



