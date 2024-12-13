package com.github.vincentrussell.query.mongodb.sql.converter.rule;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

public class MongoRule extends ExternalResource {

	String version;
    private int port = 27017;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection mongoCollection;


    public MongoRule(String version) {
        this.version = version;
    }

    @Override
    protected void before() throws Throwable {
        
        mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost:" + port));
    }

    public MongoDatabase getDatabase(String databaseName) {
        return mongoClient.getDatabase(databaseName);
    }

    @Override
    protected void after() {
        mongoClient.close();        
    }
}
