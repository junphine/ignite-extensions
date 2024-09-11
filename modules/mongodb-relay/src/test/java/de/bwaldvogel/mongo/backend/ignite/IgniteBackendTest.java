package de.bwaldvogel.mongo.backend.ignite;

import org.apache.ignite.internal.processors.mongo.MongoPluginConfiguration;
import org.bson.Document;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;


class IgniteBackendTest extends AbstractBackendTest {

    @Override
    protected MongoBackend createBackend() throws Exception {
    	return IgniteBackend.inMemory(new MongoPluginConfiguration());
    }
    
    @Override
    protected long getNumberOfOpenCursors() {
        return 0;
    }

}
