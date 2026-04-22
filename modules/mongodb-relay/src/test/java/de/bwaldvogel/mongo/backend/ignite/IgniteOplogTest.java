package de.bwaldvogel.mongo.backend.ignite;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractOplogTest;


public class IgniteOplogTest extends AbstractOplogTest {

    @Override
    protected MongoBackend createBackend() throws Exception {        
        return IgniteBackend.inMemory();
    }
    
    @Override
    protected long getNumberOfOpenCursors() {
        return 0;
    }

}
