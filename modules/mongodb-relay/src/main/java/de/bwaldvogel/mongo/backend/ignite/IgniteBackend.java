package de.bwaldvogel.mongo.backend.ignite;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.mongo.MongoPluginConfiguration;
import org.apache.ignite.plugin.PluginConfiguration;


import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class IgniteBackend extends AbstractMongoBackend {    

    private final Ignite admin; // admin store
    
    private MongoPluginConfiguration cfg;
    
    private boolean isKeepBinary = true;
    
    long oldVersion = System.nanoTime();

    public static IgniteBackend inMemory() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName("testdb");
        cfg.setLocalHost("127.0.0.1");

        MongoPluginConfiguration mongoCfg = new MongoPluginConfiguration();
        cfg.setPluginConfigurations(mongoCfg);
    	Ignite admin = Ignition.getOrStart(cfg);
        return new IgniteBackend(admin,mongoCfg);
    }
    
	public void commit() {      
        long newVersion = System.nanoTime();
        log.debug("Committed MVStore (old: {} new: {})", oldVersion, newVersion);
    }

    public IgniteBackend(Ignite mvStore) {
        this.admin = mvStore;
        var igniteCfg = mvStore.configuration();

        if (igniteCfg.getPluginConfigurations() != null) {
            for (PluginConfiguration pluginCfg : igniteCfg.getPluginConfigurations()) {
                if (pluginCfg instanceof MongoPluginConfiguration) {
                    cfg = (MongoPluginConfiguration)pluginCfg;
                    setKeepBinary(cfg.isWithBinaryStorage());
                    break;
                }
            }
        }
    }

    public IgniteBackend(Ignite mvStore,MongoPluginConfiguration cfg) {
        this.admin = mvStore;
        this.cfg = cfg;
        setKeepBinary(cfg.isWithBinaryStorage());
    }

    public IgniteBackend(String fileName) {
        this(openAdminStore(fileName));
    }

    private static Ignite openAdminStore(String fileName) {
        if (fileName == null) {
            log.info("opening ignite use default config");
        } else {
            log.info("opening ignite use config file '{}'", fileName);
        }
        Ignite admin = Ignition.start(fileName);
        return admin;
    }

    @Override
    protected MongoDatabase openOrCreateDatabase(String databaseName) {
    	String gridName = databaseName;
    	if(databaseName!=null && databaseName.equalsIgnoreCase(IgniteDatabase.DEFAULT_DB_NAME)) {
    		gridName = null;
    		databaseName = IgniteDatabase.DEFAULT_DB_NAME;
    	}
    	else if(databaseName==null || databaseName.isEmpty()) {
    		gridName = null;
    		databaseName = IgniteDatabase.DEFAULT_DB_NAME;
    	}
        else if(databaseName.equalsIgnoreCase("admin")){
            gridName = this.admin.name();
        }
    	
    	try {
	    	Ignite mvStore = Ignition.ignite(gridName);
	        return new IgniteDatabase(databaseName, this, mvStore, this.getCursorRegistry());        
    	}
    	catch(Exception e) {    		
    		throw new MongoServerException(String.format("Database %s not install!",databaseName),e);
    	}
    }    
    
    @Override
    protected Set<String> listDatabaseNames() {
        return Ignition.allGrids().stream().map(Ignite::name).map(n -> n==null?"default":n).collect(Collectors.toSet());
    }
    
    @Override
    protected Document getServerDescription(){
    	long topV = admin.cluster().topologyVersion();
    	List<String> hostSet = new ArrayList<>();
    	StringBuilder primary = new StringBuilder("");
    	for(ClusterNode node: admin.cluster().nodes()) {
    		hostSet.addAll(node.addresses());
    		if(node.isLocal()) {
    			node.addresses().forEach(a->{ if(primary.length()==0) primary.append(a);});
    		}
    	}
    	Document response = super.getServerDescription();
    	//ObjectId processId = new ObjectId(admin.cluster().id().toString().replaceAll("-", "").substring(0,24));
    	//response.append("topologyVersion", processId);
    	//response.append("hosts", hostSet);
    	//response.put("primary",primary);
    	return response;
    }

    @Override
    public void close() {
        log.info("closing {}", this);
        
        for (String name : super.listDatabaseNames()) {
        	IgniteDatabase db = (IgniteDatabase)this.resolveDatabase(name);
        	db.close();
        }
    }

    public boolean isInMemory() {
        return !admin.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled();
    }

    @Override
    public String toString() {
        if (isInMemory()) {
            return getClass().getSimpleName() + "[inMemory]";
        } else {
            return getClass().getSimpleName() + "[" + admin.name() + "]";
        }
    }

	public boolean isKeepBinary() {
		return isKeepBinary;
	}

	public void setKeepBinary(boolean isKeepBinary) {
		this.isKeepBinary = isKeepBinary;
	}

	public MongoPluginConfiguration getCfg() {
		return cfg;
	}
	
}
