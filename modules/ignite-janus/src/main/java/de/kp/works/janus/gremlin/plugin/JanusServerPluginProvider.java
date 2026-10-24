/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.kp.works.janus.gremlin.plugin;

import java.io.File;
import java.io.Serializable;
import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.graphdb.server.JanusGraphSettings;
import org.jetbrains.annotations.Nullable;
import org.janusgraph.core.ConfiguredGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.server.JanusGraphServer;

import static org.apache.ignite.spi.IgnitePortProtocol.TCP;


/**
 * Security processor provider for tests.
 */
public class JanusServerPluginProvider implements PluginProvider<JanusPluginConfiguration> {

	// One Gremlin Server for all ignite instances
	public static JanusGraphServer janusGraphServer;
	
	public static Settings settings;	

	private String databaseName;

	private JanusPlugin gremlin = new JanusPlugin();
	
	private static int counter = 0;

	/** Ignite logger. */
	private IgniteLogger log;

	private JanusPluginConfiguration cfg;

	/** {@inheritDoc} */
	@Override
	public String name() {
		return "JanusGremlinServer";
	}

	/** {@inheritDoc} */
	@Override
	public String version() {
		return "3.7.3";
	}

	/** {@inheritDoc} */
	@Override
	public String copyright() {
		return "apache copy right";
	}

	/** {@inheritDoc} */
	@Override
	public JanusPlugin plugin() {
		return gremlin;
	}

	/** {@inheritDoc} */
	@Override
	public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
	}

	/** {@inheritDoc} */
	@SuppressWarnings("unchecked")
	@Override
	public @Nullable <T> T createComponent(PluginContext ctx, Class<T> cls) {
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void start(PluginContext ctx) {
		IgniteConfiguration igniteCfg = ctx.igniteConfiguration();

		Ignite ignite = ctx.grid();
		this.log = ctx.log(this.getClass());
		this.cfg = null;

		if (igniteCfg.getPluginConfigurations() != null) {
			for (PluginConfiguration pluginCfg : igniteCfg.getPluginConfigurations()) {
				if (pluginCfg instanceof JanusPluginConfiguration) {
					cfg = (JanusPluginConfiguration) pluginCfg;
					break;
				}
			}
		}

		if (cfg == null && "janus_backend".equals(ignite.name()) && !ignite.configuration().isClientMode()) {
			// if node name == 'janus-backend' auto enable create gremlin server
			cfg = new JanusPluginConfiguration();
		}

		boolean per = igniteCfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled();
		if (cfg != null && per) {
			cfg.setPersistenceEnabled(true);
		}

		if (cfg != null && settings==null) {
			databaseName = igniteCfg.getIgniteInstanceName();
			gremlin.databaseName = databaseName;
			counter++;
			String configBase = U.getIgniteHome() + "/config/gremlin-server/";

			try {
				settings = JanusGraphSettings.read(configBase + cfg.getGremlinServerCfg());
				janusGraphServer = new JanusGraphServer(configBase + cfg.getGremlinServerCfg());
				IgniteEx igniteEx = (IgniteEx) ctx.grid();;
				igniteEx.context().ports().registerPort(settings.port, TCP, getClass());

			} catch (Exception e) {
				log.error(e.getMessage(), e);
				throw new RuntimeException(e.getMessage());
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public void stop(boolean cancel) {
		
	}

	/** {@inheritDoc} */
	@Override
	public void onIgniteStart() {
		// start gremlin server singerton when janus-backend grid start
		if (cfg != null || janusGraphServer != null) {
			try {
				JanusGraphManager.resetInstance();
		        janusGraphServer.start().exceptionally(t -> {
		        	log.error("JanusGraph Server was unable to start and will now begin shutdown", t);
		            janusGraphServer.stop().join();
		            return null;
		            
		        }).join();
				log.info("JanaGremlinServer", "listern on " + settings.host + ":" + settings.port);

			} catch (Exception e) {
				log.error("JanaGremlinServer bind fail.", e);
				// throw new RuntimeException(e);
				janusGraphServer = null;
			}
		}
		
		if (cfg != null && janusGraphServer!=null) {
			gremlin.graphManager = janusGraphServer.getGremlinServer().getServerGremlinExecutor().getGraphManager();

			if(gremlin.graphManager.getGraph(databaseName)==null) {
				// Open a {@link JanusGraph} using a previously created Configuration				
				if(!settings.graphs.containsKey(databaseName)) {
					String configBase = U.getIgniteHome() + "/config/gremlin-server";
	
					File graphFile = new File(configBase, "janus-" + databaseName + ".properties");
					if (graphFile.exists()) {
						log.info(databaseName + "::load gremlim graph config file " + graphFile.getAbsolutePath());
						settings.graphs.put(databaseName, graphFile.getAbsolutePath());
					} else {
						graphFile = new File(configBase, "janus-default.properties");
						log.info(databaseName + "::load default gremlim graph config file " + graphFile.getAbsolutePath());
						settings.graphs.put(databaseName, graphFile.getAbsolutePath());
					}
				}
				
				String graphConfig = settings.graphs.get(databaseName);
				JanusGraph g = JanusGraphFactory.open(graphConfig);
				gremlin.graphManager.putGraph(gremlin.databaseName, g);
			}
		}
	}

	

	/** {@inheritDoc} */
	@Override
	public void onIgniteStop(boolean cancel) {		
		
		if(gremlin.graphManager!=null) {			
			try {
				ConfiguredGraphFactory.drop(gremlin.databaseName);
			} 
			catch (Exception e) {	
				log.error("JanaGremlinServer close fail.", e);
			}
			gremlin.graphManager = null;			
		}
		
		if(cfg!=null) {
			counter--;
			if (janusGraphServer!= null && counter<=0) {
				log.info("JanusGremlinServer", "shutting down " + janusGraphServer.toString());
				janusGraphServer.stop();			
				janusGraphServer = null;
			}
		}	
	}

	/** {@inheritDoc} */
	@Override
	public @Nullable Serializable provideDiscoveryData(UUID nodeId) {
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void receiveDiscoveryData(UUID nodeId, Serializable data) {
		// No-op.
	}

	/** {@inheritDoc} */
	@Override
	public void validateNewNode(ClusterNode node) throws PluginValidationException {
		// No-op.
	}	
}
