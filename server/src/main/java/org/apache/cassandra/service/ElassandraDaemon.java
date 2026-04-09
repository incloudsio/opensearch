/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
 * Contains code from Elasticsearch / OpenSearch.
 *
 * Licensed under the Apache License, Version 2.0
 */
package org.apache.cassandra.service;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.WindowsTimer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elassandra.index.ElasticSecondaryIndex;
import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.common.inject.CreationException;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.common.inject.spi.Message;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.ElassandraNode;
import org.opensearch.node.InternalSettingsPreparer;
import org.opensearch.node.Node;
import org.opensearch.node.NodeValidationException;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Sets.newHashSet;

/**
 * Bootstrap: Cassandra {@code setup}/{@code joinRing} / ring ready, then embedded OpenSearch {@link Node}.
 */
public class ElassandraDaemon extends CassandraDaemon {

    private static final Logger logger = LogManager.getLogger(ElassandraDaemon.class);

    private static volatile Thread keepAliveThread;
    private static volatile CountDownLatch keepAliveLatch;

    public static ElassandraDaemon instance = null;

    protected volatile Node node = null;
    protected Environment env;

    private boolean activated = false;
    private boolean hasMetadata = false;

    public ElassandraDaemon(Environment env) {
        super(true);
        this.env = env;
        instance = this;
    }

    public Environment getEnvironment() {
        return this.env;
    }

    public Node node() {
        return this.node;
    }

    public Node node(Node node) {
        this.node = node;
        return node;
    }

    public void activate(
        boolean addShutdownHook,
        boolean createNode,
        Settings settings,
        Environment activateEnv,
        Collection<Class<? extends Plugin>> pluginList
    ) {
        try {
            DatabaseDescriptor.daemonInitialization();
            DatabaseDescriptor.createAllDirectories();
        } catch (ExceptionInInitializerError e) {
            System.out.println("Exception (" + e.getClass().getName() + ") encountered during startup: " + e.getMessage());
            String errorMessage = buildErrorMessage("Initialization", e);
            System.err.println(errorMessage);
            System.err.flush();
            System.exit(3);
        }

        String pidFile = System.getProperty("cassandra-pidfile");
        if (pidFile != null) {
            new File(pidFile).deleteOnExit();
        }

        NativeLibrary.tryMlockall();
        // OpenSearch {@code Bootstrap} is package-private; native/bootstrap work is covered by Cassandra mlockall
        // above and by probe init below (parity with fork's initializeProbes).
        ProcessProbe.getInstance();
        OsProbe.getInstance();
        JvmInfo.jvmInfo();

        if (addShutdownHook) {
            Runtime.getRuntime()
                .addShutdownHook(
                    new Thread() {
                        @Override
                        public void run() {
                            if (node != null) {
                                try {
                                    node.close();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    }
                );
        }

        if (createNode) {
            List<Class<? extends Plugin>> pluginList2 = Lists.newArrayList(pluginList);
            pluginList2.add(ElassandraPlugin.class);
            Settings merged = getSettings();
            this.node = new ElassandraNode(new Environment(merged, env.configFile()), pluginList2, true);
        }

        ElasticSecondaryIndex.runsElassandra = true;

        if (FBUtilities.isWindows) {
            WindowsTimer.startTimerPeriod(DatabaseDescriptor.getWindowsTimerInterval());
        }

        super.setup();
        super.start();

        if (this.node != null) {
            try {
                this.node.injector()
                    .getInstance(ClusterService.class)
                    .submitNumberOfShardsAndReplicasUpdate("user-keyspaces-bootstraped", null);
                this.node.start();
            } catch (NodeValidationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void systemKeyspaceInitialized() {
        logger.debug("System keyspaces initialized");
    }

    @Override
    public void userKeyspaceInitialized() {
        logger.debug("User keyspaces initialized");
        if (node != null && (SystemKeyspace.bootstrapComplete() || DatabaseDescriptor.getAutoSnapshot() == false)) {
            try {
                this.hasMetadata = this.node.injector().getInstance(ClusterService.class).hasMetaDataTable();
                if (this.hasMetadata) {
                    activateAndWaitShards("before opening user keyspaces");
                }
            } catch (Throwable e) {
                logger.warn("Unexpected error", e);
            }
        }
    }

    @Override
    public void beforeBootstrap() {
        if (node != null) {
            try {
                this.hasMetadata = this.node.injector().getInstance(ClusterService.class).hasMetaDataTable();
                activateAndWaitShards("before cassandra boostraping");
            } catch (Throwable e) {
                logger.error("Failed to load OpenSearch mapping from CQL schema before bootstraping:", e);
            }
        }
    }

    public static final String ELASSANDRA_SETUP_CLASS = "elassandra.setup.class";

    public interface SetupListener {
        void onComplete();
    }

    public void onNodeStarted() {
        // Fork registered setup listeners via ElassandraPlugin; side-car uses ClusterPlugin#onNodeStarted on the node.
    }

    public static class ElassandraPlugin extends Plugin implements ClusterPlugin {
        public ElassandraPlugin() {
            super();
        }

        @Override
        public void onNodeStarted() {
            ElassandraDaemon.instance.onNodeStarted();
        }
    }

    @Override
    public void ringReady() {
        if (node != null) {
            if (!activated) {
                try {
                    logger.info("waiting " + StorageService.RING_DELAY + "ms for CQL schema to get the opensearch mapping");
                    for (int i = 0; i < StorageService.RING_DELAY; i += 1000) {
                        if (!Schema.instance.getVersion().equals(SchemaConstants.emptyVersion)) {
                            logger.debug("got schema: {}", Schema.instance.getVersion());
                            break;
                        }
                        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                    }

                    if (!MigrationManager.isReadyForBootstrap()) {
                        logger.info("waiting for schema information to complete");
                        MigrationManager.waitUntilReadyForBootstrap();
                    }
                    this.hasMetadata = this.node.injector().getInstance(ClusterService.class).hasMetaDataTable();
                } catch (Throwable e) {
                    logger.warn("Failed to load opensearch mapping from CQL schema after joining without boostraping:", e);
                }
            }
            activateAndWaitShards(
                (hasMetadata) ? "after getting the opensearch mapping from CQL schema" : "with empty opensearch mapping"
            );
        }
    }

    public void activateAndWaitShards(String source) {
        if (!activated) {
            activated = true;
            logger.info("Activating OpenSearch, shards starting " + source);
            // OpenSearch 1.x: no ES-fork Node#activate(); barrier waits for local shards after Node#start.
            node.injector().getInstance(ClusterService.class).blockUntilShardsStarted();
            logger.info("OpenSearch shards started, ready to go on.");
        }
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
        if (node != null) {
            try {
                node.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void init(String[] args) {}

    @Override
    public void activate() {
        // JSVC hook — full node lifecycle uses {@link #activate(boolean, boolean, Settings, Environment, Collection)}.
    }

    @Override
    public void destroy() {
        super.destroy();
        if (node != null) {
            try {
                node.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (keepAliveLatch != null) {
            keepAliveLatch.countDown();
        }
    }

    public Node newNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins, boolean forbidPrivateIndexSettings) {
        Settings nodeSettings = nodeSettings(settings);
        logger.info("node settings={}", nodeSettings.toDelimitedString(','));
        List<Class<? extends Plugin>> classpathPlugins2 = Lists.newArrayList(classpathPlugins);
        classpathPlugins2.add(ElassandraPlugin.class);
        this.node = new ElassandraNode(new Environment(nodeSettings, env.configFile()), classpathPlugins2, forbidPrivateIndexSettings);
        return this.node;
    }

    public Settings getSettings() {
        return nodeSettings(env.settings());
    }

    public Settings nodeSettings(Settings settings) {
        return Settings.builder()
            .put("network.bind_host", DatabaseDescriptor.getRpcAddress().getHostAddress())
            .put("network.publish_host", FBUtilities.getBroadcastNativeAddressAndPort().address.getHostAddress())
            .put("transport.bind_host", FBUtilities.getLocalAddressAndPort().address.getHostAddress())
            .put(
                "transport.publish_host",
                Boolean.getBoolean("es.use_internal_address")
                    ? FBUtilities.getLocalAddressAndPort().address.getHostAddress()
                    : FBUtilities.getBroadcastAddressAndPort().address.getHostAddress()
            )
            .put("path.data", getElasticsearchDataDir())
            .put(settings)
            .put("discovery.type", org.elassandra.discovery.CassandraDiscoveryPlugin.CASSANDRA)
            .put("node.data", true)
            .put("node.master", true)
            .put("node.name", FBUtilities.getBroadcastAddressAndPort().address.getHostAddress())
            .put("node.attr.dc", DatabaseDescriptor.getLocalDataCenter())
            .put("node.attr.rack", DatabaseDescriptor.getEndpointSnitch().getRack(FBUtilities.getBroadcastAddressAndPort()))
            .put("cluster.name", ClusterService.getElasticsearchClusterName(env.settings()))
            .build();
    }

    public static org.opensearch.client.Client client() {
        if ((instance.node != null) && (instance.node.isClosed() == false)) {
            return instance.node.client();
        }
        return null;
    }

    public static org.opensearch.common.inject.Injector injector() {
        if ((instance.node != null) && (instance.node.isClosed() == false)) {
            return instance.node.injector();
        }
        return null;
    }

    public static String getHomeDir() {
        String cassandra_home = System.getenv("CASSANDRA_HOME");
        if (cassandra_home == null) {
            cassandra_home = System.getProperty("cassandra.home", System.getProperty("path.home"));
            if (cassandra_home == null) {
                throw new IllegalStateException(
                    "Cannot start: CASSANDRA_HOME and system properties cassandra.home or path.home are null."
                );
            }
        }
        return cassandra_home;
    }

    public static String getConfigDir() {
        String cassandra_conf = System.getenv("CASSANDRA_CONF");
        if (cassandra_conf == null) {
            cassandra_conf = System.getProperty("cassandra.conf", System.getProperty("path.conf", getHomeDir() + "/conf"));
        }
        return cassandra_conf;
    }

    public static String getElasticsearchDataDir() {
        String cassandra_storage = System.getProperty("cassandra.storagedir", getHomeDir() + File.separator + "data");
        return cassandra_storage + File.separator + "elasticsearch.data";
    }

    /**
     * CLI entry point (parity with fork). Prefer the standard OpenSearch/Cassandra launchers for production.
     */
    public static void main(String[] args) {
        try {
            DatabaseDescriptor.daemonInitialization();
            DatabaseDescriptor.createAllDirectories();
        } catch (ExceptionInInitializerError e) {
            System.out.println("Exception (" + e.getClass().getName() + ") encountered during startup: " + e.getMessage());
            String errorMessage = buildErrorMessage("Initialization", e);
            System.err.println(errorMessage);
            System.err.flush();
            System.exit(3);
        }

        boolean foreground = System.getProperty("cassandra-foreground") != null;
        if (System.getProperty("wrapper.service", "XXX").equalsIgnoreCase("true")) {
            foreground = false;
        }

        AccessController.doPrivileged(
            (PrivilegedAction<Object>) () -> {
                System.setProperty("es.set.netty.runtime.available.processors", "false");
                return null;
            }
        );

        String stage = "Initialization";

        try {
            if (!foreground) {
                System.out.close();
            }

            org.opensearch.env.Environment loaded = InternalSettingsPreparer.prepareEnvironment(
                Settings.builder().put("node.name", "node0").put("path.home", getHomeDir()).build(),
                Collections.emptyMap(),
                Paths.get(getConfigDir()),
                () -> "node0"
            );
            instance = new ElassandraDaemon(loaded);

            instance.activate(true, true, instance.env.settings(), instance.env, Collections.emptyList());
            if (!foreground) {
                System.err.close();
            }

            keepAliveLatch = new CountDownLatch(1);
            Runtime.getRuntime()
                .addShutdownHook(
                    new Thread() {
                        @Override
                        public void run() {
                            keepAliveLatch.countDown();
                        }
                    }
                );

            keepAliveThread = new Thread(
                () -> {
                    try {
                        keepAliveLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                "opensearch[keepAlive/" + Version.CURRENT + "]"
            );
            keepAliveThread.setDaemon(false);
            keepAliveThread.start();
        } catch (Throwable e) {
            String errorMessage = buildErrorMessage(stage, e);
            if (foreground) {
                System.err.println(errorMessage);
                System.err.flush();
            }
            logger.error("Exception", e);
            System.exit(3);
        }
    }

    private static String buildErrorMessage(String stage, Throwable e) {
        StringBuilder errorMessage = new StringBuilder("{").append(Version.CURRENT).append("}: ");
        errorMessage.append(stage).append(" Failed ...\n");
        if (e instanceof CreationException) {
            CreationException createException = (CreationException) e;
            Set<String> seenMessages = newHashSet();
            int counter = 1;
            for (Message message : createException.getErrorMessages()) {
                String detailedMessage;
                if (message.getCause() == null) {
                    detailedMessage = message.getMessage();
                } else {
                    detailedMessage = ExceptionsHelper.detailedMessage(message.getCause());
                }
                if (detailedMessage == null) {
                    detailedMessage = message.getMessage();
                }
                if (seenMessages.contains(detailedMessage)) {
                    continue;
                }
                seenMessages.add(detailedMessage);
                errorMessage.append("").append(counter++).append(") ").append(detailedMessage);
            }
        } else {
            errorMessage.append("- ").append(ExceptionsHelper.detailedMessage(e));
        }
        if (logger.isDebugEnabled()) {
            errorMessage.append("\n").append(ExceptionsHelper.stackTrace(e));
        }
        return errorMessage.toString();
    }
}
