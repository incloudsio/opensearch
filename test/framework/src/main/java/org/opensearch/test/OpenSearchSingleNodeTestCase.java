/* OpenSearch 1.3 API alignment applied by patch-opensearch-opens-search-single-node-opensearch13-api.sh */
/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.opensearch.test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.google.common.collect.Lists;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ElassandraDaemon;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.client.Requests;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.MockEngineFactoryPlugin;
import org.opensearch.index.mapper.MockFieldFilterPlugin;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.InternalSettingsPreparer;
import org.opensearch.node.Node;
import org.opensearch.node.NodeMocksPlugin;
import org.opensearch.node.NodeValidationException;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.ScriptService;
import org.opensearch.search.MockSearchService;
import org.opensearch.search.internal.SearchContext;
import org.elassandra.discovery.MockCassandraDiscovery;
import org.opensearch.test.store.MockFSIndexStore;
import org.opensearch.test.transport.MockTransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Permission;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * A test that keep a singleton node started for all tests that can be used to get
 * references to Guice injectors in unit tests.
 */
@ThreadLeakScope(Scope.NONE)
public abstract class OpenSearchSingleNodeTestCase extends OpenSearchTestCase {

    private static final Semaphore testMutex = new Semaphore(1);

    /** One OpenSearch data root for the JVM singleton daemon; nested {@code elasticsearch.data} + extra {@link #createTempDir()} calls confused the Lucene mock FS. */
    private static volatile String embeddedOpensearchDataPath;

    /**
     * Delegates every {@link SecurityManager} check to a delegate so embedded Cassandra/OpenSearch behavior is preserved;
     * logs a stack trace when {@code System.exit} runs (via {@link #checkExit(int)}).
     */
    private static final class DelegatingExitTraceSecurityManager extends SecurityManager {
        private final SecurityManager delegate;

        private DelegatingExitTraceSecurityManager(SecurityManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public void checkExit(int status) {
            new Exception("[elassandra.test.trace.system.exit] SecurityManager.checkExit(" + status + ")").printStackTrace(System.err);
            delegate.checkExit(status);
        }

        @Override
        public void checkPermission(Permission perm) {
            delegate.checkPermission(perm);
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            delegate.checkPermission(perm, context);
        }

        @Override
        public void checkCreateClassLoader() {
            delegate.checkCreateClassLoader();
        }

        @Override
        public void checkAccess(Thread t) {
            delegate.checkAccess(t);
        }

        @Override
        public void checkAccess(ThreadGroup g) {
            delegate.checkAccess(g);
        }

        @Override
        public void checkExec(String cmd) {
            delegate.checkExec(cmd);
        }

        @Override
        public void checkLink(String lib) {
            delegate.checkLink(lib);
        }

        @Override
        public void checkRead(FileDescriptor fd) {
            delegate.checkRead(fd);
        }

        @Override
        public void checkRead(String file) {
            delegate.checkRead(file);
        }

        @Override
        public void checkRead(String file, Object context) {
            delegate.checkRead(file, context);
        }

        @Override
        public void checkWrite(FileDescriptor fd) {
            delegate.checkWrite(fd);
        }

        @Override
        public void checkWrite(String file) {
            delegate.checkWrite(file);
        }

        @Override
        public void checkDelete(String file) {
            delegate.checkDelete(file);
        }

        @Override
        public void checkConnect(String host, int port) {
            delegate.checkConnect(host, port);
        }

        @Override
        public void checkConnect(String host, int port, Object context) {
            delegate.checkConnect(host, port, context);
        }

        @Override
        public void checkListen(int port) {
            delegate.checkListen(port);
        }

        @Override
        public void checkAccept(String host, int port) {
            delegate.checkAccept(host, port);
        }

        @Override
        public void checkMulticast(InetAddress maddr) {
            delegate.checkMulticast(maddr);
        }

        @Override
        public void checkMulticast(InetAddress maddr, byte ttl) {
            delegate.checkMulticast(maddr, ttl);
        }

        @Override
        public void checkPropertiesAccess() {
            delegate.checkPropertiesAccess();
        }

        @Override
        public void checkPropertyAccess(String key) {
            delegate.checkPropertyAccess(key);
        }

        @Override
        public void checkPrintJobAccess() {
            delegate.checkPrintJobAccess();
        }

        @Override
        public void checkPackageAccess(String pkg) {
            delegate.checkPackageAccess(pkg);
        }

        @Override
        public void checkPackageDefinition(String pkg) {
            delegate.checkPackageDefinition(pkg);
        }

        @Override
        public void checkSetFactory() {
            delegate.checkSetFactory();
        }

        @Override
        public void checkSecurityAccess(String target) {
            delegate.checkSecurityAccess(target);
        }
    }

    /**
     * Cassandra / OpenSearch may install their own {@link SecurityManager} after earlier test setup; call again after
     * work that might replace it (e.g. end of {@code setUp}, start of {@code tearDown}).
     */
    private static boolean isExitTraceEnabled() {
        return Boolean.getBoolean("elassandra.test.trace.system.exit")
            || "1".equals(System.getenv("ELASSANDRA_TEST_TRACE_SYSTEM_EXIT"))
            || "true".equalsIgnoreCase(System.getenv("ELASSANDRA_TEST_TRACE_SYSTEM_EXIT"));
    }

    /** Not synchronized: Cassandra startup threads can call into code that re-enters here; a static lock deadlocks the ctor. */
    private static void ensureDelegatingExitTraceSecurityManager() {
        if (isExitTraceEnabled() == false) {
            return;
        }
        SecurityManager sm = System.getSecurityManager();
        if (sm instanceof DelegatingExitTraceSecurityManager) {
            return;
        }
        if (sm == null) {
            System.err.println(
                "[elassandra.test.trace.system.exit] SecurityManager is null after embedded init; cannot trace System.exit"
            );
            return;
        }
        System.setSecurityManager(new DelegatingExitTraceSecurityManager(sm));
        System.err.println(
            "[elassandra.test.trace.system.exit] wrapped SecurityManager for exit tracing: " + sm.getClass().getName()
        );
    }

    /**
     * {@link com.carrotsearch.randomizedtesting.ThreadLeakControl} calls {@code checkZombies()} at the start of each
     * test {@link org.junit.runners.model.Statement} <em>before</em> {@code @Before} — too early for instance hooks.
     * Reset the marker when this class loads so embedded Cassandra / prior suites do not skip every method with
     * {@code AssumptionViolatedException} ("Leaked background threads present (zombies).").
     */
    static {
        clearRandomizedZombieMarker();
        if (RandomizedRunner.hasZombieThreads()) {
            System.err.println(
                "[elassandra] RandomizedRunner.hasZombieThreads() is true immediately after clearRandomizedZombieMarker()"
            );
        }
        if (Boolean.getBoolean("elassandra.test.shutdown.hook")) {
            Runtime.getRuntime()
                .addShutdownHook(
                    new Thread(
                        () -> System.err.println("[elassandra.test.shutdown.hook] JVM shutdown hook ran (orderly exit or System.exit)"),
                        "elassandra-shutdown-hook"
                    )
                );
            Thread.setDefaultUncaughtExceptionHandler(
                (t, e) -> {
                    System.err.println("[elassandra.test.uncaught] thread=" + t.getName() + " (" + t.getId() + ")");
                    e.printStackTrace(System.err);
                }
            );
        }
    }

    /**
     * OpenSearch side-car Gradle runs often set {@code cassandra.config=file:...} without {@code cassandra.config.dir}
     * (the main build sets the latter via BuildPlugin). {@link Environment} still needs a config directory path.
     */
    private static Path resolveCassandraConfigDir() {
        String dir = System.getProperty("cassandra.config.dir");
        if (dir != null && !dir.isEmpty()) {
            return Paths.get(dir);
        }
        String cc = System.getProperty("cassandra.config");
        if (cc != null && cc.regionMatches(true, 0, "file:", 0, 5)) {
            java.nio.file.Path p = Paths.get(java.net.URI.create(cc));
            java.nio.file.Path parent = p.getParent();
            if (parent != null) {
                return parent;
            }
        }
        String home = System.getProperty("cassandra.home");
        if (home != null && !home.isEmpty()) {
            return Paths.get(home, "conf");
        }
        throw new IllegalStateException(
            "Set cassandra.config.dir, or cassandra.config=file:.../cassandra.yaml, or cassandra.home");
    }

    /**
     * @param opensearchDataPath absolute path for OpenSearch {@code path.data} / shared data; must live under Lucene's mock filesystem
     *                           (use {@link #createTempDir()}), not under {@code cassandra.home}/data, or {@link org.opensearch.env.NodeEnvironment} fails on the test FS.
     */
    public static synchronized void initElassandraDeamon(
        Settings testSettings,
        Collection<Class<? extends Plugin>> classpathPlugins,
        String opensearchDataPath
    ) {
        if (ElassandraDaemon.instance == null) {
            System.out.println("working.dir="+System.getProperty("user.dir"));
            System.out.println("cassandra.home="+System.getProperty("cassandra.home"));
            System.out.println("cassandra.config.loader="+System.getProperty("cassandra.config.loader"));
            System.out.println("cassandra.config="+System.getProperty("cassandra.config"));
            System.out.println("cassandra.config.dir="+System.getProperty("cassandra.config.dir"));
            System.out.println("cassandra-rackdc.properties="+System.getProperty("cassandra-rackdc.properties"));
            System.out.println("cassandra.storagedir="+System.getProperty("cassandra.storagedir"));
            System.out.println("logback.configurationFile="+System.getProperty("logback.configurationFile"));

            if (Boolean.parseBoolean(System.getProperty("elassandra.test.config.override", "true"))) {
                DatabaseDescriptor.daemonInitialization(() -> {
                    String homeProp = System.getProperty("cassandra.home");
                    if (homeProp == null) {
                        throw new IllegalStateException("cassandra.home must be set for Elassandra embedded tests");
                    }
                    java.io.File home = new java.io.File(homeProp);
                    org.apache.cassandra.config.Config c = new org.apache.cassandra.config.Config();
                    c.commitlog_sync = org.apache.cassandra.config.Config.CommitLogSync.periodic;
                    c.commitlog_sync_period_in_ms = 10000;
                    c.data_file_directories = new String[] { new java.io.File(home, "data").getPath() };
                    c.commitlog_directory = new java.io.File(home, "commitlog").getPath();
                    c.saved_caches_directory = new java.io.File(home, "saved_caches").getPath();
                    c.hints_directory = new java.io.File(home, "hints").getPath();
                    c.partitioner = "org.apache.cassandra.dht.Murmur3Partitioner";
                    c.endpoint_snitch = "org.apache.cassandra.locator.SimpleSnitch";
                    java.util.Map<String, String> seedParams = java.util.Collections.singletonMap("seeds", "127.0.0.1");
                    c.seed_provider = new org.apache.cassandra.config.ParameterizedClass(
                        "org.apache.cassandra.locator.SimpleSeedProvider",
                        seedParams
                    );
                    return c;
                });
            } else {
                DatabaseDescriptor.daemonInitialization();
            }
            DatabaseDescriptor.createAllDirectories();

            CountDownLatch startLatch = new CountDownLatch(1);
            ElassandraDaemon.instance = new ElassandraDaemon(InternalSettingsPreparer.prepareEnvironment(Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), System.getProperty("cassandra.home"))
                .build(), java.util.Collections.emptyMap(), null, () -> "127.0.0.1")) {
                @Override
                public Settings nodeSettings(Settings settings) {
                    return Settings.builder()
                        .put("bootstrap.memory_lock", false)
                        .put("bootstrap.system_call_filter", false)
                        .put("discovery.type", MockCassandraDiscovery.MOCK_CASSANDRA)
                        .put(Environment.PATH_HOME_SETTING.getKey(), System.getProperty("cassandra.home"))
                        .put(Environment.PATH_DATA_SETTING.getKey(), DatabaseDescriptor.getAllDataFileLocations()[0] + File.separatorChar + "elasticsearch.data")
                        .put(Environment.PATH_REPO_SETTING.getKey(), System.getProperty("cassandra.home")+"/repo")
                        // TODO: use a consistent data path for custom paths
                        // This needs to tie into the OpenSearchIntegTestCase#indexSettings() method
                        .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), DatabaseDescriptor.getAllDataFileLocations()[0] + File.separatorChar + "elasticsearch.data")
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
                        .put("node.roles", "data")
                        .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
                        .put("node.name", "127.0.0.1")
                        //.put(EsExecutors.PROCESSORS_SETTING.getKey(), 1) // limit the number of threads created
                        //.put("script.inline", "on")
                        //.put("script.indexed", "on")
                        //.put(EsExecutors.PROCESSORS, 1) // limit the number of threads created
                        .put("client.type", "node")
                        //.put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true)

                        .put(settings)
                        .build();
                }

                @Override
                public void ringReady() {
                    super.ringReady();
                    startLatch.countDown();
                }
            };

            Settings elassandraSettings = ElassandraDaemon.instance.nodeSettings(testSettings);
            Path confPath = resolveCassandraConfigDir();
            ElassandraDaemon.instance.activate(false, false,  elassandraSettings, new Environment(elassandraSettings, confPath), classpathPlugins);

            // wait cassandra start.
            try {
                startLatch.await();
            } catch (InterruptedException e) {
            }
        }
    }

    public OpenSearchSingleNodeTestCase() {
        super();
        if (embeddedOpensearchDataPath == null) {
            synchronized (OpenSearchSingleNodeTestCase.class) {
                if (embeddedOpensearchDataPath == null) {
                    // Unique leaf: ctor thread vs suite worker can otherwise share tempDir-001 (FileAlreadyExists).
                    embeddedOpensearchDataPath =
                        createTempDir("elassandra-embedded-os-" + java.util.UUID.randomUUID().toString().replace("-", ""))
                            .toAbsolutePath()
                            .toString();
                }
            }
        }
        initElassandraDeamon(nodeSettings(1), getPlugins(), embeddedOpensearchDataPath);
        // Cassandra installs ThreadAwareSecurityManager during daemon init; wrap before @Before / rules can skip the test.
        ensureDelegatingExitTraceSecurityManager();
    }

    /**
     * Iff this returns true mock transport implementations are used for the test runs. Otherwise not mock transport impls are used.
     * The default is <tt>true</tt>
     */
    protected boolean addMockTransportService() {
        return false;
    }

    /**
     * A boolean value to enable or disable mock modules. This is useful to test the
     * system without asserting modules that to make sure they don't hide any bugs in
     * production.
     *
     * @see OpenSearchIntegTestCase
     */
    public static final String TESTS_ENABLE_MOCK_MODULES = "tests.enable_mock_modules";
    private static final boolean MOCK_MODULES_ENABLED = "true".equals(System.getProperty(TESTS_ENABLE_MOCK_MODULES, "true"));

    /** Return the mock plugins the cluster should use */
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> mocks = new ArrayList<>();
        if (MOCK_MODULES_ENABLED && randomBoolean()) { // sometimes run without those completely
            if (randomBoolean() && addMockTransportService()) {
                mocks.add(MockTransportService.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockFSIndexStore.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(NodeMocksPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockEngineFactoryPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockSearchService.TestPlugin.class);
            }
            /*
            if (randomBoolean()) {
                mocks.add(AssertingTransportInterceptor.TestPlugin.class);
            }
            */
            if (randomBoolean()) {
                mocks.add(MockFieldFilterPlugin.class);
            }
        }

        if (addMockTransportService()) {
            mocks.add(MockTransportService.TestPlugin.class);
        }

        // nodeSettings() always sets transport.type to getTestTransportType() (mock-nio); the plugin must be on the classpath.
        mocks.add(getTestTransportPlugin());
        // ElassandraNode loads only explicit plugins; register Netty HTTP without a test:framework → modules Gradle edge.
        mocks.add(loadNetty4PluginClass());
        mocks.add(OpenSearchIntegTestCase.TestSeedPlugin.class);
        mocks.add(MockCassandraDiscovery.TestPlugin.class);
        return Collections.unmodifiableList(mocks);
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends Plugin> loadNetty4PluginClass() {
        try {
            return (Class<? extends Plugin>) Class.forName("org.opensearch.transport.Netty4Plugin");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("org.opensearch.transport.Netty4Plugin must be on the test runtime classpath", e);
        }
    }

    public MockCassandraDiscovery getMockCassandraDiscovery() {
        return (MockCassandraDiscovery) clusterService().getCassandraDiscovery();
    }

    // override this to initialize the single node cluster.
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.EMPTY;
    }

    static void reset() {
    }

    static void cleanup(boolean resetNode) {
        if (ElassandraDaemon.instance.node() != null) {
            DeleteIndexRequestBuilder builder = ElassandraDaemon.instance.node().client().admin().indices().prepareDelete("*");
            assertAcked(builder.get());
            if (resetNode) {
                reset();
            }
        }
    }
    public static String encodeBasicHeader(final String username, final String password) {
        return java.util.Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8));
    }

    private Node newNode() {
        Collection<Class<? extends Plugin>> plugins = getPlugins();
        if (plugins.contains(getTestTransportPlugin()) == false) {
            plugins = new ArrayList<>(plugins);
            plugins.add(getTestTransportPlugin());
        }
        if (plugins.contains(MockCassandraDiscovery.TestPlugin.class) == false) {
            plugins = new ArrayList<>(plugins);
            plugins.add(MockCassandraDiscovery.TestPlugin.class);
        }
        logger.info("plugins={}", plugins);
        Node node = ElassandraDaemon.instance.newNode(ElassandraDaemon.instance.nodeSettings(nodeSettings()), plugins, forbidPrivateIndexSettings());
        try {
            node.activate();
            node.start();
        } catch (NodeValidationException e) {
            throw new RuntimeException(e);
        }
        // register NodeEnvironment to remove node.lock
        closeAfterTest(node.getNodeEnvironment());
        return node;
    }

    protected void startNode(long seed) throws Exception {
        ElassandraDaemon.instance.node(RandomizedContext.current().runWithPrivateRandomness(seed, this::newNode));
        // we must wait for the node to actually be up and running. otherwise the node might have started,
        // elected itself master but might not yet have removed the
        // SERVICE_UNAVAILABLE/1/state not recovered / initialized block
        ClusterAdminClient clusterAdminClient = client().admin().cluster();
        ClusterHealthRequestBuilder builder = clusterAdminClient.prepareHealth();
        ClusterHealthResponse clusterHealthResponse = builder.setWaitForGreenStatus().get();

        assertFalse(clusterHealthResponse.isTimedOut());
    }

    private static void stopNode() throws IOException {
        if (ElassandraDaemon.instance != null) {
            Node node = ElassandraDaemon.instance.node();
            if (node != null)
                node.close();
            ElassandraDaemon.instance.node(null);
            IOUtils.close(node);
        }
    }

    @Before
    @Override
    public void setUp() throws Exception {
        logger.info("[{}#{}]: acquiring semaphore ={}", getTestClass().getSimpleName(), getTestName(), testMutex.toString());
        testMutex.acquireUninterruptibly();
        super.setUp();
        //the seed has to be created regardless of whether it will be used or not, for repeatability
        long seed = random().nextLong();
        // Create the node lazily, on the first test. This is ok because we do not randomize any settings,
        // only the cluster name. This allows us to have overridden properties for plugins and the version to use.
        if (ElassandraDaemon.instance.node() == null) {
            startNode(seed);
        }
        ensureDelegatingExitTraceSecurityManager();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        ensureDelegatingExitTraceSecurityManager();
        logger.info("[{}#{}]: cleaning up after test", getTestClass().getSimpleName(), getTestName());
        super.tearDown();
        try {
            DeleteIndexRequestBuilder builder = ElassandraDaemon.instance.node().client().admin().indices().prepareDelete("*");
            assertAcked(builder.get());

            Metadata metaData = client().admin().cluster().prepareState().get().getState().metadata();
            assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsGroups(),
                metaData.persistentSettings().size(), equalTo(0));
            assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsGroups(),
                metaData.transientSettings().size(), equalTo(0));

            List<String> userKeyspaces = Lists.newArrayList(Schema.instance.getUserKeyspaces());
            userKeyspaces.remove(this.clusterService().getElasticAdminKeyspaceName());
            assertThat("test leaves a user keyspace behind:" + userKeyspaces, userKeyspaces.size(), equalTo(0));
        } catch(Exception e) {
            logger.warn("[{}#{}]: failed to clean indices and metadata: error="+e, getTestClass().getSimpleName(), getTestName());
            logger.warn("Exception:", e);
        } finally {
            testMutex.release();
            logger.info("[{}#{}]: released semaphore={}", getTestClass().getSimpleName(), getTestName(), testMutex.toString());
        }
        if (resetNodeAfterTest()) {
            assert ElassandraDaemon.instance != null;
            stopNode();
            //the seed can be created within this if as it will either be executed before every test method or will never be.
            startNode(random().nextLong());
        }
    }

    /** Resets {@link RandomizedRunner}'s static {@code zombieMarker} via reflection; see static class initializer. */
    private static void clearRandomizedZombieMarker() {
        try {
            java.lang.reflect.Field f = RandomizedRunner.class.getDeclaredField("zombieMarker");
            f.setAccessible(true);
            // Replace the marker so any stale reference cannot leave the flag true (embedded Cassandra can set it
            // between suite boundaries in the same JVM).
            f.set(null, new AtomicBoolean(false));
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static synchronized void setUpClass() throws Exception {
        clearRandomizedZombieMarker();
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        stopNode();
    }


    /**
     * This method returns <code>true</code> if the node that is used in the background should be reset
     * after each test. This is useful if the test changes the cluster state metadata etc. The default is
     * <code>false</code>.
     */
    protected boolean resetNodeAfterTest() {
        return false;
    }

    /** The plugin classes that should be added to the node. */
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return getMockPlugins();
    }

    /** Helper method to create list of plugins without specifying generic types. */
    @SafeVarargs
    @SuppressWarnings("varargs") // due to type erasure, the varargs type is non-reifiable, which causes this warning
    protected final Collection<Class<? extends Plugin>> pluginList(Class<? extends Plugin>...plugins) {
        return Arrays.asList(plugins);
    }

    /** Additional settings to add when creating the node. Also allows overriding the default settings. */
    protected Settings nodeSettings() {
        return Settings.EMPTY;
    }

    /**
     * Returns a client to the single-node cluster.
     */
    public Client client() {
        return ElassandraDaemon.instance.node().client();
    }

    /**
     * Return a reference to the singleton node.
     */
    protected Node node() {
        return ElassandraDaemon.instance.node();
    }

    public ClusterService clusterService() {
        return ElassandraDaemon.instance.node().injector().getInstance(ClusterService.class);
    }

    public UntypedResultSet process(ConsistencyLevel cl, String query) throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        return clusterService().process(cl, query);
    }

    public UntypedResultSet process(ConsistencyLevel cl, ClientState clientState, String query) throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        return clusterService().process(cl, clientState, query);
    }

    public UntypedResultSet process(ConsistencyLevel cl, String query, Object... values) throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        return clusterService().process(cl, query, values);
    }

    public UntypedResultSet process(ConsistencyLevel cl, ClientState clientState, String query, Object... values) throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        return clusterService().process(cl, clientState, query, values);
    }

    // wait for cassandra to rebuild indices on compaction manager threads.
    public boolean waitIndexRebuilt(String keyspace, List<String> types, long timeout) throws InterruptedException {
        for(int i = 0; i < timeout; i+=200) {
            if (types.stream().filter(t -> !SystemKeyspace.isIndexBuilt(keyspace, String.format(Locale.ROOT, "elastic_%s_idx", t))).count() == 0)
               return true;
            Thread.sleep(200);
        }
        return false;
    }

    public XContentBuilder discoverMapping(String type) throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject(type).field("discover", ".*").endObject().endObject();
    }

    /**
     * Get an instance for a particular class using the injector of the singleton node.
     */
    protected <T> T getInstanceFromNode(Class<T> clazz) {
        return ElassandraDaemon.instance.node().injector().getInstance(clazz);
    }

    /**
     * Create a new index on the singleton node with empty index settings.
     */
    protected IndexService createIndex(String index) {
        return createIndex(index, Settings.EMPTY);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected IndexService createIndex(String index, Settings settings) {
        return createIndex(index, settings, null, (XContentBuilder) null);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected IndexService createIndex(String index, Settings settings, String type, XContentBuilder mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null && mappings != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected IndexService createIndex(String index, Settings settings, String type, Object... mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    protected IndexService createIndex(String index, CreateIndexRequestBuilder createIndexRequestBuilder) {
        assertAcked(createIndexRequestBuilder.get());
        // Wait for the index to be allocated so that cluster state updates don't override
        // changes that would have been done locally
        ClusterHealthResponse health = client().admin().cluster()
                .health(Requests.clusterHealthRequest(index).waitForYellowStatus().waitForEvents(Priority.LANGUID)
                        .waitForNoRelocatingShards(true)).actionGet();
        assertThat(health.getStatus(), lessThanOrEqualTo(ClusterHealthStatus.YELLOW));
        assertThat("Cluster must be a single node cluster", health.getNumberOfDataNodes(), equalTo(1));
        IndicesService instanceFromNode = getInstanceFromNode(IndicesService.class);
        return instanceFromNode.indexServiceSafe(resolveIndex(index));
    }

    protected static org.opensearch.index.engine.Engine engine(IndexService service) {
        return service.getShard(0).getEngine();
    }

    public Index resolveIndex(String index) {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetadata.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    /**
     * Create a new search context.
     */
    protected SearchContext createSearchContext(IndexService indexService) {
        BigArrays bigArrays = indexService.getBigArrays();
        return new TestSearchContext(bigArrays, indexService);
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     */
    public ClusterHealthStatus ensureGreen(String... indices) {
        return ensureGreen(TimeValue.timeValueSeconds(60), indices);
    }


    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     *
     * @param timeout time out value to set on {@link org.opensearch.action.admin.cluster.health.ClusterHealthRequest}
     */
    public ClusterHealthStatus ensureGreen(TimeValue timeout, String... indices) {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest(indices).timeout(timeout).waitForGreenStatus().waitForEvents(Priority.LANGUID)
                        .waitForNoRelocatingShards(true)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        logger.debug("indices {} are green", indices.length == 0 ? "[_all]" : indices);
        return actionGet.getStatus();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return getInstanceFromNode(NamedXContentRegistry.class);
    }

    protected boolean forbidPrivateIndexSettings() {
        return true;
    }

}
