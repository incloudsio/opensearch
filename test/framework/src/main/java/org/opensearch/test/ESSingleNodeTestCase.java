/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Elassandra: single-node test base (ported from Elasticsearch 6.8 fork). Starts Cassandra + embedded
 * OpenSearch via {@link org.apache.cassandra.service.ElassandraDaemon}.
 */

package org.opensearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ElassandraDaemon;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.client.Requests;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
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
import org.opensearch.test.NodeRoles;
import org.opensearch.node.NodeValidationException;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.ScriptService;
import org.opensearch.search.MockSearchService;
import org.opensearch.search.internal.SearchContext;
import org.elassandra.discovery.MockCassandraDiscovery;
import org.opensearch.test.store.MockFSIndexStore;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Elassandra single-node tests: one shared daemon/node for the JVM (same idea as the legacy ES 6.8 class).
 */
public abstract class ESSingleNodeTestCase extends OpenSearchTestCase {

    private static final Semaphore testMutex = new Semaphore(1);

    public static synchronized void initElassandraDeamon(Settings testSettings, Collection<Class<? extends Plugin>> classpathPlugins) {
        if (ElassandraDaemon.instance == null) {
            DatabaseDescriptor.daemonInitialization();
            DatabaseDescriptor.createAllDirectories();

            CountDownLatch startLatch = new CountDownLatch(1);
            Path confPath = Paths.get(System.getProperty("cassandra.config.dir", "."));
            Environment daemonEnv = InternalSettingsPreparer.prepareEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), System.getProperty("cassandra.home")).build(),
                Collections.emptyMap(),
                confPath,
                () -> "node0"
            );
            ElassandraDaemon.instance = new ElassandraDaemon(daemonEnv) {
                @Override
                public Settings nodeSettings(Settings settings) {
                    return Settings.builder()
                        .put("discovery.type", MockCassandraDiscovery.MOCK_CASSANDRA)
                        .put(Environment.PATH_HOME_SETTING.getKey(), System.getProperty("cassandra.home"))
                        .put(Environment.PATH_DATA_SETTING.getKey(), DatabaseDescriptor.getAllDataFileLocations()[0] + File.separatorChar + "elasticsearch.data")
                        .put(Environment.PATH_REPO_SETTING.getKey(), System.getProperty("cassandra.home") + "/repo")
                        .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), DatabaseDescriptor.getAllDataFileLocations()[0] + File.separatorChar + "elasticsearch.data")
                        .put("transport.type", getTestTransportType())
                        .put(NodeRoles.dataNode())
                        .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
                        .put("node.name", "127.0.0.1")
                        .put(ScriptService.SCRIPT_DISABLE_MAX_COMPILATIONS_RATE_SETTING.getKey(), true)
                        .put("client.type", "node")
                        .put(settings)
                        .build();
                }

                @Override
                public void ringReady() {
                    startLatch.countDown();
                }
            };

            Settings elassandraSettings = ElassandraDaemon.instance.nodeSettings(testSettings);
            ElassandraDaemon.instance.activate(false, false, elassandraSettings, new Environment(elassandraSettings, confPath), classpathPlugins);

            try {
                startLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public ESSingleNodeTestCase() {
        super();
        initElassandraDeamon(nodeSettings(1), getPlugins());
    }

    protected boolean addMockTransportService() {
        return false;
    }

    public static final String TESTS_ENABLE_MOCK_MODULES = "tests.enable_mock_modules";
    private static final boolean MOCK_MODULES_ENABLED = "true".equals(System.getProperty(TESTS_ENABLE_MOCK_MODULES, "true"));

    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> mocks = new ArrayList<>();
        if (MOCK_MODULES_ENABLED && randomBoolean()) {
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
            if (randomBoolean()) {
                mocks.add(MockFieldFilterPlugin.class);
            }
        }

        if (addMockTransportService()) {
            mocks.add(getTestTransportPlugin());
        }

        mocks.add(OpenSearchIntegTestCase.TestSeedPlugin.class);
        mocks.add(MockCassandraDiscovery.TestPlugin.class);
        return Collections.unmodifiableList(mocks);
    }

    public MockCassandraDiscovery getMockCassandraDiscovery() {
        return (MockCassandraDiscovery) clusterService().getCassandraDiscovery();
    }

    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.EMPTY;
    }

    static void reset() {}

    static void cleanup(boolean resetNode) {
        if (ElassandraDaemon.instance != null && ElassandraDaemon.instance.node() != null) {
            DeleteIndexRequestBuilder builder = ElassandraDaemon.instance.node()
                .client()
                .admin()
                .indices()
                .prepareDelete("*")
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
            assertAcked(builder.get());
            if (resetNode) {
                reset();
            }
        }
    }

    public static String encodeBasicHeader(final String username, final String password) {
        return java.util.Base64.getEncoder()
            .encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8));
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
            node.start();
        } catch (NodeValidationException e) {
            throw new RuntimeException(e);
        }
        closeAfterTest(node.getNodeEnvironment());
        return node;
    }

    protected void startNode(long seed) throws Exception {
        ElassandraDaemon.instance.node(RandomizedContext.current().runWithPrivateRandomness(seed, this::newNode));
        ClusterAdminClient clusterAdminClient = client().admin().cluster();
        ClusterHealthRequestBuilder builder = clusterAdminClient.prepareHealth();
        ClusterHealthResponse clusterHealthResponse = builder.setWaitForGreenStatus().get();

        assertFalse(clusterHealthResponse.isTimedOut());
    }

    private static void stopNode() throws IOException {
        if (ElassandraDaemon.instance == null) {
            return;
        }
        Node node = ElassandraDaemon.instance.node();
        if (node != null) {
            node.close();
        }
        ElassandraDaemon.instance.node(null);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        logger.info("[{}#{}]: acquiring semaphore ={}", getTestClass().getSimpleName(), getTestName(), testMutex.toString());
        testMutex.acquireUninterruptibly();
        super.setUp();
        long seed = random().nextLong();
        if (ElassandraDaemon.instance.node() == null) {
            startNode(seed);
        }
    }

    @After
    @Override
    public void tearDown() throws Exception {
        logger.info("[{}#{}]: cleaning up after test", getTestClass().getSimpleName(), getTestName());
        super.tearDown();
        try {
            DeleteIndexRequestBuilder builder = ElassandraDaemon.instance.node()
                .client()
                .admin()
                .indices()
                .prepareDelete("*")
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
            assertAcked(builder.get());

            Metadata metadata = client().admin().cluster().prepareState().get().getState().metadata();
            assertThat(
                "test leaves persistent cluster metadata behind: " + metadata.persistentSettings().keySet(),
                metadata.persistentSettings().size(),
                equalTo(0)
            );
            assertThat(
                "test leaves transient cluster metadata behind: " + metadata.transientSettings().keySet(),
                metadata.transientSettings().size(),
                equalTo(0)
            );

            List<String> userKeyspaces = new ArrayList<>(Schema.instance.getUserKeyspaces());
            userKeyspaces.remove(this.clusterService().getElasticAdminKeyspaceName());
            assertThat("test leaves a user keyspace behind:" + userKeyspaces, userKeyspaces.size(), equalTo(0));
        } catch (Exception e) {
            logger.warn("[{}#{}]: failed to clean indices and metadata: error=" + e, getTestClass().getSimpleName(), getTestName());
            logger.warn("Exception:", e);
        } finally {
            testMutex.release();
            logger.info("[{}#{}]: released semaphore={}", getTestClass().getSimpleName(), getTestName(), testMutex.toString());
        }
        if (resetNodeAfterTest()) {
            assert ElassandraDaemon.instance != null;
            stopNode();
            startNode(random().nextLong());
        }
    }

    @BeforeClass
    public static synchronized void setUpClass() throws Exception {}

    @AfterClass
    public static void tearDownClass() throws IOException {
        stopNode();
    }

    protected boolean resetNodeAfterTest() {
        return false;
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.emptyList();
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    protected final Collection<Class<? extends Plugin>> pluginList(Class<? extends Plugin>... plugins) {
        return Arrays.asList(plugins);
    }

    protected Settings nodeSettings() {
        return Settings.EMPTY;
    }

    public Client client() {
        return ElassandraDaemon.instance.node().client();
    }

    protected Node node() {
        return ElassandraDaemon.instance.node();
    }

    public ClusterService clusterService() {
        return ElassandraDaemon.instance.node().injector().getInstance(ClusterService.class);
    }

    public UntypedResultSet process(ConsistencyLevel cl, String query) throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        return clusterService().process(cl, query);
    }

    public UntypedResultSet process(ConsistencyLevel cl, ClientState clientState, String query) throws RequestExecutionException,
        RequestValidationException,
        InvalidRequestException {
        return clusterService().process(cl, clientState, query);
    }

    public UntypedResultSet process(ConsistencyLevel cl, String query, Object... values) throws RequestExecutionException,
        RequestValidationException,
        InvalidRequestException {
        return clusterService().process(cl, query, values);
    }

    public UntypedResultSet process(ConsistencyLevel cl, ClientState clientState, String query, Object... values) throws RequestExecutionException,
        RequestValidationException,
        InvalidRequestException {
        return clusterService().process(cl, clientState, query, values);
    }

    public boolean waitIndexRebuilt(String keyspace, List<String> types, long timeout) throws InterruptedException {
        for (int i = 0; i < timeout; i += 200) {
            if (types.stream().filter(t -> !SystemKeyspace.isIndexBuilt(keyspace, String.format(Locale.ROOT, "elastic_%s_idx", t))).count() == 0) {
                return true;
            }
            Thread.sleep(200);
        }
        return false;
    }

    public XContentBuilder discoverMapping(String type) throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject(type).field("discover", ".*").endObject().endObject();
    }

    protected <T> T getInstanceFromNode(Class<T> clazz) {
        return ElassandraDaemon.instance.node().injector().getInstance(clazz);
    }

    protected IndexService createIndex(String index) {
        return createIndex(index, Settings.EMPTY);
    }

    protected IndexService createIndex(String index, Settings settings) {
        return createIndex(index, settings, null, (XContentBuilder) null);
    }

    protected IndexService createIndex(String index, Settings settings, String type, XContentBuilder mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null && mappings != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    protected IndexService createIndex(String index, Settings settings, String type, Object... mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    protected IndexService createIndex(String index, CreateIndexRequestBuilder createIndexRequestBuilder) {
        assertAcked(createIndexRequestBuilder.get());
        ClusterHealthResponse health = client().admin()
            .cluster()
            .health(
                Requests.clusterHealthRequest(index).waitForYellowStatus().waitForEvents(Priority.LANGUID).waitForNoRelocatingShards(true)
            )
            .actionGet();
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

    protected SearchContext createSearchContext(IndexService indexService) {
        BigArrays bigArrays = indexService.getBigArrays();
        return new TestSearchContext(bigArrays, indexService);
    }

    public ClusterHealthStatus ensureGreen(String... indices) {
        return ensureGreen(TimeValue.timeValueSeconds(60), indices);
    }

    public ClusterHealthStatus ensureGreen(TimeValue timeout, String... indices) {
        ClusterHealthResponse actionGet = client().admin()
            .cluster()
            .health(
                Requests.clusterHealthRequest(indices)
                    .timeout(timeout)
                    .waitForGreenStatus()
                    .waitForEvents(Priority.LANGUID)
                    .waitForNoRelocatingShards(true)
            )
            .actionGet();
        if (actionGet.isTimedOut()) {
            logger.info(
                "ensureGreen timed out, cluster state:\n{}\n{}",
                client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get()
            );
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
