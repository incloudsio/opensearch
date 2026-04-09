/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.service;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.LocalNodeMasterListener;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.node.Node;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClusterService extends AbstractLifecycleComponent {
    private final MasterService masterService;

    private final ClusterApplierService clusterApplierService;

    public static final org.opensearch.common.settings.Setting.AffixSetting<String> USER_DEFINED_METADATA = Setting.prefixKeySetting(
        "cluster.metadata.",
        (key) -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope)
    );

    /** Elassandra: secondary index class setting key (fork parity; side-car stub). */
    public static final String SETTING_CLUSTER_SECONDARY_INDEX_CLASS = "cluster.secondary_index_class";

    /** Elassandra: cluster-wide search strategy class (fork parity; matches ES fork {@code cluster.search_strategy_class}). */
    public static final String SETTING_CLUSTER_SEARCH_STRATEGY_CLASS = "cluster.search_strategy_class";

    public static final Class<?> defaultSecondaryIndexClass = org.elassandra.index.ExtendedElasticSecondaryIndex.class;

    /**
     * The node's settings.
     */
    private final Settings settings;

    private final ClusterName clusterName;

    private final OperationRouting operationRouting;

    private final ClusterSettings clusterSettings;

    private final String nodeName;

    private RerouteService rerouteService;

    private IndexingPressureService indexingPressureService;

    public ClusterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this(
            settings,
            clusterSettings,
            new MasterService(settings, clusterSettings, threadPool),
            new ClusterApplierService(Node.NODE_NAME_SETTING.get(settings), settings, clusterSettings, threadPool)
        );
    }

    public ClusterService(
        Settings settings,
        ClusterSettings clusterSettings,
        MasterService masterService,
        ClusterApplierService clusterApplierService
    ) {
        this.settings = settings;
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.masterService = masterService;
        this.operationRouting = new OperationRouting(settings, clusterSettings);
        this.clusterSettings = clusterSettings;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        // Add a no-op update consumer so changes are logged
        this.clusterSettings.addAffixUpdateConsumer(USER_DEFINED_METADATA, (first, second) -> {}, (first, second) -> {});
        this.clusterApplierService = clusterApplierService;
    }

    public synchronized void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {
        clusterApplierService.setNodeConnectionsService(nodeConnectionsService);
    }

    public void setRerouteService(RerouteService rerouteService) {
        assert this.rerouteService == null : "RerouteService is already set";
        this.rerouteService = rerouteService;
    }

    public RerouteService getRerouteService() {
        assert this.rerouteService != null : "RerouteService not set";
        return rerouteService;
    }

    @Override
    protected synchronized void doStart() {
        clusterApplierService.start();
        masterService.start();
    }

    @Override
    protected synchronized void doStop() {
        masterService.stop();
        clusterApplierService.stop();
    }

    @Override
    protected synchronized void doClose() {
        masterService.close();
        clusterApplierService.close();
    }

    /**
     * The local node.
     */
    public DiscoveryNode localNode() {
        DiscoveryNode localNode = state().getNodes().getLocalNode();
        if (localNode == null) {
            throw new IllegalStateException("No local node found. Is the node started?");
        }
        return localNode;
    }

    public OperationRouting operationRouting() {
        return operationRouting;
    }

    /**
     * The currently applied cluster state.
     * TODO: Should be renamed to appliedState / appliedClusterState
     */
    public ClusterState state() {
        return clusterApplierService.state();
    }

    /**
     * Adds a high priority applier of updated cluster states.
     */
    public void addHighPriorityApplier(ClusterStateApplier applier) {
        clusterApplierService.addHighPriorityApplier(applier);
    }

    /**
     * Adds an applier which will be called after all high priority and normal appliers have been called.
     */
    public void addLowPriorityApplier(ClusterStateApplier applier) {
        clusterApplierService.addLowPriorityApplier(applier);
    }

    /**
     * Adds a applier of updated cluster states.
     */
    public void addStateApplier(ClusterStateApplier applier) {
        clusterApplierService.addStateApplier(applier);
    }

    /**
     * Removes an applier of updated cluster states.
     */
    public void removeApplier(ClusterStateApplier applier) {
        clusterApplierService.removeApplier(applier);
    }

    /**
     * Add a listener for updated cluster states
     */
    public void addListener(ClusterStateListener listener) {
        clusterApplierService.addListener(listener);
    }

    /**
     * Removes a listener for updated cluster states.
     */
    public void removeListener(ClusterStateListener listener) {
        clusterApplierService.removeListener(listener);
    }

    /**
     * Add a listener for on/off local node master events
     */
    public void addLocalNodeMasterListener(LocalNodeMasterListener listener) {
        clusterApplierService.addLocalNodeMasterListener(listener);
    }

    public MasterService getMasterService() {
        return masterService;
    }

    /**
     * Getter and Setter for IndexingPressureService, This method exposes IndexingPressureService stats to other plugins for usage.
     * Although Indexing Pressure instances can be accessed via Node and NodeService class but none of them are
     * present in the createComponents signature of Plugin interface currently. {@link org.opensearch.plugins.Plugin#createComponents}
     * Going forward, IndexingPressureService will have required constructs for exposing listeners/interfaces for plugin development.(#478)
     */
    public void setIndexingPressureService(IndexingPressureService indexingPressureService) {
        this.indexingPressureService = indexingPressureService;
    }

    public IndexingPressureService getIndexingPressureService() {
        return indexingPressureService;
    }

    public ClusterApplierService getClusterApplierService() {
        return clusterApplierService;
    }

    public static boolean assertClusterOrMasterStateThread() {
        assert Thread.currentThread().getName().contains(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME)
            || Thread.currentThread().getName().contains(MasterService.MASTER_UPDATE_THREAD_NAME)
            : "not called from the master/cluster state update thread";
        return true;
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    /**
     * The node's settings.
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * The name of this node.
     */
    public final String getNodeName() {
        return nodeName;
    }

    /**
     * Submits a cluster state update task; unlike {@link #submitStateUpdateTask(String, Object, ClusterStateTaskConfig,
     * ClusterStateTaskExecutor, ClusterStateTaskListener)}, submitted updates will not be batched.
     *
     * @param source     the source of the cluster state update task
     * @param updateTask the full context for the cluster state update
     *                   task
     *
     */
    public <T extends ClusterStateTaskConfig & ClusterStateTaskExecutor<T> & ClusterStateTaskListener> void submitStateUpdateTask(
        String source,
        T updateTask
    ) {
        submitStateUpdateTask(source, updateTask, updateTask, updateTask, updateTask);
    }

    /**
     * Submits a cluster state update task; submitted updates will be
     * batched across the same instance of executor. The exact batching
     * semantics depend on the underlying implementation but a rough
     * guideline is that if the update task is submitted while there
     * are pending update tasks for the same executor, these update
     * tasks will all be executed on the executor in a single batch
     *
     * @param source   the source of the cluster state update task
     * @param task     the state needed for the cluster state update task
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param listener callback after the cluster state update task
     *                 completes
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTask(
        String source,
        T task,
        ClusterStateTaskConfig config,
        ClusterStateTaskExecutor<T> executor,
        ClusterStateTaskListener listener
    ) {
        submitStateUpdateTasks(source, Collections.singletonMap(task, listener), config, executor);
    }

    /**
     * Submits a batch of cluster state update tasks; submitted updates are guaranteed to be processed together,
     * potentially with more tasks of the same executor.
     *
     * @param source   the source of the cluster state update task
     * @param tasks    a map of update tasks and their corresponding listeners
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTasks(
        final String source,
        final Map<T, ClusterStateTaskListener> tasks,
        final ClusterStateTaskConfig config,
        final ClusterStateTaskExecutor<T> executor
    ) {
        masterService.submitStateUpdateTasks(source, tasks, config, executor);
    }

    // --- Elassandra side-car compile stubs (no runtime behaviour; full port replaces these) ---

    public String getExtensionKey(org.opensearch.cluster.metadata.IndexMetadata indexMetaData) {
        return indexMetaData.getIndex().getName();
    }

    public void putIndexMetaDataExtension(
        org.opensearch.cluster.metadata.IndexMetadata indexMetaData,
        java.util.Map<String, java.nio.ByteBuffer> extensions
    ) {
    }

    public void setDiscovery(org.opensearch.discovery.Discovery discovery) {
    }

    public org.opensearch.indices.IndicesService getIndicesService() {
        return null;
    }

    public org.opensearch.index.IndexService indexServiceSafe(org.opensearch.index.Index index) {
        return null;
    }

    public org.elassandra.cluster.SchemaManager getSchemaManager() {
        return null;
    }

    public void writeMetadataToSchemaMutations(
        org.opensearch.cluster.metadata.Metadata metadata,
        java.util.Collection<org.apache.cassandra.db.Mutation> mutations,
        java.util.Collection<org.apache.cassandra.transport.Event.SchemaChange> events
    ) throws org.apache.cassandra.exceptions.ConfigurationException, java.io.IOException {
    }

    public void commitMetaData(
        org.opensearch.cluster.metadata.Metadata oldMetaData,
        org.opensearch.cluster.metadata.Metadata newMetaData,
        String source
    ) throws org.elassandra.ConcurrentMetaDataUpdateException, org.apache.cassandra.exceptions.UnavailableException, java.io.IOException {
    }

    public java.util.UUID readMetaDataOwner(long version) {
        return null;
    }

    /** Elassandra admin keyspace name (CQL); side-car stub. */
    public String getElasticAdminKeyspaceName() {
        return "elastic_admin";
    }

    /** Elassandra admin metadata CQL table name (matches ES fork {@code ELASTIC_ADMIN_METADATA_TABLE}). */
    public static final String ELASTIC_ADMIN_METADATA_TABLE = "metadata_log";

    /**
     * Merge per-table index metadata extensions into the cluster metadata builder (Elassandra fork parity).
     */
    public org.opensearch.cluster.metadata.Metadata.Builder mergeIndexMetaData(
        org.opensearch.cluster.metadata.Metadata.Builder metaDataBuilder,
        String indexName,
        List<org.opensearch.cluster.metadata.IndexMetadata> mappings
    ) {
        if (mappings == null || mappings.isEmpty()) {
            return metaDataBuilder;
        }
        org.opensearch.cluster.metadata.IndexMetadata base = metaDataBuilder.get(indexName);
        org.opensearch.cluster.metadata.IndexMetadata.Builder indexBuilder = base != null
            ? org.opensearch.cluster.metadata.IndexMetadata.builder(base)
            : org.opensearch.cluster.metadata.IndexMetadata.builder(mappings.get(0));
        int start = base == null ? 1 : 0;
        for (int i = start; i < mappings.size(); i++) {
            org.opensearch.cluster.metadata.IndexMetadata im = mappings.get(i);
            for (ObjectObjectCursor<String, org.opensearch.cluster.metadata.MappingMetadata> c : im.getMappings()) {
                indexBuilder.putMapping(c.value);
            }
        }
        return metaDataBuilder.put(indexBuilder);
    }

    /** Apply CQL table extensions to metadata (fork parity; side-car no-op merge). */
    public org.opensearch.cluster.metadata.Metadata.Builder mergeWithTableExtensions(
        org.opensearch.cluster.metadata.Metadata.Builder metaDataBuilder
    ) {
        return metaDataBuilder;
    }

    /** Virtual index mapping overlay (fork parity; side-car passthrough). */
    public org.opensearch.cluster.metadata.Metadata addVirtualIndexMappings(org.opensearch.cluster.metadata.Metadata metadata) {
        return metadata;
    }

    /** Read persisted metadata from elastic_admin table (fork parity; side-car returns empty). */
    public org.opensearch.cluster.metadata.Metadata readMetaData(org.apache.cassandra.schema.TableMetadata cfm) {
        return org.opensearch.cluster.metadata.Metadata.EMPTY_METADATA;
    }

    /** Submit async shard/replica update after keyspace RF change (fork parity; side-car stub). */
    public void submitNumberOfShardsAndReplicasUpdate(String source, String ksName) {}

    /** CQL extension key naming {@code <elastic_admin_ks>(_<dc>)?/<index>}. */
    public boolean isValidExtensionKey(String extensionName) {
        return extensionName != null && extensionName.contains("/");
    }

    /** Deserialize index metadata from a table extension cell (fork parity; minimal stub). */
    public org.opensearch.cluster.metadata.IndexMetadata getIndexMetaDataFromExtension(java.nio.ByteBuffer value) {
        return org.opensearch.cluster.metadata.IndexMetadata.builder("__extension__").numberOfShards(1).numberOfReplicas(0).build();
    }

    /** Build secondary index name from a CQL table name (fork parity). */
    public static String buildIndexName(String cfName) {
        return "elastic_" + cfName;
    }

    private static final java.util.regex.Pattern INDEX_TO_NAME_PATTERN = java.util.regex.Pattern.compile("\\.|\\-");

    public static String indexToKsName(String index) {
        return INDEX_TO_NAME_PATTERN.matcher(index).replaceAll("_");
    }

    /** Elassandra replication factor helper (CQL keyspace name). */
    public static int replicationFactor(String keyspace) {
        return 1;
    }

    public static final String SETTING_SYSTEM_SYNCHRONOUS_REFRESH = "es.synchronous_refresh";
    public static final String SYNCHRONOUS_REFRESH = "synchronous_refresh";
    public static final String SNAPSHOT_WITH_SSTABLE = "snapshot_with_sstable";
    public static final String INCLUDE_HOST_ID = "include_node_id";
    public static final String INDEX_ON_COMPACTION = "index_on_compaction";
    public static final String INDEX_STATIC_COLUMNS = "index_static_columns";
    public static final String INDEX_STATIC_ONLY = "index_static_only";
    public static final String INDEX_STATIC_DOCUMENT = "index_static_document";
    public static final String INDEX_INSERT_ONLY = "index_insert_only";
    public static final String INDEX_OPAQUE_STORAGE = "index_opaque_storage";
    public static final String SETTING_SYSTEM_SNAPSHOT_WITH_SSTABLE = "es.snapshot_with_sstable";
    public static final String SETTING_SYSTEM_INDEX_ON_COMPACTION = "es.index_on_compaction";
    public static final String SETTING_SYSTEM_INDEX_INSERT_ONLY = "es.index_insert_only";
    public static final String SETTING_SYSTEM_INDEX_OPAQUE_STORAGE = "es.index_opaque_storage";
    public static final String SETTING_SYSTEM_TOKEN_RANGES_QUERY_EXPIRE = "es.token_ranges_query_expire_minutes";

    /** Elassandra: publish local shard routing into gossip-related path (side-car stub). */
    public void publishShardRoutingState(String indexName, org.opensearch.cluster.routing.ShardRoutingState state) {
    }

    /** Elassandra: internal cluster-state publish hook (side-car stub). */
    public void publishX1() {
    }

    /** Elassandra: search routing refresh (side-car stub). */
    public org.elassandra.cluster.routing.PrimaryFirstSearchStrategy.PrimaryFirstRouter updateRouter(
        org.opensearch.cluster.metadata.IndexMetadata indexMetadata,
        org.opensearch.cluster.ClusterState state
    ) {
        return null;
    }

    /** Elassandra: current search strategy router (side-car stub). */
    public org.elassandra.cluster.routing.AbstractSearchStrategy.Router getRouter(
        org.opensearch.cluster.metadata.IndexMetadata indexMetadata,
        org.opensearch.cluster.ClusterState state
    ) {
        return null;
    }

    /**
     * Elassandra: CQL process (fork parity). Delegates to Cassandra {@code QueryProcessor} for side-car compile.
     */
    public org.apache.cassandra.cql3.UntypedResultSet process(
        org.apache.cassandra.db.ConsistencyLevel cl,
        String query,
        Object... values
    ) throws org.apache.cassandra.exceptions.RequestExecutionException,
             org.apache.cassandra.exceptions.RequestValidationException,
             org.apache.cassandra.exceptions.InvalidRequestException {
        return org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal(query, values);
    }

    /** Elassandra: CQL with explicit {@link org.apache.cassandra.service.ClientState} (fork parity). */
    public org.apache.cassandra.cql3.UntypedResultSet process(
        org.apache.cassandra.db.ConsistencyLevel cl,
        org.apache.cassandra.service.ClientState clientState,
        String query
    ) throws org.apache.cassandra.exceptions.RequestExecutionException,
             org.apache.cassandra.exceptions.RequestValidationException,
             org.apache.cassandra.exceptions.InvalidRequestException {
        return process(cl, query);
    }

    /** Elassandra: CQL with client state and bound values (fork parity). */
    public org.apache.cassandra.cql3.UntypedResultSet process(
        org.apache.cassandra.db.ConsistencyLevel cl,
        org.apache.cassandra.service.ClientState clientState,
        String query,
        Object... values
    ) throws org.apache.cassandra.exceptions.RequestExecutionException,
             org.apache.cassandra.exceptions.RequestValidationException,
             org.apache.cassandra.exceptions.InvalidRequestException {
        return process(cl, query, values);
    }

    /** Elassandra: access Cassandra discovery from tests (fork parity; side-car may return null). */
    public org.elassandra.discovery.CassandraDiscovery getCassandraDiscovery() {
        return null;
    }

    /**
     * Elassandra: lightweight transaction / CAS write (fork parity; side-car executes as unconditional write).
     */
    public boolean processWriteConditional(
        org.apache.cassandra.db.ConsistencyLevel cl,
        org.apache.cassandra.db.ConsistencyLevel serialCl,
        String query,
        Object... values
    ) throws org.apache.cassandra.exceptions.RequestExecutionException,
             org.apache.cassandra.exceptions.RequestValidationException,
             org.apache.cassandra.exceptions.InvalidRequestException {
        process(cl, query, values);
        return true;
    }

}
