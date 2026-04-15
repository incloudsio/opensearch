/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
 * Contains some code from Elasticsearch (http://www.elastic.co)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elassandra.discovery;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.net.InetAddresses;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elassandra.ConcurrentMetaDataUpdateException;
import org.elassandra.PaxosMetaDataUpdateException;
import org.elassandra.gateway.CassandraGatewayService;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.client.transport.NoNodeAvailableException;
import org.opensearch.cluster.*;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.service.MasterService;
import org.opensearch.common.Priority;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.discovery.AckClusterStatePublishResponseHandler;
import org.opensearch.discovery.Discovery;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.discovery.DiscoverySettings;
import org.opensearch.cluster.coordination.NoMasterBlockService;
import org.opensearch.discovery.DiscoveryStats;
import org.opensearch.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.service.GZipStringCompressor.compress;
import static org.apache.cassandra.service.GZipStringCompressor.uncompressIfGZipped;
import static org.opensearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

/**
 * https://www.elastic.co/guide/en/elasticsearch/reference/6.3/modules-discovery-zen.html
 *
 * Discover the cluster topology from cassandra snitch and settings, mappings, blocks from the elastic_admin keyspace.
 * Publishing is just a notification to refresh in memory configuration from the cassandra table.
 * @author vroyer
 *
 */
public class CassandraDiscovery extends AbstractLifecycleComponent implements Discovery, IEndpointStateChangeSubscriber, AppliedClusterStateAction.AppliedClusterStateListener {
    final Logger logger = LogManager.getLogger(CassandraDiscovery.class);

    private static final ImmutableSet<DiscoveryNodeRole> CASSANDRA_ROLES = ImmutableSet.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE);
    private final TransportService transportService;

    private final Settings settings;
    private final MasterService masterService;
    private final ClusterService clusterService;
    private final ClusterSettings clusterSettings;
    private final ClusterApplier clusterApplier;
    private final AtomicReference<ClusterState> committedState; // last committed cluster state

    private final ClusterName clusterName;
    private final DiscoverySettings discoverySettings;
    private final NamedWriteableRegistry namedWriteableRegistry;

    private final PendingClusterStatesQueue pendingStatesQueue;
    private final AppliedClusterStateAction appliedClusterStateAction;
    private final AtomicReference<AckClusterStatePublishResponseHandler> handlerRef = new AtomicReference<>();
    private final Object stateMutex = new Object();

    private final GossipCluster gossipCluster;

    private final InetAddressAndPort localAddress;
    private final String localDc;

    private final RoutingTableUpdateTaskExecutor routingTableUpdateTaskExecutor;
    /**
     * When searchEnabled=true, local shards are visible for routing, otherwise, local shards are seen as UNASSIGNED.
     * This allows to gracefully shutdown or start the node for maintenance like an offline repair or rebuild_index.
     */
    private final AtomicBoolean searchEnabled = new AtomicBoolean(false);

    /**
     * Compress the gossip application state X1
     */
    private static final String COMPRESS_X1_SYSTEM_PROP = "es.compress_x1";

    private final boolean gzip = Boolean.parseBoolean(System.getProperty(COMPRESS_X1_SYSTEM_PROP, "false"));

    /**
     * If autoEnableSearch=true, search is automatically enabled when the node becomes ready to operate, otherwise, searchEnabled should be manually set to true.
     */
    private final AtomicBoolean autoEnableSearch = new AtomicBoolean(System.getProperty("es.auto_enable_search") == null || Boolean.getBoolean("es.auto_enable_search"));

    /**
     * Same logic as Elassandra's {@code IndexMetadata#keyspace()}; stock {@code IndexMetadata} has no such helper.
     */
    private static String keyspaceForIndex(IndexMetadata indexMetaData) {
        String ks = indexMetaData.getSettings().get("index.keyspace");
        if (ks != null) {
            return ks;
        }
        return ClusterService.indexToKsName(indexMetaData.getIndex().getName());
    }

    public static final Setting<Integer> MAX_PENDING_CLUSTER_STATES_SETTING =
            Setting.intSetting("discovery.cassandra.publish.max_pending_cluster_states", 1024, 1, Property.NodeScope);

    public CassandraDiscovery(final Settings settings,
                              final TransportService transportService,
                              final MasterService masterService,
                              final ClusterService clusterService,
                              final ClusterApplier clusterApplier,
                              final ClusterSettings clusterSettings,
                              final NamedWriteableRegistry namedWriteableRegistry) {
        super();
        this.settings = settings;
        this.masterService = masterService;
        this.clusterApplier = clusterApplier;
        this.clusterService = clusterService;
        this.clusterSettings = clusterSettings;
        this.discoverySettings = new DiscoverySettings(settings, clusterSettings);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.transportService = transportService;
        this.clusterName = clusterService.getClusterName();

        this.committedState = new AtomicReference<>();
        this.clusterService.setDiscovery(this);

        this.masterService.setClusterStateSupplier(() -> committedState.get());
        this.masterService.setClusterStatePublisher(this::publish);

        this.localAddress = FBUtilities.getBroadcastAddressAndPort();
        this.localDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(this.localAddress);

        this.gossipCluster = new GossipCluster();
        this.pendingStatesQueue = new PendingClusterStatesQueue(logger, MAX_PENDING_CLUSTER_STATES_SETTING.get(settings));
        this.appliedClusterStateAction = new AppliedClusterStateAction(settings, transportService, this, discoverySettings);
        this.routingTableUpdateTaskExecutor = new RoutingTableUpdateTaskExecutor();
    }

    public class GossipNode {
        boolean removed = false;
        DiscoveryNode discoveryNode;
        Map<String,ShardRoutingState> shardRoutingStateMap;
        /** Gossip-derived status; kept here so {@link DiscoveryNode} can stay stock (e.g. OpenSearch side-car). */
        ElassandraGossipNodeStatus gossipStatus;

        public GossipNode(DiscoveryNode discoveryNode, Map<String,ShardRoutingState> shardRoutingStateMap, ElassandraGossipNodeStatus gossipStatus) {
            this.discoveryNode = discoveryNode;
            this.shardRoutingStateMap = shardRoutingStateMap;
            this.gossipStatus = gossipStatus;
        }

        public GossipNode(DiscoveryNode discoveryNode, Map<String,ShardRoutingState> shardRoutingStateMap) {
            this(discoveryNode, shardRoutingStateMap, ElassandraGossipNodeStatus.UNKNOWN);
        }

        public GossipNode(DiscoveryNode discoveryNode) {
            this(discoveryNode, new HashMap<>(), ElassandraGossipNodeStatus.UNKNOWN);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            GossipNode other = (GossipNode) obj;
            return Objects.equals(removed, other.removed) &&
                Objects.equals(discoveryNode, other.discoveryNode) &&
                Objects.equals(this.shardRoutingStateMap, other.shardRoutingStateMap) &&
                Objects.equals(this.gossipStatus, other.gossipStatus);
        }

        @Override
        public int hashCode() {
            return Objects.hash(removed, discoveryNode, shardRoutingStateMap, gossipStatus);
        }
    }

    public class GossipCluster {
        private final ConcurrentMap<UUID, GossipNode> remoteMembers = new ConcurrentHashMap<>();

        public DiscoveryNodes nodes() {
            // Must use the same id as DiscoveryNode#getId() for the local node. SystemKeyspace.getLocalHostId() can
            // diverge from TransportService#getLocalNode() in some embedded / test setups; if localNodeId/masterNodeId
            // do not match the map key in DiscoveryNodes, getMasterNode() returns null and cluster state publish fails.
            DiscoveryNode local = localNode();
            DiscoveryNodes.Builder nodesBuilder = new DiscoveryNodes.Builder()
                .localNodeId(local.getId())
                .masterNodeId(local.getId())
                .add(local);
            for (GossipNode node : remoteMembers.values()) {
                // filter removed nodes, but keep it to avoid detecting them as new nodes.
                if (!node.removed) {
                    nodesBuilder.add(node.discoveryNode);
                }
            }
            return nodesBuilder.build();
        }

        public DiscoveryNode getDiscoveryNode(UUID id) {
            return remoteMembers.containsKey(id) ? remoteMembers.get(id).discoveryNode : null;
        }

        public Map<String,ShardRoutingState> getShardRoutingState(UUID id) {
            return remoteMembers.containsKey(id) ? remoteMembers.get(id).shardRoutingStateMap : null;
        }

        public boolean contains(UUID id) {
            return remoteMembers.containsKey(id);
        }

        public Collection<GossipNode> remoteMembers() {
            return remoteMembers.values();
        }

        public ShardRoutingState getShardRoutingState(UUID nodeUuid, org.opensearch.index.Index index) {
            if (UUID.fromString(localNode().getId()).equals(nodeUuid)) {
                if (isSearchEnabled()) {
                    try {
                        IndexShard localIndexShard = clusterService.indexServiceSafe(index).getShardOrNull(0);
                        if (localIndexShard != null && localIndexShard.routingEntry() != null)
                            return localIndexShard.routingEntry().state();

                        // shardRouting not yet created.
                        return ShardRoutingState.INITIALIZING;
                    } catch (IndexNotFoundException e) {
                    }
                }
                return ShardRoutingState.UNASSIGNED;
            }

            GossipNode gossipNode = remoteMembers.get(nodeUuid);
            if (gossipNode == null)
                return ShardRoutingState.UNASSIGNED;

            ShardRoutingState shardRoutingState = gossipNode.shardRoutingStateMap.get(index.getName());
            return (shardRoutingState == null) ? ShardRoutingState.UNASSIGNED : shardRoutingState;
        }

        public GossipNode remove(final UUID hostId, final String source) {
            GossipNode oldGossipNode = remoteMembers.computeIfPresent(hostId, (k,v) -> {
                v.removed = true;
                return v;
            });
            if (oldGossipNode != null) {
                clusterService.submitStateUpdateTask(source, new RoutingTableUpdateTask(true),
                    routingTableUpdateTaskExecutor, routingTableUpdateTaskExecutor, routingTableUpdateTaskExecutor);
            }
            return oldGossipNode;
        }

        public void update(final InetAddress endpoint, EndpointState epState, String source, boolean allowClusterStateUpdate) {
            if (epState.getApplicationState(ApplicationState.HOST_ID) == null || epState.getApplicationState(ApplicationState.HOST_ID).value == null)
                return;

            UUID hostId = UUID.fromString(epState.getApplicationState(ApplicationState.HOST_ID).value);

            VersionedValue vv = epState.getApplicationState(ApplicationState.STATUS);
            if (vv != null && ("removed".startsWith(vv.value) || "LEFT".startsWith(vv.value))) {
                remove(hostId, "remove-" + endpoint);
                return;
            }

            String x1 = null;
            try {
                x1 = epState.getApplicationState(ApplicationState.X1) == null ? null : uncompressIfGZipped(epState.getApplicationState(ApplicationState.X1).value);
            } catch (IOException e) {
                logger.warn("Decompression of gossip application state X1 failed, use the value as it : {}", e.getMessage(), e);
                x1 = epState.getApplicationState(ApplicationState.X1).value;
            }
            update(hostId, source, endpoint, getInternalIp(epState), getRpcAddress(epState), discoveryNodeStatus(epState), x1,  allowClusterStateUpdate);
        }

        /**
         * Trigger routing table update if node status or x1 changed, or a new ALIVE node appear.
         * Trigger nodes update if node IP or  name changed
         */
        private void update(final UUID hostId,
                                 final String source,
                                 final InetAddress endpoint,
                                 final InetAddress internalIp,
                                 final InetAddress rpcAddress,
                                 final ElassandraGossipNodeStatus status,
                                 final String x1,
                                 boolean allowClusterStateUpdate
        ) {
            if (localNode().getId().equals(hostId.toString())) {
                // ignore GOSSIP update related to our self node.
                logger.debug("Ignoring GOSSIP update for node id={} ip={} because it's mine", hostId, endpoint);
                return;
            } else {
                logger.debug("updating id={} endpoint={} source=[{}] status=[{}] x1=[{}]", hostId, endpoint, source, status, x1);
            }

            final TransportAddress addr = new TransportAddress(Boolean.getBoolean("es.use_internal_address") ? internalIp : rpcAddress, publishPort());
            remoteMembers.compute(hostId, (k,gn) -> {
                boolean nodeUpdate = false;
                boolean routingUpdate = false;
                Map<String, ShardRoutingState> x1Map = new HashMap<>();
                Set<String> updatedIndices = Collections.EMPTY_SET;

                if (x1 != null && status.isAlive()) {
                    try {
                        x1Map = jsonMapper.readValue(x1, indexShardStateTypeReference);
                    } catch (IOException e) {
                        logger.error("Failed to parse X1 for node [{}] x1={}", hostId, x1);
                    }
                }

                if (gn == null) {
                    // new node
                    ImmutableMap.Builder<String, String> attrs =  ImmutableMap.builder();
                    attrs.put("dc", localDc);
                    attrs.put("rack", DatabaseDescriptor.getEndpointSnitch().getRack(InetAddressAndPort.getByAddress(endpoint)));
                    logger.debug("Add node NEW host_id={} endpoint={} internal_ip={}, rpc_address={}, status={}",
                        hostId, NetworkAddress.format(endpoint),
                        internalIp == null ? null : NetworkAddress.format(internalIp),
                        rpcAddress == null ? null : NetworkAddress.format(rpcAddress),
                        status);
                    gn = new GossipNode(new DiscoveryNode(buildNodeName(endpoint), hostId.toString(), addr, attrs.build(), CASSANDRA_ROLES, Version.CURRENT), x1Map, status);
                    nodeUpdate = true;
                    routingUpdate = status.isAlive();
                } else {
                    DiscoveryNode dn = gn.discoveryNode;

                    // status changed
                    if (!gn.gossipStatus.equals(status)) {
                        logger.debug("Update node STATUS host_id={} endpoint={} internal_ip={} rpc_address={}, status={}",
                            hostId, NetworkAddress.format(endpoint),
                            internalIp == null ? null : NetworkAddress.format(internalIp),
                            rpcAddress == null ? null : NetworkAddress.format(rpcAddress),
                            status);
                        gn.gossipStatus = status;
                        nodeUpdate = true;
                        routingUpdate = true;

                        if (!status.isAlive()) {
                            // node probably down, notify metaDataVersionAckListener..
                            notifyHandler(Gossiper.instance.getEndpointStateForEndpoint(InetAddressAndPort.getByAddress(endpoint)));
                        }
                    }

                    // Node name or IP changed
                    if (!dn.getName().equals(buildNodeName(endpoint)) || !dn.getAddress().equals(addr)) {
                        // update DiscoveryNode IP if endpoint is ALIVE
                        if (status.equals(ElassandraGossipNodeStatus.ALIVE)) {
                            logger.debug("Update node IP host_id={} endpoint={} internal_ip={}, rpc_address={}, status={}",
                                hostId, NetworkAddress.format(endpoint),
                                internalIp == null ? null : NetworkAddress.format(internalIp),
                                rpcAddress == null ? null : NetworkAddress.format(rpcAddress),
                                status);
                            gn = new GossipNode(new DiscoveryNode(buildNodeName(endpoint), hostId.toString(), addr, dn.getAttributes(), CASSANDRA_ROLES, Version.CURRENT), gn.shardRoutingStateMap, status);
                            nodeUpdate = true;
                        } else {
                            logger.debug("Ignoring node DEAD host_id={} endpoint={} internal_ip={}, rpc_address={}, status={}",
                                hostId, NetworkAddress.format(endpoint),
                                internalIp == null ? null : NetworkAddress.format(internalIp),
                                rpcAddress == null ? null : NetworkAddress.format(rpcAddress),
                                status);
                        }
                    }

                    // X1 changed
                    if (!gn.shardRoutingStateMap.equals(x1Map)) {
                        routingUpdate = true;
                        if (nodeUpdate == false && !x1Map.isEmpty()) {
                            MapDifference<String, ShardRoutingState> mapDifference = Maps.difference(x1Map, gn.shardRoutingStateMap);
                            if (!mapDifference.entriesDiffering().isEmpty() || !mapDifference.entriesOnlyOnRight().isEmpty()) {
                                updatedIndices = mapDifference.entriesDiffering().keySet();
                                logger.trace("Updating routing table source=[{}] for indices={}", source, updatedIndices);
                            }
                        }
                    }
                    gn.shardRoutingStateMap = x1Map;

                    if (allowClusterStateUpdate && (nodeUpdate || routingUpdate)) {
                        logger.debug("Updating routing table node source=[{}] nodeUpdate={} routingUpdate={}", source, nodeUpdate, routingUpdate);
                        RoutingTableUpdateTask routingTableUpdateTask = (routingUpdate && !nodeUpdate) ?
                            new RoutingTableUpdateTask(true, updatedIndices) :
                            new RoutingTableUpdateTask(routingUpdate);

                        clusterService.submitStateUpdateTask(source, routingTableUpdateTask,
                            routingTableUpdateTaskExecutor, routingTableUpdateTaskExecutor, routingTableUpdateTaskExecutor);
                    }
                }
                return gn;
            });
        }
    }

    public PendingClusterStatesQueue pendingStatesQueue() {
        return this.pendingStatesQueue;
    }

    public static String buildNodeName(InetAddress addr) {
        String hostname = NetworkAddress.format(addr);
        if (hostname != null)
            return hostname;
        return String.format(Locale.getDefault(), "node%03d%03d%03d%03d",
                (int) (addr.getAddress()[0] & 0xFF), (int) (addr.getAddress()[1] & 0xFF),
                (int) (addr.getAddress()[2] & 0xFF), (int) (addr.getAddress()[3] & 0xFF));
    }

    @Override
    protected void doStart()  {
        // OpenSearch Node.start() runs discovery.start() before clusterService.start() so the applier can
        // receive setInitialState(...). Legacy Node.activate() called initClusterState() explicitly; ensure
        // the standard path does too (idempotent when activate() already initialized).
        if (committedState.get() == null) {
            initClusterState(transportService.getLocalNode());
        }
        Gossiper.instance.register(this);
        synchronized (gossipCluster) {
            logger.debug("Connected to cluster [{}]", clusterName.value());
            logger.info("localNode name={} id={} localAddress={} publish_host={}", localNode().getName(), localNode().getId(), localAddress, localNode().getAddress());

            // initialize cluster from cassandra local token map
            for(InetAddressAndPort endpoint : StorageService.instance.getTokenMetadata().getAllEndpoints()) {
                if (!this.localAddress.equals(endpoint) && this.localDc.equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint))) {
                    String hostId = StorageService.instance.getHostIdForEndpoint(endpoint).toString();
                    UntypedResultSet rs = executeInternal("SELECT preferred_ip, rpc_address from system." + SystemKeyspace.LEGACY_PEERS +"  WHERE peer = ?", endpoint.address);
                    if (!rs.isEmpty()) {
                        UntypedResultSet.Row row = rs.one();
                        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                        gossipCluster.update(endpoint.address, epState, "discovery-init", false);
                    }
                }
            }

            // walk the gossip states
            for (InetAddressAndPort endpoint : Gossiper.instance.getEndpoints()) {
                EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                if (epState == null)
                    continue;

                if (!epState.getStatus().equals(VersionedValue.STATUS_NORMAL) && !epState.getStatus().equals(VersionedValue.SHUTDOWN)) {
                    logger.info("Ignoring node state={}", epState);
                    continue;
                }

                if (isLocal(endpoint)) {
                    VersionedValue vv = epState.getApplicationState(ApplicationState.HOST_ID);
                    if (vv != null) {
                        String hostId = vv.value;
                        if (!this.localNode().getId().equals(hostId)) {
                            gossipCluster.update(endpoint.address, epState, "discovery-init-gossip", false);
                        }
                    }
                }
            }
        }

        // Cassandra is usually in the NORMAL state when discovery start.
        if (isNormal(Gossiper.instance.getEndpointStateForEndpoint(this.localAddress)) && isAutoEnableSearch()) {
            try {
                this.setSearchEnabled(true);
            } catch (IOException e) {
                logger.error("Failed to set searchEnabled",e);
            }
        }

        clusterService.submitStateUpdateTask("starting-cassandra-discovery", new RoutingTableUpdateTask(true),
            routingTableUpdateTaskExecutor, routingTableUpdateTaskExecutor, routingTableUpdateTaskExecutor);
    }

    public ClusterState initClusterState(DiscoveryNode localNode) {
        ClusterState.Builder builder = ClusterState.builder(clusterName);
        ClusterState clusterState = builder.nodes(DiscoveryNodes.builder().add(localNode)
                .localNodeId(localNode.getId())
                .masterNodeId(localNode.getId())
                .build())
            .blocks(ClusterBlocks.builder()
                .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                .addGlobalBlock(CassandraGatewayService.NO_CASSANDRA_RING_BLOCK))
            .build();
        setCommittedState(clusterState);
        this.clusterApplier.setInitialState(clusterState);
        return clusterState;
    }

    class RoutingTableUpdateTask  {
        final Set<String> indices;   // update routing for these indices
        final boolean updateRouting;// update routinTable (X1 or status change)

        RoutingTableUpdateTask(String index) {
            this(true, Collections.singleton(index));
        }

        RoutingTableUpdateTask(boolean updateRouting) {
            this(updateRouting, Collections.EMPTY_SET);
        }

        RoutingTableUpdateTask(boolean updateRouting, Set<String> indices) {
            this.indices = indices;
            this.updateRouting = updateRouting;
        }

        public Set<String> indices()  { return this.indices; }
        public boolean updateRouting() { return this.updateRouting; }
    }

    /**
     * Computation of the routing table for several indices (or for all indices if nodes changed) for batched cluster state updates.
     */
    class RoutingTableUpdateTaskExecutor implements ClusterStateTaskExecutor<RoutingTableUpdateTask>, ClusterStateTaskConfig, ClusterStateTaskListener {

        @Override
        public ClusterTasksResult<RoutingTableUpdateTask> execute(ClusterState currentState, List<RoutingTableUpdateTask> tasks) throws Exception {
            boolean updateRouting = tasks.stream().filter(RoutingTableUpdateTask::updateRouting).count() > 0;
            Set<Index> indices = tasks.stream().map(RoutingTableUpdateTask::indices)
                .flatMap(Set::stream)
                .map(i -> Optional.ofNullable(currentState.metadata().hasIndex(i) ? currentState.metadata().index(i).getIndex() : null))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());

            DiscoveryNodes discoverNodes = nodes();
            ClusterState.Builder clusterStateBuilder = ClusterState.builder(currentState);
            clusterStateBuilder.nodes(discoverNodes);

            if (currentState.nodes().getSize() != discoverNodes.getSize() || updateRouting) {
                // Routing-only updates (searchEnabled/X1 changes) should not bump metadata.version when shard/replica
                // counts are unchanged; that drift breaks the later schema CAS against Cassandra metadata_log.
                Metadata.Builder metaDataBuilder = null;
                for (Iterator<IndexMetadata> it = currentState.metadata().iterator(); it.hasNext();) {
                    IndexMetadata indexMetaData = it.next();
                    final int targetShards = discoverNodes.getSize();
                    final int targetReplicas = Math.max(0, ClusterService.replicationFactor(keyspaceForIndex(indexMetaData)) - 1);
                    if (indexMetaData.getNumberOfShards() == targetShards && indexMetaData.getNumberOfReplicas() == targetReplicas) {
                        continue;
                    }
                    if (metaDataBuilder == null) {
                        metaDataBuilder = Metadata.builder(currentState.metadata());
                    }
                    IndexMetadata.Builder indexMetaDataBuilder = IndexMetadata.builder(indexMetaData);
                    indexMetaDataBuilder.numberOfShards(targetShards);
                    indexMetaDataBuilder.numberOfReplicas(targetReplicas);
                    metaDataBuilder.put(indexMetaDataBuilder.build(), false);
                }
                if (metaDataBuilder != null) {
                    clusterStateBuilder.metadata(metaDataBuilder.build());
                }
                ClusterState workingClusterState = clusterStateBuilder.build();
                RoutingTable routingTable = RoutingTable.build(clusterService, workingClusterState);
                ClusterState resultingState = ClusterState.builder(workingClusterState).routingTable(routingTable).build();
                return ClusterTasksResult.<RoutingTableUpdateTask>builder().successes(tasks).build(resultingState);
            }

            // only update routing table for some indices
            RoutingTable routingTable = indices.isEmpty() ?
                    RoutingTable.build(clusterService, clusterStateBuilder.build()) :
                    RoutingTable.build(clusterService, clusterStateBuilder.build(), indices);
            ClusterState resultingState = ClusterState.builder(currentState).routingTable(routingTable).build();
            return ClusterTasksResult.<RoutingTableUpdateTask>builder().successes(tasks).build(resultingState);
        }

        @Override
        public TimeValue timeout() {
            return null;
        }

        @Override
        public Priority priority() {
            return Priority.NORMAL;
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.error("unexpected failure during [{}]", e, source);
        }
    }

    private long getMetadataVersion(VersionedValue versionValue) {
        int i = versionValue.value.indexOf('/');
        if (i > 0) {
            try {
                return Long.valueOf(versionValue.value.substring(i+1));
            } catch (NumberFormatException e) {
                logger.error("Unexpected gossip.X2 value "+versionValue.value, e);
            }
        }
        return -1;
    }

    private int publishPort() {
        try {
            return settings.getAsInt("transport.netty.publish_port", settings.getAsInt("transport.publish_port",settings.getAsInt("transport.tcp.port", 9300)));
        } catch (SettingsException | NumberFormatException e) {
            String publishPort = settings.get("transport.netty.publish_port", settings.get("transport.publish_port",settings.get("transport.tcp.port", "9300")));
            if (publishPort.indexOf("-") > 0) {
                return Integer.parseInt(publishPort.split("-")[0]);
            } else {
                throw e;
            }
        }
    }

    private boolean isLocal(InetAddressAndPort endpoint) {
        return DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint).equals(localDc);
    }

    private boolean isMember(InetAddressAndPort endpoint) {
        return !this.localAddress.equals(endpoint) && DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint).equals(localDc);
    }

    /**
     * #183 lookup EndpointState with the node name = cassandra broadcast address.
     * ES RPC adress can be different from the cassandra broadcast address.
     */
    public boolean isNormal(DiscoveryNode node) {
        // endpoint address = C* broadcast address = Elasticsearch node name (transport may be bound to C* internal or C* RPC broadcast)
        final InetAddressAndPort endpoint;
        try {
            endpoint = InetAddressAndPort.getByName(node.getName());
        } catch (UnknownHostException e) {
            logger.warn("Node name [{}] could not be resolved to an endpoint: {}", node.getName(), e.toString());
            return false;
        }
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null) {
            logger.warn(
                "Node endpoint address=[{}] name=[{}] state not found",
                node.getAddress().address().getAddress(),
                node.getName());
            return false;
        }
        return state.isAlive() && state.getStatus().equals(VersionedValue.STATUS_NORMAL);
    }

    private boolean isNormal(EndpointState state) {
        return state != null && state.isAlive() && state.getStatus().equals(VersionedValue.STATUS_NORMAL);
    }

    public static InetAddress getInternalIp(EndpointState epState) {
        return epState.getApplicationState(ApplicationState.INTERNAL_IP) == null ? null :
                InetAddresses.forString(epState.getApplicationState(ApplicationState.INTERNAL_IP).value);
    }

    public static InetAddress getRpcAddress(EndpointState epState) {
        return epState.getApplicationState(ApplicationState.RPC_ADDRESS) == null ? null :
                InetAddresses.forString(epState.getApplicationState(ApplicationState.RPC_ADDRESS).value);
    }

    @Override
    public void beforeChange(InetAddressAndPort endpoint, EndpointState state, ApplicationState appState, VersionedValue value) {
        //logger.debug("beforeChange Endpoint={} EndpointState={}  ApplicationState={} value={}", endpoint, state, appState, value);
    }

    @Override
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue versionValue) {
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        traceEpState(endpoint.address, epState);

        String hostId = epState.getApplicationState(ApplicationState.HOST_ID).value;
        if (hostId != null && isMember(endpoint)) {
            if (logger.isTraceEnabled())
                logger.trace("Endpoint={} ApplicationState={} value={}", endpoint, state, versionValue);

            gossipCluster.update(endpoint.address, epState, "onChange-" + endpoint, true);
        }

        // self status update.
        if (this.localAddress.equals(endpoint)) {
            switch (state) {
            case STATUS:
                if (logger.isTraceEnabled())
                    logger.trace("Endpoint={} STATUS={} => may update searchEnabled", endpoint, versionValue);


                // update searchEnabled according to the node status and autoEnableSearch.
                if (isNormal(Gossiper.instance.getEndpointStateForEndpoint(endpoint))) {
                    if (!this.searchEnabled.get() && this.autoEnableSearch.get()) {
                        try {
                            setSearchEnabled(true, true);
                        } catch (IOException e) {
                            logger.error("Failed to enable search",e);
                        }
                    }
                    publishX2(this.committedState.get(), true);
                 } else {
                    // node is leaving or whatever, disabling search.
                    if (this.searchEnabled.get()) {
                        try {
                            setSearchEnabled(false, true);
                        } catch (IOException e) {
                            logger.error("Failed to disable search",e);
                        }
                    }
                }
                break;
            }
        }
    }

    /**
     * Warning: IEndpointStateChangeSubscriber.onXXXX should not block (on connection timeout or clusterState update) to avoid gossip issues.
     */

    private void traceEpState(InetAddress endpoint, EndpointState epState) {
        if (logger.isTraceEnabled())
            logger.trace("Endpoint={} isAlive={} STATUS={} HOST_ID={} INTERNAL_IP={} RPC_ADDRESS={} SCHEMA={} X1={} X2={}", endpoint,
                    epState.isAlive(),
                    epState.getStatus(),
                    epState.getApplicationState(ApplicationState.HOST_ID),
                    epState.getApplicationState(ApplicationState.INTERNAL_IP),
                    epState.getApplicationState(ApplicationState.RPC_ADDRESS),
                    epState.getApplicationState(ApplicationState.SCHEMA),
                    epState.getApplicationState(ApplicationState.X1),
                    epState.getApplicationState(ApplicationState.X2));
    }

    @Override
    public void onAlive(InetAddressAndPort endpoint, EndpointState epState) {
        if (isMember(endpoint)) {
            traceEpState(endpoint.address, epState);
            logger.debug("Endpoint={} isAlive={} => update node + connecting", endpoint, epState.isAlive());
            gossipCluster.update(endpoint.address, epState, "onAlive-" + endpoint, true);
        }
    }

    @Override
    public void onDead(InetAddressAndPort endpoint, EndpointState epState) {
        if (isMember(endpoint)) {
            traceEpState(endpoint.address, epState);
            logger.warn("Endpoint={} isAlive={} => update node + disconnecting", endpoint, epState.isAlive());
            gossipCluster.update(endpoint.address, epState, "onDead-" + endpoint, true);
        }
    }

    @Override
    public void onRestart(InetAddressAndPort endpoint, EndpointState epState) {
        if (isMember(endpoint)) {
            traceEpState(endpoint.address, epState);
            gossipCluster.update(endpoint.address, epState, "onAlive-" + endpoint, true);
        }
    }

    @Override
    public void onJoin(InetAddressAndPort endpoint, EndpointState epState) {
        if (isLocal(endpoint)) {
            traceEpState(endpoint.address, epState);
            gossipCluster.update(endpoint.address, epState, "onAlive-" + endpoint, true);
        }
    }

    @Override
    public void onRemove(InetAddressAndPort endpoint) {
        if (this.localAddress.equals(endpoint)) {
            try {
                setSearchEnabled(false);
            } catch (IOException e) {
            }
        } else if (isMember(endpoint)) {
            EndpointState ep = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (ep != null) {
                VersionedValue vv = ep.getApplicationState(ApplicationState.HOST_ID);
                if (vv != null && vv.value != null) {
                    String hostId = vv.value;
                    UUID hostUuid = UUID.fromString(vv.value);
                    if (!localNode().getId().equals(hostId) && gossipCluster.contains(hostUuid)) {
                        logger.warn("Removing node ip={} node={}  => disconnecting", endpoint, hostId);
                        notifyHandler(Gossiper.instance.getEndpointStateForEndpoint(endpoint));
                        gossipCluster.remove(hostUuid, "onRemove-" + endpoint.address.getHostAddress());
                    }
                }
            }
        }
    }

    /**
     * Release the listener when all attendees have reached the expected version or become down.
     * Called by the cassandra gossiper thread from onChange() or onDead() or onRemove().
     */
    public void notifyHandler(EndpointState endPointState) {
        VersionedValue hostIdValue = endPointState.getApplicationState(ApplicationState.HOST_ID);
        if (hostIdValue == null)
            return; // happen when we are removing a node while updating the mapping

        String hostId = hostIdValue.value;
        if (hostId == null || localNode().getId().equals(hostId))
            return;

        if (!endPointState.isAlive() || !endPointState.getStatus().equals("NORMAL")) {
            // node was removed from the gossiper, down or leaving, acknowledge to avoid locking.
            AckClusterStatePublishResponseHandler handler = handlerRef.get();
            if (handler != null) {
                DiscoveryNode node = nodes().get(hostId);
                if (node != null) {
                    logger.debug("nack node={}", node.getId());
                    handler.onFailure(node, new NoNodeAvailableException("Node "+hostId+" unavailable"));
                }
            }
        }
    }

    @Override
    protected void doStop() throws OpenSearchException {
        Gossiper.instance.unregister(this);

        synchronized (gossipCluster) {
            gossipCluster.remoteMembers.clear();
        }
    }

    private static final ApplicationState ELASTIC_SHARDS_STATES = ApplicationState.X1;
    private static final ApplicationState ELASTIC_META_DATA = ApplicationState.X2;
    private static final com.fasterxml.jackson.databind.ObjectMapper jsonMapper = new com.fasterxml.jackson.databind.ObjectMapper();
    private static final TypeReference<Map<String, ShardRoutingState>> indexShardStateTypeReference = new TypeReference<Map<String, ShardRoutingState>>() {};

    public Map<String,ShardRoutingState> getShardRoutingState(UUID nodeUuid) {
        return gossipCluster.getShardRoutingState(nodeUuid);
    }

    public boolean isSearchEnabled() {
        return this.searchEnabled.get();
    }

    public boolean isAutoEnableSearch() {
        return this.autoEnableSearch.get();
    }

    public void setAutoEnableSearch(boolean newValue) {
       this.autoEnableSearch.set(newValue);
    }

    // warning: called from the gossiper.
    public void setSearchEnabled(boolean ready) throws IOException {
        setSearchEnabled(ready, false);
    }

    public void setSearchEnabled(boolean ready, boolean forcePublishX1) throws IOException {
        if (ready && !isNormal(Gossiper.instance.getEndpointStateForEndpoint(this.localAddress))) {
            throw new IOException("Cassandra not ready for search");
        }
        if (searchEnabled.getAndSet(ready) != ready || forcePublishX1) {
            logger.info("searchEnabled set to [{}]", ready);
            publishX1(forcePublishX1);
            clusterService.submitStateUpdateTask("searchEnabled-changed-to-"+ready, new RoutingTableUpdateTask(true),
                routingTableUpdateTaskExecutor, routingTableUpdateTaskExecutor, routingTableUpdateTaskExecutor);
        }
    }

    public void updateRoutingTable(Set<String> indices, ClusterStateTaskListener listener) {
        clusterService.submitStateUpdateTask("update-routing-table", new RoutingTableUpdateTask(true, indices),
            routingTableUpdateTaskExecutor, routingTableUpdateTaskExecutor, listener);
    }

    public void publishX1() throws JsonGenerationException, JsonMappingException, IOException {
        publishX1(false);
    }

    // Warning: on nodetool enablegossip, Gossiper.instance.isEnable() may be false while receiving a onChange event !
    private void publishX1(boolean force) throws JsonGenerationException, JsonMappingException, IOException {
        if (Gossiper.instance.isEnabled() || force) {
            ClusterBlockException blockException = clusterState().blocks().globalBlockedException(ClusterBlockLevel.READ);
            if (blockException != null)
                logger.debug("Node not ready for READ block={}", clusterState().blocks());
            if (searchEnabled.get() && blockException == null) {
                Map<String, ShardRoutingState> localShardStateMap = new HashMap<>();
                if (clusterService.getIndicesService() != null) {
                    for(IndexService indexService : clusterService.getIndicesService()) {
                        try {
                            IndexShard localIndexShard = indexService.getShardOrNull(0);
                            localShardStateMap.put(indexService.index().getName(),
                                (localIndexShard != null && localIndexShard.routingEntry() != null) ?
                                    localIndexShard.routingEntry().state() :
                                    ShardRoutingState.INITIALIZING);
                        } catch (ShardNotFoundException | IndexNotFoundException e) {
                        }
                    }
                }
                String newValue = jsonMapper.writerWithType(indexShardStateTypeReference).writeValueAsString(localShardStateMap);
                Gossiper.instance.addLocalApplicationState(ELASTIC_SHARDS_STATES, StorageService.instance.valueFactory.datacenter(gzip ? compress(newValue) : newValue));
            } else {
                // publish an empty map, so other nodes will see local shards UNASSIGNED.
                // empty doesn't have to be GZipped
                Gossiper.instance.addLocalApplicationState(ELASTIC_SHARDS_STATES, StorageService.instance.valueFactory.datacenter("{}"));
            }
        } else {
            logger.warn("Gossiper not yet enabled to publish X1");
        }
    }

    public void publishX2(ClusterState clusterState) {
        publishX2(clusterState, false);
    }

    public void publishX2(ClusterState clusterState, boolean force) {
        if (Gossiper.instance.isEnabled() || force) {
            Gossiper.instance.addLocalApplicationState(ELASTIC_META_DATA, StorageService.instance.valueFactory.datacenter(clusterState.metadata().x2()));
            if (logger.isTraceEnabled())
                logger.trace("X2={} published in gossip state", clusterState.metadata().x2());
        } else {
            logger.warn("Gossiper not yet enabled to publish X2");
        }
    }

    @Override
    protected void doClose()  {
        Gossiper.instance.unregister(this);
    }

    public ClusterState clusterState() {
        ClusterState clusterState = committedState.get();
        assert clusterState != null : "accessing cluster state before it is set";
        return clusterState;
    }

    // visible for testing
    void setCommittedState(ClusterState clusterState) {
        synchronized (stateMutex) {
            committedState.set(clusterState);
            publishX2(clusterState);
        }
    }

    public DiscoveryNode localNode() {
        return this.transportService.getLocalNode();
    }

    public String nodeDescription() {
        return clusterName.value() + "/" + localNode().getId();
    }

    public DiscoveryNodes nodes() {
        return this.gossipCluster.nodes();
    }

    public ElassandraGossipNodeStatus discoveryNodeStatus(final EndpointState epState) {
        if (epState == null || !epState.isAlive()) {
            return ElassandraGossipNodeStatus.DEAD;
        }
        if (epState.getApplicationState(ApplicationState.X2) == null) {
            return ElassandraGossipNodeStatus.DISABLED;
        }
        if (VersionedValue.STATUS_NORMAL.equals(epState.getStatus()) ||
            VersionedValue.STATUS_LEAVING.equals(epState.getStatus())  ||
            VersionedValue.STATUS_MOVING.equals(epState.getStatus())
        ) {
            return ElassandraGossipNodeStatus.ALIVE;
        }
        return ElassandraGossipNodeStatus.DEAD;
    }

    @Override
    public void startInitialJoin() {
        publishX2(this.committedState.get());
    }

    /**
     * Publish all the changes to the cluster from the master (can be called just by the master). The publish
     * process should apply this state to the master as well!
     *
     * The {@link AckListener} allows to keep track of the ack received from nodes, and verify whether
     * they updated their own cluster state or not.
     *
     * The method is guaranteed to throw a {@link FailedToCommitClusterStateException} if the change is not committed and should be rejected.
     * Any other exception signals the something wrong happened but the change is committed.
     *
     * Strapdata NOTES:
     * Publish is blocking until change is apply locally, but not while waiting remote nodes.
     * When the last remote node acknowledge a metadata version, this finally acknowledge the calling task.
     * According to the Metadata.clusterUuid in the new clusterState, the node acts as the coordinator or participant.
     */
    @Override
    public void publish(
        final ClusterChangedEvent clusterChangedEvent,
        final org.opensearch.action.ActionListener<Void> publishListener,
        final AckListener ackListener) {
        ClusterState previousClusterState = clusterChangedEvent.previousState();
        ClusterState newClusterState = normalizeSchemaUpdateClusterState(
            clusterChangedEvent.schemaUpdate(),
            previousClusterState,
            clusterChangedEvent.state()
        );
        logger.warn(
            "publish source={} schemaUpdate={} localNode={} metadata={}",
            clusterChangedEvent.source(),
            clusterChangedEvent.schemaUpdate(),
            localNode().getId(),
            newClusterState.metadata().x2()
        );

        long startTimeNS = System.nanoTime();
        try {
            if (clusterChangedEvent.schemaUpdate().updated()) {
                // update and broadcast the metadata through a CQL schema update + ack from participant nodes
                if (localNode().getId().equals(newClusterState.metadata().clusterUUID())) {
                    publishAsCoordinator(clusterChangedEvent, ackListener, startTimeNS);
                } else {
                    publishAsParticipator(clusterChangedEvent, ackListener, startTimeNS);
                }
            } else {
                // publish local cluster state update (for blocks, nodes or routing update)
                publishLocalUpdate(clusterChangedEvent, ackListener, startTimeNS);
            }
            publishListener.onResponse(null);
        } catch (Exception e) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - startTimeNS)));
            StringBuilder sb = new StringBuilder("failed to execute cluster state update in ").append(executionTime)
                    .append(", state:\nversion [")
                    .append(previousClusterState.version()).
                    append("], source [").append(clusterChangedEvent.source()).append("]\n");
            logger.warn(sb.toString(), e);
            publishListener.onFailure(e);
            throw new OpenSearchException(e);
        }
    }

    /**
     * Publish the new metadata through a CQL schema update (a blocking schema update unless we update a CQL map as a dynamic nested object),
     * and wait acks (AckClusterStatePublishResponseHandler) from participant nodes with state alive+NORMAL.
     */
    void publishAsCoordinator(final ClusterChangedEvent clusterChangedEvent, final AckListener ackListener, final long startTimeNS)
        throws InterruptedException, IOException {
        logger.debug("Coordinator update source={} metadata={}", clusterChangedEvent.source(), clusterChangedEvent.state().metadata().x2());

        ClusterState previousClusterState = clusterChangedEvent.previousState();
        ClusterState newClusterState = normalizeSchemaUpdateClusterState(
            clusterChangedEvent.schemaUpdate(),
            previousClusterState,
            clusterChangedEvent.state()
        );
        DiscoveryNodes nodes = clusterChangedEvent.state().nodes();
        DiscoveryNode localNode = nodes.getLocalNode();

        // increment metadata.version
        newClusterState = ClusterState.builder(newClusterState)
                .metadata(Metadata.builder(newClusterState.metadata()).incrementVersion().build())
                .build();

        Collection<Mutation> mutations = clusterChangedEvent.mutations() == null ? new ArrayList<>() : clusterChangedEvent.mutations();
        Collection<Event.SchemaChange> events = clusterChangedEvent.events() == null ? new ArrayList<>() : clusterChangedEvent.events();
        try {
            // TODO: track change to update CQL schema when really needed
            clusterService.writeMetadataToSchemaMutations(newClusterState.metadata(), mutations, events);
        } catch (ConfigurationException | IOException e1) {
            throw new OpenSearchException(e1);
        }

        try {
            // PAXOS schema update commit
            logger.warn(
                "commit metadata source={} prev={} next={}",
                clusterChangedEvent.source(),
                previousClusterState.metadata().x2(),
                newClusterState.metadata().x2()
            );
            clusterService.commitMetaData(previousClusterState.metadata(), newClusterState.metadata(), clusterChangedEvent.source());
            notifyCommit(ackListener, startTimeNS);

            // compute alive node for awaiting applied acknowledgment
            long publishingStartInNanos = System.nanoTime();
            Set<DiscoveryNode> nodesToPublishTo = new HashSet<>(nodes.getSize());
            for (final DiscoveryNode node : nodes) {
                if (isNormal(node))
                    nodesToPublishTo.add(node);
            }
            logger.trace("New coordinator handler for nodes={}", nodesToPublishTo);
            final AckClusterStatePublishResponseHandler handler = new AckClusterStatePublishResponseHandler(nodesToPublishTo, ackListener);
            handlerRef.set(handler);

            // apply new CQL schema
            if (mutations != null && mutations.size() > 0) {
                logger.debug("Applying CQL schema source={} update={} mutations={} ",
                        clusterChangedEvent.source(), clusterChangedEvent.schemaUpdate(), mutations);

                // unless update is UPDATE_ASYNCHRONOUS, block until schema is applied.
                MigrationManager.announce(mutations, this.clusterService.getSchemaManager().getInhibitedSchemaListeners());

                // build routing table when keyspaces are created locally
                newClusterState = ClusterState.builder(newClusterState)
                        .routingTable(buildRoutingTableWithRetry(newClusterState))
                        .build();
                logger.debug("CQL source={} SchemaChanges={}", clusterChangedEvent.source(), events);
            }

            // add new cluster state into the pending-to-apply cluster states queue, listening ack from remote nodes.
            final AtomicBoolean processedOrFailed = new AtomicBoolean();
            pendingStatesQueue.addPending(newClusterState, new PendingClusterStatesQueue.StateProcessedListener() {
                @Override
                public void onNewClusterStateProcessed() {
                    processedOrFailed.set(true);
                    handler.onResponse(localNode);
                }

                @Override
                public void onNewClusterStateFailed(Exception e) {
                    processedOrFailed.set(true);
                    handler.onFailure(localNode, e);
                    logger.warn((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage(
                            "failed while applying cluster state locally [{}]", clusterChangedEvent.source()), e);
                }
            });

            // apply the next-to-process cluster state.
            synchronized (stateMutex) {
                if (clusterChangedEvent.previousState() != this.committedState.get()) {
                    throw new FailedToCommitClusterStateException("local state was mutated while CS update was published to other nodes");
                }

                boolean sentToApplier = processNextCommittedClusterState("committed source=" + clusterChangedEvent.source() + " metadata=" + newClusterState.metadata().x2());
                if (sentToApplier == false && processedOrFailed.get() == false) {
                    logger.warn("metadata={} has neither been processed nor failed", newClusterState.metadata().x2());
                    assert false : "cluster state published locally neither processed nor failed: " + newClusterState;
                    return;
                }
            }

            // wait all nodes are applied.
            final TimeValue publishTimeout = discoverySettings.getPublishTimeout();
            long timeLeftInNanos = Math.max(0, publishTimeout.nanos() - (System.nanoTime() - publishingStartInNanos));
            if (!handler.awaitAllNodes(TimeValue.timeValueNanos(timeLeftInNanos))) {
                logger.info("commit source={} metadata={} timeout with pending nodes={}",
                        clusterChangedEvent.source(), newClusterState.metadata().x2(), Arrays.toString(handler.pendingNodes()));
            } else {
                logger.debug("commit source={} metadata={} applied succefully on nodes={}",
                        clusterChangedEvent.source(), newClusterState.metadata().x2(), nodesToPublishTo);
            }

        } catch (ConcurrentMetaDataUpdateException e) {
            UUID owner = clusterService.readMetaDataOwner(newClusterState.metadata().version());
            notifyCommit(ackListener, startTimeNS);
            ackListener.onNodeAck(localNode, null);
            try {
                final Metadata committedMetaData = clusterService.loadGlobalState();
                if (metadataUpdateSatisfied(previousClusterState.metadata(), newClusterState.metadata(), committedMetaData)) {
                    logger.warn(
                        "PAXOS concurrent update, source={} metadata={}, owner={}, committed metadata already satisfies requested update; reloading committed metadata",
                        clusterChangedEvent.source(),
                        newClusterState.metadata().x2(),
                        owner
                    );
                    reloadCommittedMetadata(clusterChangedEvent.source(), committedMetaData);
                } else {
                    // Refresh the local cluster state from Cassandra first so the delayed task can replay
                    // against the winning metadata instead of waiting forever for an unrelated later change.
                    logger.warn(
                        "PAXOS concurrent update, source={} metadata={}, owner={}, reloading committed metadata and resubmitting task on next metadata change",
                        clusterChangedEvent.source(),
                        newClusterState.metadata().x2(),
                        owner
                    );
                    resubmitTaskOnNextChange(clusterChangedEvent);
                    reloadCommittedMetadata(clusterChangedEvent.source(), committedMetaData);
                }
            } catch (Exception reloadFailure) {
                logger.warn(
                    "PAXOS concurrent update, source={} metadata={}, owner={}, failed to inspect committed metadata; resubmit task on next metadata change",
                    clusterChangedEvent.source(),
                    newClusterState.metadata().x2(),
                    owner,
                    reloadFailure
                );
                resubmitTaskOnNextChange(clusterChangedEvent);
            }
            return;
        } catch(UnavailableException e) {
            logger.error("PAXOS not enough available nodes, source={} metadata={}",
                    clusterChangedEvent.source(), newClusterState.metadata().x2());
            ackListener.onNodeAck(localNode, e);
            throw e;
        } catch(WriteTimeoutException e) {
            // see https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
            logger.warn("PAXOS write timeout, source={} metadata={} writeType={}, reading the owner of version={}",
                    clusterChangedEvent.source(), newClusterState.metadata().x2(), e.writeType, newClusterState.metadata().version());

            // read the owner for the expected version to know if PAXOS transaction succeed or not.
            UUID owner = clusterService.readMetaDataOwner(newClusterState.metadata().version());
            if (owner == null || !owner.equals(newClusterState.metadata().clusterUUID())) {
                logger.warn("PAXOS timeout and failed to write version={}, owner={}", newClusterState.metadata().version(), owner);
                throw new PaxosMetaDataUpdateException(e);
            }

            logger.warn("PAXOS timeout but succesfully write x2={}", newClusterState.metadata().x2());
            notifyCommit(ackListener, startTimeNS);
            ackListener.onNodeAck(localNode, e);
        } finally {
            handlerRef.set(null);
        }
    }

    /**
     * Publish the new metadata and notify the coordinator through an appliedClusterStateAction.
     */
    void publishAsParticipator(final ClusterChangedEvent clusterChangedEvent, final AckListener ackListener, final long startTimeNS) {
        ClusterState newClusterState = clusterChangedEvent.state();
        String reason = clusterChangedEvent.source();

        final DiscoveryNode coordinatorNode = newClusterState.nodes().get(newClusterState.metadata().clusterUUID());
        logger.debug("Participator update reason={} metadata={} coordinator={}", reason, newClusterState.metadata().x2(), coordinatorNode);

        if (newClusterState.metadata().version() <= clusterState().metadata().version()) {
            logger.warn("Ignore and acknowlegde obsolete update metadata={}", newClusterState.metadata().x2());
            if (coordinatorNode != null) {
                // coordinator from a remote DC maybe null.
                CassandraDiscovery.this.appliedClusterStateAction.sendAppliedToNode(coordinatorNode, newClusterState, null);
            }
            return;
        }

        notifyCommit(ackListener, startTimeNS);
        final AtomicBoolean processedOrFailed = new AtomicBoolean();
        this.pendingStatesQueue.addPending(newClusterState,  new PendingClusterStatesQueue.StateProcessedListener() {
            @Override
            public void onNewClusterStateProcessed() {
                if (coordinatorNode != null) {
                    logger.trace("sending applied state=[{}] to coordinator={} reason={}",
                            newClusterState.metadata().x2(), coordinatorNode, reason);
                    CassandraDiscovery.this.appliedClusterStateAction.sendAppliedToNode(coordinatorNode, newClusterState, null);
                }
            }

            @Override
            public void onNewClusterStateFailed(Exception e) {
                if (coordinatorNode != null) {
                    logger.trace("sending failed state=[{}] to coordinator={} reason={} exception={}",
                            newClusterState.metadata().x2(), coordinatorNode, reason, e.toString());
                    CassandraDiscovery.this.appliedClusterStateAction.sendAppliedToNode(coordinatorNode, newClusterState, e);
                }
            }
        });
        // apply the next-to-process cluster state.
        synchronized (stateMutex) {
            boolean sentToApplier = processNextCommittedClusterState(
                    "committed version [" + newClusterState.metadata().x2() + "] source [" + reason + "]");
            if (sentToApplier == false  && processedOrFailed.get() == false) {
                logger.warn("metadata={} has neither been processed nor failed", newClusterState.metadata().x2());
                assert false : "cluster state published locally neither processed nor failed: " + newClusterState;
                return;
            }
        }
    }

    /**
     * Publish a local cluster state update (no coordination) coming from a CQL schema update.
     */
    void publishLocalUpdate(final ClusterChangedEvent clusterChangedEvent, final AckListener ackListener, final long startTimeNS) {
        ClusterState newClusterState = rebaseLocalClusterState(clusterChangedEvent);
        logger.debug("Local update source={} metadata={}", clusterChangedEvent.source(), newClusterState.metadata().x2());
        notifyCommit(ackListener, startTimeNS);
        final AtomicBoolean processedOrFailed = new AtomicBoolean();
        pendingStatesQueue.addPending(newClusterState,
            new PendingClusterStatesQueue.StateProcessedListener() {
                @Override
                public void onNewClusterStateProcessed() {
                    processedOrFailed.set(true);
                    // simulate ack from all nodes, elassandra only update the local clusterState here.
                    clusterChangedEvent.state().nodes().forEach(node -> ackListener.onNodeAck(node, null));
                }

                @Override
                public void onNewClusterStateFailed(Exception e) {
                    processedOrFailed.set(true);
                    // simulate nack from all nodes, elassandra only update the local clusterState here.
                    clusterChangedEvent.state().nodes().forEach(node -> ackListener.onNodeAck(node, e));
                    logger.warn((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage(
                            "failed while applying cluster state locally source={}", clusterChangedEvent.source()), e);
                }
            });

        // apply the next-to-process cluster state.
        synchronized (stateMutex) {
            if (clusterChangedEvent.previousState() != this.committedState.get()) {
                throw new FailedToCommitClusterStateException("local state was mutated while CS update was published to other nodes");
            }

            boolean sentToApplier = processNextCommittedClusterState(
                    "committed version [" + newClusterState.metadata().x2() + "] source [" + clusterChangedEvent.source() + "]");
            if (sentToApplier == false && processedOrFailed.get() == false) {
                logger.warn("metadata={} source=[{}] has neither been processed nor failed", newClusterState.metadata().x2(), clusterChangedEvent.source());
                assert false : "cluster state published locally neither processed nor failed: " + newClusterState;
                return;
            }
        }
    }

    private void notifyCommit(AckListener ackListener, long startTimeNS) {
        // OpenSearch expects the commit signal before any subsequent node acks for the same update.
        ackListener.onCommit(TimeValue.timeValueNanos(System.nanoTime() - startTimeNS));
    }

    private ClusterState rebaseLocalClusterState(ClusterChangedEvent clusterChangedEvent) {
        final ClusterState previousState = clusterChangedEvent.previousState();
        final ClusterState newClusterState = clusterChangedEvent.state();
        final Metadata previousMetaData = previousState.metadata();
        final Metadata newMetaData = newClusterState.metadata();

        if (newMetaData == previousMetaData) {
            return newClusterState;
        }

        final boolean staleVersion = newMetaData.version() <= previousMetaData.version();
        final boolean staleCoordinator = newMetaData.clusterUUID().equals(previousMetaData.clusterUUID()) == false;
        if (staleVersion == false && staleCoordinator == false) {
            return newClusterState;
        }

        final String coordinatorId = resolveCoordinatorId(previousMetaData.clusterUUID());
        final Metadata rebasedMetaData = Metadata.builder(newMetaData)
            .clusterUUID(coordinatorId)
            .version(staleVersion ? previousMetaData.version() + 1 : newMetaData.version())
            .build();
        logger.debug(
            "rebasing local update source={} metadata={} -> {}",
            clusterChangedEvent.source(),
            newMetaData.x2(),
            rebasedMetaData.x2()
        );
        return ClusterState.builder(newClusterState).metadata(rebasedMetaData).build();
    }

    private ClusterState normalizeSchemaUpdateClusterState(
        ClusterStateTaskConfig.SchemaUpdate schemaUpdate,
        ClusterState previousClusterState,
        ClusterState newClusterState
    ) {
        if (schemaUpdate.updated() == false || ClusterState.UNKNOWN_UUID.equals(newClusterState.metadata().clusterUUID()) == false) {
            return newClusterState;
        }

        final String coordinatorId = resolveCoordinatorId(previousClusterState.metadata().clusterUUID());
        return ClusterState.builder(newClusterState)
            .metadata(Metadata.builder(newClusterState.metadata()).clusterUUID(coordinatorId).build())
            .build();
    }

    private String resolveCoordinatorId(String clusterUUID) {
        return ClusterState.UNKNOWN_UUID.equals(clusterUUID) ? localNode().getId() : clusterUUID;
    }

    protected void resubmitTaskOnNextChange(final ClusterChangedEvent clusterChangedEvent) {
        final long resubmitTimeMillis = System.currentTimeMillis();
        final Metadata baselineMetaData = clusterChangedEvent.previousState().metadata();
        final AtomicBoolean replayed = new AtomicBoolean(false);
        final ClusterStateListener listener = new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                tryResubmitDelayedUpdate(clusterChangedEvent, baselineMetaData, resubmitTimeMillis, replayed, this, event.state());
            }
        };
        clusterService.addListener(listener);
        tryResubmitDelayedUpdate(clusterChangedEvent, baselineMetaData, resubmitTimeMillis, replayed, listener, clusterService.state());
    }

    private void tryResubmitDelayedUpdate(
        ClusterChangedEvent clusterChangedEvent,
        Metadata baselineMetaData,
        long resubmitTimeMillis,
        AtomicBoolean replayed,
        ClusterStateListener listener,
        ClusterState currentState
    ) {
        if (metadataChangedSince(currentState.metadata(), baselineMetaData) == false) {
            return;
        }
        if (replayed.compareAndSet(false, true) == false) {
            return;
        }

        final long lostTimeMillis = System.currentTimeMillis() - resubmitTimeMillis;
        final long remainingTimeMillis = 30 * 1000L - lostTimeMillis;
        clusterService.removeListener(listener);
        if (remainingTimeMillis <= 0L) {
            logger.warn(
                "metadata={} => drop expired delayed update source={} tasks={} lostTimeMillis={}",
                currentState.metadata().x2(),
                clusterChangedEvent.source(),
                clusterChangedEvent.taskInputs().updateTasks,
                lostTimeMillis
            );
            return;
        }

        Priority priority = Priority.URGENT;
        TimeValue timeout = TimeValue.timeValueMillis(remainingTimeMillis);
        Map<Object, ClusterStateTaskListener> map = clusterChangedEvent.taskInputs().updateTasksToMap(priority, lostTimeMillis);
        logger.warn(
            "metadata={} => resubmit delayed update source={} tasks={} priority={} remaing timeout={}",
            currentState.metadata().x2(),
            clusterChangedEvent.source(),
            clusterChangedEvent.taskInputs().updateTasks,
            priority,
            timeout
        );
        clusterService.submitStateUpdateTasks(
            clusterChangedEvent.source(),
            map,
            ClusterStateTaskConfig.build(priority, timeout),
            clusterChangedEvent.taskInputs().executor
        );
    }

    private void reloadCommittedMetadata(String source) {
        try {
            reloadCommittedMetadata(source, clusterService.loadGlobalState());
        } catch (Exception e) {
            logger.error("failed to reload committed metadata after [{}]", source, e);
        }
    }

    private void reloadCommittedMetadata(String source, Metadata reloadedMetaData) {
        clusterService.submitStateUpdateTask("reload-committed-metadata-after-concurrent-update", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                if (reloadedMetaData.version() < currentState.metadata().version()) {
                    logger.warn(
                        "skip stale committed metadata reload after [{}], committed metadata={} current metadata={}",
                        source,
                        reloadedMetaData.x2(),
                        currentState.metadata().x2()
                    );
                    return currentState;
                }
                ClusterState updatedState = ClusterState.builder(currentState)
                    .metadata(Metadata.builder(reloadedMetaData))
                    .build();
                try {
                    return ClusterState.builder(updatedState)
                        .routingTable(buildRoutingTableWithRetry(updatedState))
                        .build();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new OpenSearchException(e);
                }
            }

            @Override
            public void onFailure(String taskSource, Exception t) {
                logger.error("unexpected failure during [{}] triggered by [{}]", t, taskSource, source);
            }
        });
    }

    private boolean metadataUpdateSatisfied(Metadata previousMetaData, Metadata expectedMetaData, Metadata actualMetaData) {
        if (Metadata.isGlobalStateEquals(previousMetaData, expectedMetaData) == false
            && Metadata.isGlobalStateEquals(expectedMetaData, actualMetaData) == false) {
            return false;
        }

        Set<String> changedIndices = new HashSet<>();
        previousMetaData.indices().keysIt().forEachRemaining(changedIndices::add);
        expectedMetaData.indices().keysIt().forEachRemaining(changedIndices::add);
        for (String index : changedIndices) {
            IndexMetadata previousIndexMetaData = previousMetaData.index(index);
            IndexMetadata expectedIndexMetaData = expectedMetaData.index(index);
            if (Objects.equals(previousIndexMetaData, expectedIndexMetaData)) {
                continue;
            }
            if (Objects.equals(expectedIndexMetaData, actualMetaData.index(index)) == false) {
                return false;
            }
        }
        return true;
    }

    private boolean metadataChangedSince(Metadata currentMetaData, Metadata baselineMetaData) {
        return currentMetaData.version() != baselineMetaData.version()
            || currentMetaData.clusterUUID().equals(baselineMetaData.clusterUUID()) == false;
    }

    private RoutingTable buildRoutingTableWithRetry(ClusterState clusterState) throws InterruptedException {
        final int expectedIndices = expectedRoutableIndexCount(clusterState);
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        RoutingTable routingTable = RoutingTable.EMPTY_ROUTING_TABLE;
        RuntimeException lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                routingTable = RoutingTable.build(clusterService, clusterState);
                lastFailure = null;
                if (routingTable.indicesRouting().size() >= expectedIndices) {
                    return routingTable;
                }
            } catch (RuntimeException e) {
                lastFailure = e;
            }
            Thread.sleep(50L);
        }

        if (lastFailure != null) {
            logger.warn(
                "routing table build failed after schema update retries, metadata={} expectedIndices={}",
                clusterState.metadata().x2(),
                expectedIndices,
                lastFailure
            );
            throw lastFailure;
        }

        if (routingTable.indicesRouting().size() < expectedIndices) {
            logger.warn(
                "routing table still incomplete after schema update, metadata={} expectedIndices={} actualIndices={}",
                clusterState.metadata().x2(),
                expectedIndices,
                routingTable.indicesRouting().size()
            );
        }
        return routingTable;
    }

    private int expectedRoutableIndexCount(ClusterState clusterState) {
        int expectedIndices = 0;
        for (IndexMetadata indexMetaData : clusterState.metadata()) {
            if (indexMetaData.getState() == IndexMetadata.State.OPEN) {
                expectedIndices++;
            }
        }
        return expectedIndices;
    }

    // receive ack from remote nodes when cluster state applied.
    @Override
    public void onClusterStateApplied(String nodeId, String x2, Exception e, ActionListener<Void> processedListener) {
        logger.trace("received state=[{}] applied from={}", x2, nodeId);
        try {
            AckClusterStatePublishResponseHandler handler = this.handlerRef.get();
            DiscoveryNode node = this.committedState.get().nodes().get(nodeId);
            if (handler != null && node != null) {
                if (e != null) {
                    logger.trace("state=[{}] apply failed from node={}", x2, nodeId);
                    handler.onFailure(node, e);
                } else {
                    logger.trace("state=[{}] apply from node={}", x2, nodeId);
                    handler.onResponse(node);
                }
            }
            processedListener.onResponse(null);
        } catch(Exception ex) {
            processedListener.onFailure(ex);
        }
    }


    // return true if state has been sent to applier
    boolean processNextCommittedClusterState(String reason) {
        assert Thread.holdsLock(stateMutex);

        final ClusterState newClusterState = pendingStatesQueue.getNextClusterStateToProcess();
        final ClusterState currentState = committedState.get();
        // all pending states have been processed
        if (newClusterState == null) {
            return false;
        }

        if (newClusterState.nodes().getMasterNode() == null) {
            IllegalStateException e = new IllegalStateException(
                "received a cluster state without a resolvable master (masterNodeId="
                    + newClusterState.nodes().getMasterNodeId()
                    + ", localNodeId="
                    + newClusterState.nodes().getLocalNodeId()
                    + ")");
            logger.error(e.getMessage());
            try {
                pendingStatesQueue.markAsFailed(newClusterState, e);
            } catch (Exception inner) {
                logger.error((java.util.function.Supplier<?>) () -> new ParameterizedMessage("unexpected exception while failing [{}]", reason), inner);
            }
            return false;
        }
        if (newClusterState.blocks().hasGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ALL)) {
            IllegalStateException e = new IllegalStateException("received a cluster state with a no-master block");
            logger.error(e.getMessage());
            try {
                pendingStatesQueue.markAsFailed(newClusterState, e);
            } catch (Exception inner) {
                logger.error((java.util.function.Supplier<?>) () -> new ParameterizedMessage("unexpected exception while failing [{}]", reason), inner);
            }
            return false;
        }

        try {
            if (shouldIgnoreOrRejectNewClusterState(logger, currentState, newClusterState)) {
                String message = String.format(
                    Locale.ROOT,
                    "rejecting cluster state version [%d] uuid [%s] received from [%s]",
                    newClusterState.version(),
                    newClusterState.stateUUID(),
                    newClusterState.nodes().getMasterNodeId()
                );
                throw new IllegalStateException(message);
            }
        } catch (Exception e) {
            try {
                pendingStatesQueue.markAsFailed(newClusterState, e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error((java.util.function.Supplier<?>) () -> new ParameterizedMessage("unexpected exception while failing [{}]", reason), inner);
            }
            return false;
        }

        if (currentState.blocks().hasGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ALL)) {
            // its a fresh update from the master as we transition from a start of not having a master to having one
            logger.debug("got first state from fresh master [{}]", newClusterState.nodes().getMasterNodeId());
        }

        if (currentState == newClusterState) {
            return false;
        }

        committedState.set(newClusterState);

        clusterApplier.onNewClusterState("apply cluster state (from " + newClusterState.metadata().clusterUUID() + "[" + reason + "])",
            this::clusterState,
            new ClusterApplier.ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    try {
                        pendingStatesQueue.markAsProcessed(newClusterState);
                    } catch (Exception e) {
                        onFailure(source, e);
                    }
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error((java.util.function.Supplier<?>) () -> new ParameterizedMessage("unexpected failure applying [{}]", reason), e);
                    try {
                        // TODO: use cluster state uuid instead of full cluster state so that we don't keep reference to CS around
                        // for too long.
                        pendingStatesQueue.markAsFailed(newClusterState, e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.error((java.util.function.Supplier<?>) () -> new ParameterizedMessage("unexpected exception while failing [{}]", reason), inner);
                    }
                }
            });

        return true;
    }

    public static boolean shouldIgnoreOrRejectNewClusterState(Logger logger, ClusterState currentState, ClusterState newClusterState) {
        if (newClusterState.version() < currentState.version()) {
            logger.debug("received a cluster state that is not newer than the current one, ignoring (received {}, current {})",
                    newClusterState.version(), currentState.version());
            return true;
        }
        if (newClusterState.metadata().version() < currentState.metadata().version()) {
            logger.debug("received a cluster state metadata.verson that is not newer than the current one, ignoring (received {}, current {})",
                    newClusterState.metadata().version(), currentState.metadata().version());
            return true;
        }
        if (!newClusterState.metadata().clusterUUID().equals(currentState.metadata().clusterUUID()) &&
             newClusterState.metadata().version() == currentState.metadata().version() &&
             currentState.metadata().version() > 0)  {
            logger.debug("received a remote cluster state with same metadata.version, ignoring (received {}, current {})", newClusterState.metadata().version(), currentState.metadata().version());
            return true;
        }
        return false;
    }

    /**
     * does simple sanity check of the incoming cluster state. Throws an exception on rejections.
     */
    static void validateIncomingState(Logger logger, ClusterState incomingState, ClusterState lastState) {
        final ClusterName incomingClusterName = incomingState.getClusterName();
        if (!incomingClusterName.equals(lastState.getClusterName())) {
            logger.warn("received cluster state from [{}] which is also master but with a different cluster name [{}]",
                incomingState.nodes().getMasterNode(), incomingClusterName);
            throw new IllegalStateException("received state from a node that is not part of the cluster");
        }
        if (lastState.nodes().getLocalNode().equals(incomingState.nodes().getLocalNode()) == false) {
            logger.warn("received a cluster state from [{}] and not part of the cluster, should not happen",
                incomingState.nodes().getMasterNode());
            throw new IllegalStateException("received state with a local node that does not match the current local node");
        }

        if (shouldIgnoreOrRejectNewClusterState(logger, lastState, incomingState)) {
            String message = String.format(
                Locale.ROOT,
                "rejecting cluster state version [%d] received from [%s]",
                incomingState.metadata().x2(),
                incomingState.nodes().getMasterNodeId()
            );
            logger.warn(message);
            throw new IllegalStateException(message);
        }
    }

    @Override
    public DiscoveryStats stats() {
        return null;
    }

}
