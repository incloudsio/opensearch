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
 *
 * Side-car overlay: OpenSearch 1.3 GatewayService ctor uses Discovery (not GatewayMetaState/IndicesService); gateway()
 * is not exposed — keep a local Gateway for Elassandra recovery hook.
 */
package org.elassandra.gateway;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.Discovery;
import org.opensearch.gateway.Gateway;
import org.opensearch.gateway.GatewayService;
import org.opensearch.gateway.TransportNodesListGatewayMetaState;
import org.opensearch.rest.RestStatus;
import org.opensearch.threadpool.ThreadPool;

import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class CassandraGatewayService extends GatewayService {
    final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(getClass());

    public static final ClusterBlock NO_CASSANDRA_RING_BLOCK = new ClusterBlock(12, "no cassandra ring", true, true, true, RestStatus.SERVICE_UNAVAILABLE, EnumSet.of(ClusterBlockLevel.READ));

    private final ClusterService clusterService;

    private final AtomicBoolean recovered = new AtomicBoolean();

    private final Gateway gateway;

    @Inject
    public CassandraGatewayService(
        Settings settings,
        AllocationService allocationService,
        ClusterService clusterService,
        ThreadPool threadPool,
        TransportNodesListGatewayMetaState listGatewayMetaState,
        Discovery discovery
    ) {
        super(settings, allocationService, clusterService, threadPool, listGatewayMetaState, discovery);
        this.clusterService = clusterService;
        this.gateway = new Gateway(settings, clusterService, listGatewayMetaState);
    }

    /**
     * release the NO_CASSANDRA_RING_BLOCK and update routingTable since the node'state = NORMAL (i.e a member of the ring)
     * (may be update when replaying the cassandra logs)
     */
    public void enableMetaDataPersictency() {
        clusterService.submitStateUpdateTask("gateway-cassandra-ring-ready", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                logger.debug("releasing the cassandra ring block...");

                // remove the block, since we recovered from gateway
                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(NO_CASSANDRA_RING_BLOCK);

                // update the state to reflect
                ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).build();

                // update routing table
                RoutingTable routingTable = RoutingTable.build(clusterService, updatedState);
                return ClusterState.builder(updatedState).routingTable(routingTable).build();
            }

            @Override
            public void onFailure(String source, Exception t) {
                logger.error("unexpected failure during [{}]", t, source);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("cassandra ring block released");
                try {
                    clusterService.publishX1();
                } catch (Exception e) {
                    logger.error("unexpected failure on X1 publishing during [{}]", source, e);
                }
            }
        });
    }

    @Override
    protected void performStateRecovery(boolean enforceRecoverAfterTime, String reason) {
        final Gateway.GatewayStateRecoveredListener recoveryListener = new GatewayRecoveryListener();
        gateway.performStateRecovery(recoveryListener);
    }

    class GatewayRecoveryListener implements Gateway.GatewayStateRecoveredListener {

        @Override
        public void onSuccess(final ClusterState recoveredState) {
            logger.trace("Successful state recovery, importing cluster state...");
            clusterService.submitStateUpdateTask("cassandra-gateway-recovery-state", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    // remove the block, since we recovered from gateway
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder()
                        .blocks(currentState.blocks())
                        .blocks(recoveredState.blocks())
                        .removeGlobalBlock(STATE_NOT_RECOVERED_BLOCK);

                    Metadata.Builder metaDataBuilder = Metadata.builder(recoveredState.metadata());
                    // automatically generate a UID for the metadata if we need to
                    metaDataBuilder.generateClusterUuidIfNeeded();

                    if (recoveredState.metadata().settings().getAsBoolean("cluster.blocks.read_only", false)) {
                        blocks.addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK);
                    }

                    for (IndexMetadata indexMetaData : recoveredState.metadata()) {
                        metaDataBuilder.put(indexMetaData, false);
                        blocks.addBlocks(indexMetaData);
                    }

                    // update the state to reflect the new metadata and routing
                    ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metadata(metaDataBuilder).build();
                    RoutingTable newRoutingTable = RoutingTable.build(CassandraGatewayService.this.clusterService, updatedState);
                    return ClusterState.builder(updatedState).routingTable(newRoutingTable).build();
                }

                @Override
                public void onFailure(String source, Exception t) {
                    logger.error("Unexpected failure during [{}]", t, source);
                    GatewayRecoveryListener.this.onFailure("failed to updated cluster state");
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("Recovered [{}] indices into cluster_state metadata={}/{}", newState.metadata().indices().size(), newState.metadata().clusterUUID(), newState.metadata().version());
                }
            });
        }

        @Override
        public void onFailure(String message) {
            recovered.set(false);
            // don't remove the block here, we don't want to allow anything in such a case
            logger.info("Metadata state not restored, reason: {}", message);
        }

    }
}
