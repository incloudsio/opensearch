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
package org.elassandra.shard;

import org.elassandra.cluster.SchemaManager;
import org.elassandra.index.ElasticSecondaryIndex;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Post applied cluster state service to update gossip X1 shards state.
 * Publish local ShardRouting state in the gossip state X1.
 */
public class CassandraShardStateListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(CassandraShardStateListener.class);

    private final ClusterService clusterService;
    private final RoutingTableUpdateTaskExecutor routingTableUpdateTaskExecutor;

    @Inject
    public CassandraShardStateListener(Settings settings, ClusterService clusterService) {
        this.clusterService = clusterService;
        this.routingTableUpdateTaskExecutor = new RoutingTableUpdateTaskExecutor();
    }

    /**
     * Called after the index shard has been started.
     */
    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        try {
            logger.info("shard [{}][0] started", indexShard.shardId().getIndexName());
            String keyspace = indexShard.indexService().keyspace();
            for(String type : indexShard.indexService().mapperService().types()) {
                ElasticSecondaryIndex esi = ElasticSecondaryIndex.elasticSecondayIndices.get(keyspace + "." + SchemaManager.typeToCfName(keyspace, type));
                if (esi != null)
                    esi.onShardStarted(indexShard);
            }
            clusterService.publishShardRoutingState(indexShard.shardId().getIndexName(), ShardRoutingState.STARTED);
            clusterService.submitStateUpdateTask("shard-started-update-routing index="+indexShard.indexService().index().getName(),
                    new RoutingTableUpdateTask(indexShard.indexService().index()), routingTableUpdateTaskExecutor, routingTableUpdateTaskExecutor, routingTableUpdateTaskExecutor);
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        }
    }


    class RoutingTableUpdateTask  {
        Index index;

        RoutingTableUpdateTask(Index index) {
            this.index = index;
        }

        public Index index() {
            return this.index;
        }
    }

    /**
     * Computation of the routing table for several indices for batched cluster state updates.
     */
    class RoutingTableUpdateTaskExecutor implements ClusterStateTaskExecutor<RoutingTableUpdateTask>, ClusterStateTaskConfig, ClusterStateTaskListener {
        @Override
        public ClusterTasksResult<RoutingTableUpdateTask> execute(ClusterState currentState, List<RoutingTableUpdateTask> tasks) throws Exception {
            RoutingTable routingTable = RoutingTable.build(clusterService, currentState, tasks.stream().map(RoutingTableUpdateTask::index).collect(Collectors.toList()));
            ClusterState resultingState = ClusterState.builder(currentState).routingTable(routingTable).build();
            return ClusterTasksResult.builder().successes((List)tasks).build(resultingState);
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

    /**
     * Called before the index shard gets closed.
     */
    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        try {
            clusterService.publishShardRoutingState(shardId.getIndexName(), ShardRoutingState.UNASSIGNED);
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        }
    }

    /**
     * Called after the index shard has been deleted from disk.
     *
     * Note: this method is only called if the deletion of the shard did finish without an exception
     *
     */
    @Override
    public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        try {
            clusterService.publishShardRoutingState(shardId.getIndexName(), null);
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        }
    }

    @Override
    public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
        for(String type : indexService.mapperService().types()) {
            SchemaManager.typeToCfName(indexService.keyspace(), type, true);
        }
    }
}
