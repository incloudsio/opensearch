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

package org.elassandra.cluster;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elassandra.index.ElasticSecondaryIndex;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateTaskConfig.SchemaUpdate;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Listen for cassandra schema changes and elasticsearch cluster state changes.
 */
public class SchemaListener extends SchemaChangeListener implements ClusterStateListener
{
    final ClusterService clusterService;
    final Logger logger;

    Metadata recordedMetaData = null;
    final ListMultimap<String, IndexMetadata> recordedIndexMetaData = ArrayListMultimap.create();

    public SchemaListener(Settings settings, ClusterService clusterService) {
        this.clusterService = clusterService;
        this.clusterService.addListener(this);
        this.logger = LogManager.getLogger(getClass());
    }

    /**
     * Apply pending mapping updates to Elasticsearch cluster state (Cassandra 4 uses per-change callbacks; we flush after each table change).
     */
    private void flushTransaction() {
        if (recordedMetaData != null || !recordedIndexMetaData.isEmpty()) {
            final ClusterState currentState = this.clusterService.state();
            final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
            final Metadata sourceMetaData = (recordedMetaData == null) ? currentState.metadata() : recordedMetaData;
            final Metadata.Builder metaDataBuilder = Metadata.builder(sourceMetaData);
            if (recordedMetaData == null) {
                recordedIndexMetaData.keySet().forEach( i -> clusterService.mergeIndexMetaData(metaDataBuilder, i, recordedIndexMetaData.get(i)));
            } else {
                clusterService.mergeWithTableExtensions(metaDataBuilder);
            }

            if (sourceMetaData.settings().getAsBoolean("cluster.blocks.read_only", false))
                blocks.addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK);

            for (IndexMetadata indexMetaData : sourceMetaData)
                blocks.updateBlocks(indexMetaData);

            final Metadata targetMetaData = metaDataBuilder.build();

            clusterService.submitStateUpdateTask("cql-schema-mapping-update", new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final ClusterState.Builder newStateBuilder = ClusterState.builder(currentState);
                    ClusterState newClusterState = newStateBuilder.incrementVersion()
                        .metadata(clusterService.addVirtualIndexMappings(targetMetaData))
                        .blocks(blocks)
                        .build();
                    newClusterState = ClusterState.builder(newClusterState)
                            .routingTable(RoutingTable.build(SchemaListener.this.clusterService, newClusterState))
                            .build();
                    return newClusterState;
                }

                @Override
                public void onFailure(String source, Exception t) {
                    logger.error("unexpected failure during [{}]", t, source);
                }

                @Override
                public SchemaUpdate schemaUpdate() {
                    return SchemaUpdate.UPDATE;
                }
            });
        }
        recordedIndexMetaData.clear();
        recordedMetaData = null;
    }

    @Override
    public void onCreateTable(String keyspace, String table) {
        recordedIndexMetaData.clear();
        recordedMetaData = null;
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        TableMetadata cfm = Schema.instance.getTableMetadata(keyspace, table);
        if (ksm == null || cfm == null)
            return;
        logger.trace("{}.{}", ksm.name, cfm.name);
        if (!isElasticAdmin(ksm.name, cfm.name)) {
            updateElasticsearchMapping(ksm, cfm);
        }
        flushTransaction();
    }

    @Override
    public void onAlterTable(String keyspace, String table, boolean affectsStatements) {
        recordedIndexMetaData.clear();
        recordedMetaData = null;
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        TableMetadata cfm = Schema.instance.getTableMetadata(keyspace, table);
        if (ksm == null || cfm == null)
            return;
        logger.trace("{}.{}", ksm.name, cfm.name);
        if (isElasticAdmin(ksm.name, cfm.name)) {
            recordedMetaData = clusterService.readMetaData(cfm);
        } else {
            updateElasticsearchMapping(ksm, cfm);
        }
        flushTransaction();
    }

    @Override
    public void onAlterKeyspace(String ksName) {
        logger.trace("{}", ksName);
        Metadata metadata = this.clusterService.state().metadata();
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        for(ObjectCursor<IndexMetadata> imdCursor : metadata.indices().values()) {
            if (ksName.equals(imdCursor.value.keyspace())) {
                KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(ksName);
                if (ksm != null && ksm.params != null && ksm.params.replication != null && ksm.params.replication.klass.isAssignableFrom(NetworkTopologyStrategy.class)) {
                    String localDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(local);
                    try {
                        Integer rf = Integer.parseInt(ksm.params.replication.asMap().get(localDc));
                        if (!rf.equals(imdCursor.value.getNumberOfReplicas()+1)) {
                            logger.debug("Submit numberOfReplicas update  for indices based on keyspace [{}]", ksName);
                            clusterService.submitNumberOfShardsAndReplicasUpdate("update-shard-replicas", ksName);
                        }
                    } catch(NumberFormatException e) {
                        // ignore
                    }
                }
            }
        }
    }

    @Override
    public void onDropKeyspace(String ksName) {
        logger.trace("{}", ksName);
    }

    boolean isElasticAdmin(String ksName, String cfName) {
        return (clusterService.getElasticAdminKeyspaceName().equals(ksName) &&
                ClusterService.ELASTIC_ADMIN_METADATA_TABLE.equals(cfName));
    }

    void updateElasticsearchMapping(KeyspaceMetadata ksm, TableMetadata cfm) {
        boolean hasSecondaryIndex = cfm.indexes.has(SchemaManager.buildIndexName(cfm.name));
        for(Map.Entry<String, ByteBuffer> e : cfm.params.extensions.entrySet()) {
            if (clusterService.isValidExtensionKey(e.getKey())) {
                    IndexMetadata indexMetaData = clusterService.getIndexMetaDataFromExtension(e.getValue());
                    recordedIndexMetaData.put(indexMetaData.getIndex().getName(), indexMetaData);

                    if (hasSecondaryIndex)
                        indexMetaData.getMappings().forEach( m -> SchemaManager.typeToCfName(ksm.name, m.value.type()) );
            }
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        for(ElasticSecondaryIndex esi : ElasticSecondaryIndex.elasticSecondayIndices.values())
            esi.clusterChanged(event);
    }
}
