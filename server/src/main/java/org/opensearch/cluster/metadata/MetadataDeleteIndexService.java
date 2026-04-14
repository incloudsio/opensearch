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

package org.opensearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.HashMultimap;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.schema.ElassandraSchemaBridge;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.utils.FBUtilities;
import org.elassandra.cluster.SchemaManager;
import org.opensearch.cluster.ClusterStateTaskConfig.SchemaUpdate;
import org.opensearch.index.mapper.MapperService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.delete.DeleteIndexClusterStateUpdateRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.index.Index;
import org.opensearch.snapshots.RestoreService;
import org.opensearch.snapshots.SnapshotInProgressException;
import org.opensearch.snapshots.SnapshotsService;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Deletes indices.
 */
public class MetadataDeleteIndexService {

    private static final Logger logger = LogManager.getLogger(MetadataDeleteIndexService.class);

    private final Settings settings;
    private final ClusterService clusterService;

    private final AllocationService allocationService;

    @Inject
    public MetadataDeleteIndexService(Settings settings, ClusterService clusterService, AllocationService allocationService) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    public void deleteIndices(
        final DeleteIndexClusterStateUpdateRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        clusterService.submitStateUpdateTask(
            "delete-index " + Arrays.toString(request.indices()),
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public SchemaUpdate schemaUpdate() {
                    return SchemaUpdate.UPDATE;
                }

                @Override
                public ClusterState execute(final ClusterState currentState) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ClusterState execute(
                    final ClusterState currentState,
                    Collection<Mutation> mutations,
                    Collection<Event.SchemaChange> events
                ) {
                    return deleteIndices(currentState, Sets.newHashSet(request.indices()), mutations, events);
                }
            }
        );
    }

    // for testing purposes only
    public ClusterState deleteIndices(ClusterState currentState, Set<Index> indices) {
        return deleteIndices(currentState, indices, new ArrayList<Mutation>(), new ArrayList<Event.SchemaChange>());
    }

    // collected keyspaces and tables for deleted indices
    class KeyspaceRemovalInfo {
        String keyspace;
        Set<IndexMetadata> indices = new HashSet<>();
        Set<String> droppableTables = new HashSet<>();
        Set<String> unindexableTables = new HashSet<>();
        boolean droppableKeyspace = true;

        KeyspaceRemovalInfo(String keyspace) {
            this.keyspace = keyspace;
        }

        void addRemovedIndex(IndexMetadata indexMetadata, boolean clusterDropOnDelete) {
            assert this.keyspace.equals(indexMetadata.keyspace()) : "Keyspace does not match";
            indices.add(indexMetadata);
            for (ObjectCursor<MappingMetadata> type : indexMetadata.getMappings().values()) {
                if (MapperService.DEFAULT_MAPPING.equals(type.value.type()) == false) {
                    String tableName = SchemaManager.typeToCfName(indexMetadata.keyspace(), type.value.type());
                    unindexableTables.add(tableName);
                    if (indexMetadata.getSettings().getAsBoolean("index.drop_on_delete_index", clusterDropOnDelete)) {
                        droppableTables.add(tableName);
                    }
                }
            }
            if (indexMetadata.getSettings().getAsBoolean("index.drop_on_delete_index", clusterDropOnDelete) == false) {
                droppableKeyspace = false;
            }
        }

        void removeUsedTables(IndexMetadata indexMetadata) {
            assert this.keyspace.equals(indexMetadata.keyspace()) : "Keyspace does not match";
            for (ObjectCursor<MappingMetadata> type : indexMetadata.getMappings().values()) {
                String tableName = SchemaManager.typeToCfName(indexMetadata.keyspace(), type.value.type());
                droppableTables.remove(tableName);
                unindexableTables.remove(tableName);
                droppableKeyspace = false;
            }
        }

        void drop(Collection<Mutation> mutations, Collection<Event.SchemaChange> events) {
            logger.debug(
                "drop keyspaces={} droppableKeyspace={} droppableTables={} unindexableTables={}",
                keyspace,
                droppableKeyspace,
                droppableTables,
                unindexableTables
            );

            if (droppableKeyspace) {
                clusterService.getSchemaManager().dropIndexKeyspace(keyspace, mutations, events);
                return;
            }

            KeyspaceMetadata ksm = SchemaManager.getKSMetaDataCopy(keyspace);
            if (ksm == null) {
                return;
            }

            for (String table : droppableTables) {
                ksm = clusterService.getSchemaManager().dropTable(ksm, table, mutations, events);
                unindexableTables.remove(table);
            }
            for (String table : unindexableTables) {
                clusterService.getSchemaManager().dropSecondaryIndex(ksm, table, mutations, events);
            }

            HashMultimap<String, IndexMetadata> tableExtensionToRemove = HashMultimap.create();
            for (IndexMetadata indexMetadata : indices) {
                for (ObjectCursor<MappingMetadata> type : indexMetadata.getMappings().values()) {
                    String table = SchemaManager.typeToCfName(indexMetadata.keyspace(), type.value.type());
                    if (droppableTables.contains(table) == false && unindexableTables.contains(table) == false) {
                        tableExtensionToRemove.put(table, indexMetadata);
                    }
                }
            }
            Mutation.SimpleBuilder builder = ElassandraSchemaBridge.makeCreateKeyspaceMutation(ksm, FBUtilities.timestampMicros());
            boolean removedExtension = false;
            for (String table : tableExtensionToRemove.keySet()) {
                TableMetadata cfm = ksm.getTableOrViewNullable(table);
                clusterService.getSchemaManager().removeTableExtensionToMutationBuilder(cfm, tableExtensionToRemove.get(table), builder);
                removedExtension = true;
            }
            if (removedExtension) {
                mutations.add(builder.build());
            }
        }
    }

    /**
     * Delete some indices from the cluster state.
     */
    public ClusterState deleteIndices(
        ClusterState currentState,
        Set<Index> indices,
        Collection<Mutation> mutations,
        Collection<Event.SchemaChange> events
    ) {
        final Metadata meta = currentState.metadata();
        final Set<Index> indicesToDelete = new HashSet<>();
        final Map<Index, DataStream> backingIndices = new HashMap<>();
        for (Index index : indices) {
            IndexMetadata indexMetadata = meta.getIndexSafe(index);
            IndexAbstraction.DataStream parent = meta.getIndicesLookup().get(indexMetadata.getIndex().getName()).getParentDataStream();
            if (parent != null) {
                if (parent.getWriteIndex().equals(indexMetadata)) {
                    throw new IllegalArgumentException(
                        "index ["
                            + index.getName()
                            + "] is the write index for data stream ["
                            + parent.getName()
                            + "] and cannot be deleted"
                    );
                } else {
                    backingIndices.put(index, parent.getDataStream());
                }
            }
            indicesToDelete.add(indexMetadata.getIndex());
        }

        Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentState, indicesToDelete);
        if (snapshottingIndices.isEmpty() == false) {
            throw new SnapshotInProgressException(
                "Cannot delete indices that are being snapshotted: "
                    + snapshottingIndices
                    + ". Try again after snapshot finishes or cancel the currently running snapshot."
            );
        }

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(meta);
        ClusterBlocks.Builder clusterBlocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());

        final IndexGraveyard.Builder graveyardBuilder = IndexGraveyard.builder(metadataBuilder.indexGraveyard());
        final int previousGraveyardSize = graveyardBuilder.tombstones().size();
        final boolean clusterDropOnDelete = currentState.metadata()
            .settings()
            .getAsBoolean("cluster.drop_on_delete_index", Boolean.getBoolean("es.drop_on_delete_index"));
        final Map<String, KeyspaceRemovalInfo> removalInfoMap = new HashMap<>();
        for (final Index index : indices) {
            String indexName = index.getName();
            logger.info("{} deleting index", index);
            routingTableBuilder.remove(indexName);
            clusterBlocksBuilder.removeIndexBlocks(indexName);
            metadataBuilder.remove(indexName);
            if (backingIndices.containsKey(index)) {
                DataStream parent = metadataBuilder.dataStream(backingIndices.get(index).getName());
                metadataBuilder.put(parent.removeBackingIndex(index));
            }

            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);
            KeyspaceRemovalInfo removalInfo = removalInfoMap.computeIfAbsent(indexMetadata.keyspace(), KeyspaceRemovalInfo::new);
            removalInfo.addRemovedIndex(indexMetadata, clusterDropOnDelete);
        }
        final IndexGraveyard currentGraveyard = graveyardBuilder.addTombstones(indices).build(settings);
        metadataBuilder.indexGraveyard(currentGraveyard);
        logger.trace(
            "{} tombstones purged from the cluster state. Previous tombstone size: {}. Current tombstone size: {}.",
            graveyardBuilder.getNumPurged(),
            previousGraveyardSize,
            currentGraveyard.getTombstones().size()
        );

        Metadata newMetadata = metadataBuilder.build();
        ClusterBlocks blocks = clusterBlocksBuilder.build();

        for (ObjectCursor<IndexMetadata> cursor : newMetadata.indices().values()) {
            final IndexMetadata indexMetadata = cursor.value;
            removalInfoMap.computeIfPresent(indexMetadata.keyspace(), (keyspace, removalInfo) -> {
                removalInfo.removeUsedTables(indexMetadata);
                return removalInfo;
            });
        }
        for (KeyspaceRemovalInfo removalInfo : removalInfoMap.values()) {
            removalInfo.drop(mutations, events);
        }

        ImmutableOpenMap<String, ClusterState.Custom> customs = currentState.getCustoms();
        final RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
        RestoreInProgress updatedRestoreInProgress = RestoreService.updateRestoreStateWithDeletedIndices(restoreInProgress, indices);
        if (updatedRestoreInProgress != restoreInProgress) {
            ImmutableOpenMap.Builder<String, ClusterState.Custom> builder = ImmutableOpenMap.builder(customs);
            builder.put(RestoreInProgress.TYPE, updatedRestoreInProgress);
            customs = builder.build();
        }

        return allocationService.reroute(
            ClusterState.builder(currentState)
                .routingTable(routingTableBuilder.build())
                .metadata(newMetadata)
                .blocks(blocks)
                .customs(customs)
                .build(),
            "deleted indices [" + indices + "]"
        );
    }
}
